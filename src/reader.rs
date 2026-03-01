use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::TarFormat;

/// Split a tar entry path into (key, suffix) operating on raw bytes.
/// Splits at the first `.` in the basename (after last `/`).
///
/// Examples:
/// `/a/b/c/000042.cls.txt` -> `( "/a/b/c/000042", "cls.txt" )`
/// ` d/e/f/000042.img.jpg` -> `( "d/e/f/000042" , "img.jpg" )`
/// ` ./g/h/000042.webp`    -> `( "./g/h/000042" , "webp"    )`
fn split_key_suffix(path: &[u8]) -> Option<(&[u8], &[u8])> {
    let basename_start = path.iter().rposition(|&b| b == b'/').map_or(0, |i| i + 1);
    let dot = memchr::memchr(b'.', &path[basename_start..])? + basename_start;
    let key = &path[..dot];
    let suffix = &path[dot + 1..];
    if key.is_empty() || suffix.is_empty() {
        None
    } else {
        Some((key, suffix))
    }
}

// Lustre file system (and many others) perform best with large sequential reads.
const LUSTRE_OPTIMAL_BUFFER: usize = 1024 * 1024 * 16;

/// A single field within a sample: a suffix and its data.
pub struct Field {
    pub suffix: String,
    pub data: Vec<u8>,
}

/// A single sample from a tar archive.
pub struct Sample {
    pub key: String,
    pub url: Arc<str>,
    pub fields: Vec<Field>,
}

/// Open a tar archive with decompression based on [`TarFormat`].
/// For compressed formats, wraps the decompressor in a BufReader so the tar
/// crate's small reads are served from buffer.
fn open_tar(path: &Path) -> Result<tar::Archive<Box<dyn Read>>> {
    let file = File::open(path)
        .with_context(|| format!("Failed to open archive at: {}", path.display()))?;
    advise_sequential(&file);
    let buf = BufReader::with_capacity(LUSTRE_OPTIMAL_BUFFER, file);
    let format = TarFormat::from_path(path).unwrap_or(TarFormat::Tar);

    let reader: Box<dyn Read> = match format {
        TarFormat::Tar => Box::new(buf),
        TarFormat::TarGz | TarFormat::Tgz => Box::new(BufReader::with_capacity(
            LUSTRE_OPTIMAL_BUFFER,
            flate2::read::GzDecoder::new(buf),
        )),
        TarFormat::TarBz2 => Box::new(BufReader::with_capacity(
            LUSTRE_OPTIMAL_BUFFER,
            bzip2::read::BzDecoder::new(buf),
        )),
        TarFormat::TarXz => Box::new(BufReader::with_capacity(
            LUSTRE_OPTIMAL_BUFFER,
            xz2::read::XzDecoder::new(buf),
        )),
        TarFormat::TarZst => {
            let mut decoder = zstd::stream::Decoder::new(buf)?;
            decoder.window_log_max(31)?;
            Box::new(BufReader::with_capacity(LUSTRE_OPTIMAL_BUFFER, decoder))
        }
    };

    Ok(tar::Archive::new(reader))
}

/// Hint the OS to prefetch aggressively for sequential access.
fn advise_sequential(file: &File) {
    unsafe {
        libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
    }
}

/// Streaming iterator over [`Sample`]s in a tar archive.
pub struct SampleReader {
    entries: tar::Entries<'static, Box<dyn Read>>,
    _archive: Box<tar::Archive<Box<dyn Read>>>,
    url: Arc<str>,
    current_key: Vec<u8>,
    current_sample: Vec<Field>,
    suffixes: Option<HashSet<String>>,
    done: bool,
}

impl SampleReader {
    pub fn open(path: &Path) -> Result<Self> {
        let url: Arc<str> = Arc::from(path.display().to_string().as_str());
        let archive = Box::new(open_tar(path)?);

        let archive_ptr = Box::into_raw(archive);
        let entries = unsafe { (*archive_ptr).entries()? };
        let entries = unsafe { std::mem::transmute(entries) };
        let archive = unsafe { Box::from_raw(archive_ptr) };

        Ok(Self {
            entries,
            _archive: archive,
            url,
            current_key: Vec::new(),
            current_sample: Vec::new(),
            suffixes: None,
            done: false,
        })
    }

    /// Only read entries whose suffix is in `suffixes`.
    pub fn with_suffixes(mut self, suffixes: impl IntoIterator<Item = String>) -> Self {
        self.suffixes = Some(suffixes.into_iter().collect());
        self
    }

    fn wants_suffix(&self, suffix: &[u8]) -> bool {
        match &self.suffixes {
            None => true,
            Some(set) => {
                if let Ok(s) = std::str::from_utf8(suffix) {
                    set.contains(s)
                } else {
                    false
                }
            }
        }
    }

    /// Build a finished Sample from the current accumulation, moving data out.
    fn take_sample(&mut self) -> Sample {
        let key = String::from_utf8_lossy(&self.current_key).into_owned();
        Sample {
            key,
            url: Arc::clone(&self.url),
            fields: std::mem::take(&mut self.current_sample),
        }
    }
}

impl Iterator for SampleReader {
    type Item = Result<Sample>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let entry = match self.entries.next() {
                Some(Ok(e)) => e,
                Some(Err(e)) => return Some(Err(e.into())),
                None => {
                    self.done = true;
                    if !self.current_sample.is_empty() {
                        return Some(Ok(self.take_sample()));
                    }
                    return None;
                }
            };

            if entry.header().entry_type().is_dir() {
                continue;
            }

            let size = entry.size() as usize;

            // Analyze path while borrowing entry, extract only the decisions.
            let analysis = {
                let path_bytes = entry.path_bytes();
                let (key, suffix) = match split_key_suffix(&path_bytes) {
                    Some(pair) => pair,
                    None => {
                        let path_display = String::from_utf8_lossy(&path_bytes);
                        return Some(Err(anyhow::anyhow!(
                            "Invalid entry path (missing key or suffix): {path_display}"
                        )));
                    }
                };

                let key_changed = self.current_key != key;
                let want = self.wants_suffix(suffix);

                if key_changed {
                    self.current_key.clear();
                    self.current_key.extend_from_slice(key);
                }

                // Only allocate suffix string when we'll actually use it
                let suffix_str = if want {
                    Some(String::from_utf8_lossy(suffix).into_owned())
                } else {
                    None
                };

                (key_changed, suffix_str)
            }; // path_bytes borrow dropped here

            let (key_changed, suffix_str) = analysis;

            if key_changed {
                let prev_sample = if !self.current_sample.is_empty() {
                    Some(self.take_sample())
                } else {
                    None
                };

                if let Some(suffix_str) = suffix_str {
                    let mut data = Vec::with_capacity(size + 64);
                    let mut entry = entry;
                    if let Err(e) = entry.read_to_end(&mut data) {
                        return Some(Err(e.into()));
                    }
                    self.current_sample.push(Field {
                        suffix: suffix_str,
                        data,
                    });
                }

                if let Some(sample) = prev_sample {
                    return Some(Ok(sample));
                }
                continue;
            }

            // Same key, accumulate fields
            if let Some(suffix_str) = suffix_str {
                let mut data = Vec::with_capacity(size + 64);
                let mut entry = entry;
                if let Err(e) = entry.read_to_end(&mut data) {
                    return Some(Err(e.into()));
                }
                self.current_sample.push(Field {
                    suffix: suffix_str,
                    data,
                });
            }
        }
    }
}
