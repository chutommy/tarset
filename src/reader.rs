use std::collections::HashSet;
use std::fs::File;
use std::io::{BufReader, Read};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};

use crate::LUSTRE_OPTIMAL_BUFFER;
use crate::TarFormat;
use crate::sample::{Field, Sample};

/// Split a tar entry path into `(key, suffix)` at the first `.` in the basename.
///
/// ```text
/// /a/b/000042.cls.txt -> ("/a/b/000042", "cls.txt")
/// ./g/h/000042.webp   -> ("./g/h/000042", "webp")
/// ```
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

/// Open a tar archive with decompression based on [`TarFormat`].
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

/// Streaming iterator that yields [`Sample`]s from a tar archive.
/// Consecutive tar entries sharing the same key are grouped into a single sample.
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

        // SAFETY: archive is heap-pinned and outlives entries via _archive field.
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

    /// Filter entries by suffix. Only matching entries will have their data read.
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

    /// Extract ad build a Sample from the current accumulation.
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

            // Parse path and decide what to do before consuming the entry.
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

                let suffix_str = if want {
                    Some(String::from_utf8_lossy(suffix).into_owned())
                } else {
                    None
                };

                let key_owned = if key_changed {
                    Some(key.to_vec())
                } else {
                    None
                };

                (key_changed, suffix_str, key_owned)
            };

            let (key_changed, suffix_str, new_key) = analysis;

            if key_changed {
                let prev_sample = if !self.current_sample.is_empty() {
                    Some(self.take_sample())
                } else {
                    None
                };

                self.current_key = new_key.unwrap();

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
