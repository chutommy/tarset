use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub type Sample = HashMap<String, Vec<u8>>;

fn split_key_suffix(path: &str) -> Option<(&str, &str)> {
    let (key, suffix) = path.split_once('.')?;
    if key.is_empty() || suffix.is_empty() {
        return None;
    }
    Some((key, suffix))
}

fn open_tar(path: &Path) -> Result<tar::Archive<Box<dyn Read>>> {
    let file = File::open(path).with_context(|| path.display().to_string())?;
    let buf = BufReader::new(file);
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

    let reader: Box<dyn Read> = if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
        Box::new(flate2::read::GzDecoder::new(buf))
    } else if name.ends_with(".tar.bz2") {
        Box::new(bzip2::read::BzDecoder::new(buf))
    } else if name.ends_with(".tar.xz") {
        Box::new(xz2::read::XzDecoder::new(buf))
    } else if name.ends_with(".tar.zst") {
        Box::new(zstd::stream::Decoder::new(buf)?)
    } else {
        Box::new(buf)
    };

    Ok(tar::Archive::new(reader))
}

/// Iterate over samples in a single tar file.
///
/// Consecutive entries sharing the same key (prefix before first dot) are
/// grouped into one [`Sample`]. Each sample contains:
/// - `__key__` — the sample key (e.g. `"000042"`)
/// - `__url__` — the tar file path
/// - one entry per suffix (e.g. `".jpg"` → raw bytes)
pub fn read_samples(path: &Path) -> Result<Vec<Sample>> {
    let url = path.display().to_string().into_bytes();
    let mut archive = open_tar(path)?;

    let mut samples: Vec<Sample> = Vec::new();
    let mut current_key: Option<String> = None;
    let mut current_sample = Sample::new();

    for entry in archive
        .entries()
        .with_context(|| path.display().to_string())?
    {
        let mut entry = entry.with_context(|| format!("reading entry in {}", path.display()))?;

        let entry_path = entry
            .path()
            .with_context(|| format!("entry path in {}", path.display()))?
            .to_string_lossy()
            .into_owned();

        // Skip directories
        if entry.header().entry_type().is_dir() {
            continue;
        }

        let (key, suffix) = match split_key_suffix(&entry_path) {
            Some(pair) => pair,
            None => continue,
        };

        // Key changed — flush the current sample
        if current_key.as_deref() != Some(key) {
            if current_key.is_some() {
                samples.push(current_sample);
                current_sample = Sample::new();
            }
            current_key = Some(key.to_owned());
            current_sample.insert("__key__".to_owned(), key.as_bytes().to_vec());
            current_sample.insert("__url__".to_owned(), url.clone());
        }

        let mut data = Vec::new();
        entry
            .read_to_end(&mut data)
            .with_context(|| format!("{entry_path} in {}", path.display()))?;
        current_sample.insert(suffix.to_owned(), data);
    }

    // Flush last sample
    if current_key.is_some() {
        samples.push(current_sample);
    }

    Ok(samples)
}

/// Read samples from multiple tar files in parallel.
pub fn read_samples_multi(paths: &[PathBuf]) -> Result<Vec<Sample>> {
    use rayon::prelude::*;

    let per_file: Vec<Result<Vec<Sample>>> = paths.par_iter().map(|p| read_samples(p)).collect();

    let mut all = Vec::new();
    for result in per_file {
        all.extend(result?);
    }
    Ok(all)
}
