//! Resolves user-provided source strings (file paths, directories, globs)
//! into a deduplicated, sorted list of absolute tar file paths.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use walkdir::{DirEntry, WalkDir};

use crate::TarFormat;

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn is_tarfile(path: &Path) -> bool {
    TarFormat::from_path(path).is_some()
}

fn is_glob(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

fn make_absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).with_context(|| path.display().to_string())
}

/// Recursively walks a directory, collecting tar files while skipping hidden entries.
fn resolve_dir(path: &Path) -> Result<Vec<PathBuf>> {
    let abs = make_absolute(path)?;
    let mut results = Vec::new();
    for entry in WalkDir::new(&abs)
        .into_iter()
        .filter_entry(|e| !is_hidden(e))
    {
        let entry = entry.with_context(|| format!("walking {}", path.display()))?;
        // file_type() is cached from readdir d_type; no extra stat call
        if !entry.file_type().is_file() {
            continue;
        }
        if is_tarfile(entry.path()) {
            results.push(make_absolute(entry.path())?);
        }
    }
    Ok(results)
}

/// Expands a glob pattern, keeping only paths that match a known tar extension.
fn resolve_glob(pattern: &str) -> Result<Vec<PathBuf>> {
    let mut results = Vec::new();
    for entry in glob::glob(pattern).with_context(|| pattern.to_string())? {
        let path = entry.with_context(|| format!("glob {pattern}"))?;
        if is_tarfile(&path) {
            results.push(make_absolute(&path)?);
        }
    }
    Ok(results)
}

/// Resolves a single source: glob pattern, directory, or direct file path.
fn resolve_source(source: &str) -> Result<Vec<PathBuf>> {
    if is_glob(source) {
        return resolve_glob(source);
    }

    let path = Path::new(source);
    // using metadata call to reduce stat round-trips
    let meta = path.metadata().with_context(|| source.to_string())?;

    if meta.is_dir() {
        resolve_dir(path)
    } else if meta.is_file() && is_tarfile(path) {
        Ok(vec![make_absolute(path)?])
    } else {
        bail!("{source}: not a valid tarfile or directory")
    }
}

/// Minimum number of sources before parallelizing resolution.
/// Below this threshold, thread pool overhead outweighs the I/O gains.
const PAR_THRESHOLD: usize = 4;

/// Resolves multiple source strings into deduplicated, sorted absolute paths.
/// Falls back to sequential resolution when the number of sources is small.
pub fn resolve_sources(sources: &[&str]) -> Result<Vec<PathBuf>> {
    let mut resolved: Vec<PathBuf> = Vec::new();

    if sources.len() >= PAR_THRESHOLD {
        // multithreaded resolution
        let per_source: Vec<Result<Vec<PathBuf>>> =
            sources.par_iter().map(|s| resolve_source(s)).collect();
        for result in per_source {
            resolved.extend(result?);
        }
    } else {
        // sequential resolution
        for source in sources {
            resolved.extend(resolve_source(source)?);
        }
    }

    resolved.sort_unstable();
    resolved.dedup();
    Ok(resolved)
}
