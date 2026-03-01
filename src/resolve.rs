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

/// Recursively collect tar files from a directory.
fn resolve_dir(path: &Path) -> Result<Vec<PathBuf>> {
    let abs = make_absolute(path)?;
    let mut results = Vec::new();
    for entry in WalkDir::new(&abs)
        .into_iter()
        .filter_entry(|e| !is_hidden(e))
    {
        let entry = entry.with_context(|| format!("walking {}", path.display()))?;
        if !entry.file_type().is_file() {
            continue;
        }
        if is_tarfile(entry.path()) {
            results.push(make_absolute(entry.path())?);
        }
    }
    Ok(results)
}

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

fn resolve_source(source: &str) -> Result<Vec<PathBuf>> {
    if is_glob(source) {
        return resolve_glob(source);
    }

    let path = Path::new(source);
    let meta = path.metadata().with_context(|| source.to_string())?;

    if meta.is_dir() {
        resolve_dir(path)
    } else if meta.is_file() && is_tarfile(path) {
        Ok(vec![make_absolute(path)?])
    } else {
        bail!("{source}: not a valid tarfile or directory")
    }
}

/// Parallelizes resolution when there are enough sources to justify the overhead.
const PAR_THRESHOLD: usize = 8;

/// Resolve multiple source strings into deduplicated, sorted absolute paths.
pub fn resolve_sources(sources: &[&str]) -> Result<Vec<PathBuf>> {
    let mut resolved: Vec<PathBuf> = Vec::new();

    if sources.len() >= PAR_THRESHOLD {
        let per_source: Vec<Result<Vec<PathBuf>>> =
            sources.par_iter().map(|s| resolve_source(s)).collect();
        for result in per_source {
            resolved.extend(result?);
        }
    } else {
        for source in sources {
            resolved.extend(resolve_source(source)?);
        }
    }

    resolved.sort_unstable();
    resolved.dedup();
    Ok(resolved)
}
