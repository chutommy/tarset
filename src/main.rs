use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rayon::prelude::*;
use walkdir::{DirEntry, WalkDir};

const TAR_EXTENSIONS: &[&str] = &[".tar", ".tar.gz", ".tgz", ".tar.bz2", ".tar.xz", ".tar.zst"];

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}

fn is_tarfile(path: &Path) -> bool {
    let name = match path.file_name().and_then(|n| n.to_str()) {
        Some(n) => n,
        None => return false,
    };
    TAR_EXTENSIONS.iter().any(|ext| name.ends_with(ext))
}

fn is_glob(s: &str) -> bool {
    s.contains('*') || s.contains('?') || s.contains('[')
}

fn make_absolute(path: &Path) -> Result<PathBuf> {
    std::path::absolute(path).with_context(|| path.display().to_string())
}

fn resolve_dir(path: &Path) -> Result<Vec<PathBuf>> {
    let mut results = Vec::new();
    let walker = WalkDir::new(path).into_iter();
    for entry in walker.filter_entry(|e| !is_hidden(e)) {
        let entry = entry.with_context(|| format!("walking {}", path.display()))?;
        // file_type() is cached -> no extra stat
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

pub fn resolve_sources(sources: &[String]) -> Result<Vec<PathBuf>> {
    let per_source: Vec<Result<Vec<PathBuf>>> =
        sources.par_iter().map(|s| resolve_source(s)).collect();

    let mut resolved: Vec<PathBuf> = Vec::new();
    for result in per_source {
        resolved.extend(result?);
    }

    resolved.sort_unstable();
    resolved.dedup();
    Ok(resolved)
}

fn main() {
    let sources = vec![
        "/home/tommy/Documents/webdatataset/testdata/testgz.tar".to_string(),
        "/home/tommy/Documents/webdatataset/testdata/testgz.tar".to_string(),
        "/home/tommy/Documents/*/testdata/*.tar".to_string(),
        "/home/tommy/Documents/*/testdata/*.tgz".to_string(),
        "/home/tommy/Documents/mortar".to_string(),
        "/home/tommy/Documents".to_string(),
    ];
    match resolve_sources(&sources) {
        Ok(resolved) => {
            for path in &resolved {
                println!("{}", path.display());
            }
        }
        Err(e) => eprintln!("Error: {e:#}"),
    }
}
