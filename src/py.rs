use std::path::Path;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

use crate::reader;
use crate::resolve;
use crate::sample;
use crate::writer;

/// A named blob within a sample (suffix + raw bytes).
#[pyclass(name = "Field")]
pub struct PyField {
    #[pyo3(get)]
    pub suffix: String,
    data: Vec<u8>,
}

#[pymethods]
impl PyField {
    // TODO: benchmark PyBytes::new (copies data) vs holding an Arc<[u8]> and
    // using PyBytes::new_with or a buffer-protocol object to avoid the copy.
    #[getter]
    fn data<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.data)
    }

    fn __repr__(&self) -> String {
        format!("Field(suffix={:?}, len={})", self.suffix, self.data.len())
    }
}

impl From<sample::Field> for PyField {
    fn from(f: sample::Field) -> Self {
        Self {
            suffix: f.suffix,
            data: f.data,
        }
    }
}

/// A group of fields sharing the same key within a tar archive.
#[pyclass(name = "Sample")]
pub struct PySample {
    #[pyo3(get)]
    pub key: String,
    #[pyo3(get)]
    pub url: String,
    fields: Vec<sample::Field>,
}

#[pymethods]
impl PySample {
    // TODO: benchmark eager Vec<PyField> conversion vs lazy iteration.
    // Currently calling .fields drains all fields at once; if users typically
    // access only one or two fields, a lazy approach could save allocations.
    #[getter]
    fn fields(&mut self) -> Vec<PyField> {
        std::mem::take(&mut self.fields)
            .into_iter()
            .map(PyField::from)
            .collect()
    }

    fn __repr__(&self) -> String {
        let suffixes: Vec<&str> = self.fields.iter().map(|f| f.suffix.as_str()).collect();
        format!("Sample(key={:?}, suffixes={suffixes:?})", self.key)
    }
}

impl From<sample::Sample> for PySample {
    fn from(s: sample::Sample) -> Self {
        Self {
            key: s.key,
            url: s.url.to_string(),
            fields: s.fields,
        }
    }
}

/// Streaming iterator that yields samples from a tar archive.
#[pyclass(name = "SampleReader", unsendable)]
pub struct PySampleReader {
    inner: reader::SampleReader,
}

#[pymethods]
impl PySampleReader {
    #[new]
    #[pyo3(signature = (path, *, suffixes=None))]
    fn new(path: &str, suffixes: Option<Vec<String>>) -> PyResult<Self> {
        let mut reader = reader::SampleReader::open(Path::new(path))
            .map_err(|e| PyRuntimeError::new_err(format!("{e:#}")))?;
        if let Some(s) = suffixes {
            reader.set_suffixes(s);
        }
        Ok(Self { inner: reader })
    }

    fn set_suffixes(&mut self, suffixes: Vec<String>) {
        self.inner.set_suffixes(suffixes);
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<PySample>> {
        match slf.inner.next() {
            Some(Ok(sample)) => Ok(Some(sample.into())),
            Some(Err(e)) => Err(PyRuntimeError::new_err(format!("{e:#}"))),
            None => Ok(None),
        }
    }
}

/// Writes samples into a tar archive.
#[pyclass(name = "SampleWriter", unsendable)]
pub struct PySampleWriter {
    inner: Option<writer::SampleWriter>,
}

#[pymethods]
impl PySampleWriter {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        let w = writer::SampleWriter::create(Path::new(path))
            .map_err(|e| PyRuntimeError::new_err(format!("{e:#}")))?;
        Ok(Self { inner: Some(w) })
    }

    fn write_sample(&mut self, sample: &PySample) -> PyResult<()> {
        let w = self
            .inner
            .as_mut()
            .ok_or_else(|| PyRuntimeError::new_err("writer already finished"))?;
        let s = sample::Sample {
            key: sample.key.clone(),
            url: Arc::from(sample.url.as_str()),
            fields: sample
                .fields
                .iter()
                .map(|f| sample::Field {
                    suffix: f.suffix.clone(),
                    data: f.data.clone(),
                })
                .collect(),
        };
        w.write_sample(&s)
            .map_err(|e| PyRuntimeError::new_err(format!("{e:#}")))
    }

    fn finish(&mut self) -> PyResult<()> {
        let w = self
            .inner
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("writer already finished"))?;
        w.finish()
            .map_err(|e| PyRuntimeError::new_err(format!("{e:#}")))
    }

    fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __exit__(
        &mut self,
        _exc_type: Option<&Bound<'_, pyo3::types::PyType>>,
        _exc_val: Option<&Bound<'_, pyo3::types::PyAny>>,
        _exc_tb: Option<&Bound<'_, pyo3::types::PyAny>>,
    ) -> PyResult<bool> {
        if self.inner.is_some() {
            self.finish()?;
        }
        Ok(false)
    }
}

/// Resolve source paths/globs/directories into deduplicated tar file paths.
#[pyfunction]
pub fn resolve_sources(sources: Vec<String>) -> PyResult<Vec<String>> {
    let refs: Vec<&str> = sources.iter().map(|s| s.as_str()).collect();
    let paths =
        resolve::resolve_sources(&refs).map_err(|e| PyRuntimeError::new_err(format!("{e:#}")))?;
    Ok(paths.into_iter().map(|p| p.display().to_string()).collect())
}
