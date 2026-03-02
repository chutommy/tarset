pub mod consts;
pub mod format;
pub mod py;
pub mod reader;
pub mod resolve;
pub mod sample;
pub mod writer;

pub use format::TarFormat;
pub use reader::SampleReader;
pub use sample::{Field, Sample};
pub use writer::SampleWriter;

#[pyo3::pymodule]
mod tarset {
    use super::py;

    #[pymodule_export]
    use py::PyField as Field;
    #[pymodule_export]
    use py::PySample as Sample;
    #[pymodule_export]
    use py::PySampleReader as SampleReader;
    #[pymodule_export]
    use py::PySampleWriter as SampleWriter;
    #[pymodule_export]
    use py::resolve_sources;
}
