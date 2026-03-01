pub mod format;
pub mod reader;
pub mod resolve;
pub mod sample;
pub mod writer;

// Lustre file system (and many others) perform best with large sequential reads.
pub(crate) const LUSTRE_OPTIMAL_BUFFER: usize = 1024 * 1024 * 16;

pub use format::TarFormat;
pub use reader::SampleReader;
pub use sample::{Field, Sample};
pub use writer::SampleWriter;
