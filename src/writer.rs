use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::TarFormat;
use crate::sample::Sample;

// Lustre file system (and many others) perform best with large sequential reads.
const LUSTRE_OPTIMAL_BUFFER: usize = 1024 * 1024 * 16;

/// Writes [`Sample`]s into a tar archive, the inverse of
/// [`SampleReader`](crate::reader::SampleReader).
///
/// Format (and compression) is detected from the file extension.
/// Both the raw file and compressor are wrapped in 16 MB buffers.
pub struct SampleWriter {
    builder: tar::Builder<Box<dyn Write>>,
}

impl SampleWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create archive at: {}", path.display()))?;
        let buf = BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, file);
        let format = TarFormat::from_path(path).unwrap_or(TarFormat::Tar);

        let writer: Box<dyn Write> = match format {
            TarFormat::Tar => Box::new(buf),
            TarFormat::TarGz | TarFormat::Tgz => Box::new(BufWriter::with_capacity(
                LUSTRE_OPTIMAL_BUFFER,
                flate2::write::GzEncoder::new(buf, flate2::Compression::default()),
            )),
            TarFormat::TarBz2 => Box::new(BufWriter::with_capacity(
                LUSTRE_OPTIMAL_BUFFER,
                bzip2::write::BzEncoder::new(buf, bzip2::Compression::default()),
            )),
            TarFormat::TarXz => Box::new(BufWriter::with_capacity(
                LUSTRE_OPTIMAL_BUFFER,
                xz2::write::XzEncoder::new(buf, 6),
            )),
            TarFormat::TarZst => {
                let encoder = zstd::stream::Encoder::new(buf, 3)?;
                Box::new(BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, encoder))
            }
        };

        Ok(Self {
            builder: tar::Builder::new(writer),
        })
    }

    /// Write all fields of a sample as tar entries at `{key}.{suffix}`.
    pub fn write_sample(&mut self, sample: &Sample) -> Result<()> {
        for field in &sample.fields {
            let path = format!("{}.{}", sample.key, field.suffix);
            let mut header = tar::Header::new_gnu();
            header.set_size(field.data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            self.builder.append_data(&mut header, &path, &*field.data)?;
        }
        Ok(())
    }

    /// Finalize the archive, flushing all buffers and compression trailers.
    pub fn finish(self) -> Result<()> {
        self.builder.into_inner()?.flush()?;
        Ok(())
    }
}
