use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

use anyhow::{Context, Result};

use crate::TarFormat;
use crate::consts::LUSTRE_OPTIMAL_BUFFER;
use crate::sample::Sample;

type Buf = BufWriter<File>;

/// Extension of [`Write`] that can finalize a compression stream.
trait FinishableWrite: Write {
    fn finish(self: Box<Self>) -> io::Result<()>;
}

impl FinishableWrite for File {
    fn finish(mut self: Box<Self>) -> io::Result<()> {
        self.flush()
    }
}

impl FinishableWrite for Buf {
    fn finish(mut self: Box<Self>) -> io::Result<()> {
        self.flush()
    }
}

macro_rules! impl_finishable {
    ($($t:ty),+ $(,)?) => {$(
        impl FinishableWrite for $t {
            fn finish(self: Box<Self>) -> io::Result<()> {
                (*self).finish()?.flush()
            }
        }
    )+};
}

impl_finishable!(
    flate2::write::GzEncoder<Buf>,
    bzip2::write::BzEncoder<Buf>,
    xz2::write::XzEncoder<Buf>,
    zstd::stream::Encoder<'static, Buf>,
);

/// Create a writer based on [`TarFormat`].
///
/// Compressed formats get a `BufWriter` between file and compressor so that
/// compressed output is flushed to disk in large blocks. Plain `.tar` skips
/// this because the caller already provides an outer `BufWriter`.
///
/// TODO: the caller wraps this in another LUSTRE_OPTIMAL_BUFFER-sized BufWriter,
/// so compressed formats are double-buffered (~32 MiB). Benchmark whether
/// removing the inner buffer (or reducing its size) hurts throughput.
fn open_tar_file(path: &Path) -> Result<Box<dyn FinishableWrite + 'static>> {
    let file = File::create(path)
        .with_context(|| format!("Failed to create archive at: {}", path.display()))?;
    let format = TarFormat::from_path(path).unwrap_or(TarFormat::Tar);

    let writer: Box<dyn FinishableWrite> = match format {
        TarFormat::Tar => Box::new(file),
        // TODO: benchmark compression levels vs throughput for each codec.
        // Current values are reasonable defaults but may not be optimal for
        // the typical tar-of-many-small-blobs workload.
        TarFormat::TarGz | TarFormat::Tgz => {
            let buf = BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, file);
            let default = flate2::Compression::default(); // level 6
            Box::new(flate2::write::GzEncoder::new(buf, default))
        }
        TarFormat::TarBz2 => {
            let buf = BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, file);
            let default = bzip2::Compression::default(); // level 6
            Box::new(bzip2::write::BzEncoder::new(buf, default))
        }
        TarFormat::TarXz => {
            let buf = BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, file);
            Box::new(xz2::write::XzEncoder::new(buf, 6))
        }
        TarFormat::TarZst => {
            let buf = BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, file);
            Box::new(zstd::stream::Encoder::new(buf, 3)?)
        }
    };

    Ok(writer)
}

/// Writes [`Sample`]s into a tar archive as the inverse of
/// [`SampleReader`](crate::reader::SampleReader).
pub struct SampleWriter {
    builder: tar::Builder<BufWriter<Box<dyn FinishableWrite>>>,
}

impl SampleWriter {
    pub fn create(path: &Path) -> Result<Self> {
        let writer = BufWriter::with_capacity(LUSTRE_OPTIMAL_BUFFER, open_tar_file(path)?);
        Ok(Self {
            builder: tar::Builder::new(writer),
        })
    }

    /// Write all fields of a sample as tar entries at `{key}.{suffix}`.
    ///
    /// The key's basename must not contain `.`; the reader splits at the first
    /// `.` in the basename, so a dot in the key would corrupt the round-trip.
    pub fn write_sample(&mut self, sample: &Sample) -> Result<()> {
        let basename_start = sample.key.rfind('/').map_or(0, |i| i + 1);
        anyhow::ensure!(
            !sample.key[basename_start..].contains('.'),
            "sample key basename must not contain '.': {:?}",
            sample.key,
        );

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
        let mut buf = self.builder.into_inner()?;
        buf.flush()?;
        let writer = buf.into_inner().map_err(|e| e.into_error())?;
        writer.finish()?;
        Ok(())
    }
}
