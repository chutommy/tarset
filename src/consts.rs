// Lustre file system (and many others) perform best with large sequential reads.
// TODO: benchmark 4 MiB, 8 MiB, 16 MiB, 32 MiB on Lustre vs NVMe vs networked FS.
// A single constant is used for both read and write, it may be worth
// having separate values (reads may benefit from larger buffers than writes).
pub(crate) const LUSTRE_OPTIMAL_BUFFER: usize = 1024 * 1024 * 16;
