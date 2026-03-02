// Lustre file system (and many others) perform best with large sequential reads.
pub(crate) const LUSTRE_OPTIMAL_BUFFER: usize = 1024 * 1024 * 16;
