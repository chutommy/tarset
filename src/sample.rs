use std::sync::Arc;

/// A named blob within a [`Sample`].
pub struct Field {
    pub suffix: String,
    pub data: Vec<u8>,
}

/// A group of [`Field`]s sharing the same key within a tar archive.
pub struct Sample {
    pub key: String,
    pub url: Arc<str>,
    pub fields: Vec<Field>,
}
