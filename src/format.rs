use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TarFormat {
    Tar,
    TarGz,
    Tgz,
    TarBz2,
    TarXz,
    TarZst,
}

impl TarFormat {
    const ALL: &[TarFormat] = &[
        TarFormat::Tar,
        TarFormat::TarGz,
        TarFormat::Tgz,
        TarFormat::TarBz2,
        TarFormat::TarXz,
        TarFormat::TarZst,
    ];

    pub const fn extension(self) -> &'static str {
        match self {
            TarFormat::Tar => ".tar",
            TarFormat::TarGz => ".tar.gz",
            TarFormat::Tgz => ".tgz",
            TarFormat::TarBz2 => ".tar.bz2",
            TarFormat::TarXz => ".tar.xz",
            TarFormat::TarZst => ".tar.zst",
        }
    }

    pub fn from_path(path: &Path) -> Option<TarFormat> {
        let name = path.file_name()?.to_str()?;
        Self::ALL
            .iter()
            .find(|fmt| name.ends_with(fmt.extension()))
            .copied()
    }
}
