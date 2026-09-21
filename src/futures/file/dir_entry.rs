//! # Dir entry
//! One entry a directory listing found

use crate::futures::file::metadata::FileKind;
use std::path::{Path, PathBuf};

/// One entry in a directory
///
/// #### Note
/// A snapshot, taken when the listing ran
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DirEntry {
    /// The entry, joined onto the directory that was listed
    path: PathBuf,

    /// What kind of thing it is, without following a link
    kind: FileKind,
}

impl DirEntry {
    pub(crate) fn new(path: PathBuf, kind: FileKind) -> Self {
        Self { path, kind }
    }

    /// The entry's path, joined onto the directory that was listed
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Takes the path out
    pub fn into_path(self) -> PathBuf {
        self.path
    }

    /// What kind of thing it is
    ///
    /// A link is reported as a link, never as what it points at
    pub fn kind(&self) -> FileKind {
        self.kind
    }
}
