//! # Metadata
//! What a `stat` says, in a shape that fits in a task slot

use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What kind of thing a path turned out to be
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FileKind {
    /// An ordinary file
    File,

    /// A directory
    Dir,

    /// A symbolic link
    ///
    /// Only ever seen through `symlink_metadata`
    Symlink,

    /// A socket, fifo, device, or anything else
    Other,
}

/// What a `stat` said about a path
///
/// #### Note
/// A snapshot, taken when the task ran. Nothing keeps it up to
/// date
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Metadata {
    /// Length in bytes
    len: u64,

    /// The full mode word, permissions and type together
    mode: u32,

    /// Which of the mode word's type bits were set
    kind: FileKind,

    /// Last write
    modified: SystemTime,

    /// Last read
    accessed: SystemTime,

    /// When it came into being
    created: SystemTime,
}

impl Metadata {
    /// Copies the fields worth keeping out of a `stat`
    pub(crate) fn from_stat(raw: &libc::stat) -> Self {
        Self {
            len: raw.st_size.max(0) as u64,
            mode: raw.st_mode as u32,
            kind: FileKind::from_mode(raw.st_mode),
            modified: stamp(raw.st_mtime, raw.st_mtime_nsec),
            accessed: stamp(raw.st_atime, raw.st_atime_nsec),
            created: stamp(raw.st_birthtime, raw.st_birthtime_nsec),
        }
    }

    /// The length in bytes
    ///
    /// Meaningless for anything that isn't a file
    pub fn len(&self) -> u64 {
        self.len
    }

    /// Whether there is nothing in it
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The mode word, permissions and type bits together
    pub fn mode(&self) -> u32 {
        self.mode
    }

    /// What kind of thing it is
    pub fn kind(&self) -> FileKind {
        self.kind
    }

    /// Whether it is an ordinary file
    pub fn is_file(&self) -> bool {
        self.kind == FileKind::File
    }

    /// Whether it is a directory
    pub fn is_dir(&self) -> bool {
        self.kind == FileKind::Dir
    }

    /// Whether it is a symbolic link
    ///
    /// Never true through `metadata`, which follows the link
    pub fn is_symlink(&self) -> bool {
        self.kind == FileKind::Symlink
    }

    /// When it was last written
    pub fn modified(&self) -> SystemTime {
        self.modified
    }

    /// When it was last read
    pub fn accessed(&self) -> SystemTime {
        self.accessed
    }

    /// When it was created
    pub fn created(&self) -> SystemTime {
        self.created
    }
}

impl FileKind {
    /// Reads the type bits out of a mode word
    fn from_mode(mode: libc::mode_t) -> Self {
        match mode & libc::S_IFMT {
            libc::S_IFREG => Self::File,
            libc::S_IFDIR => Self::Dir,
            libc::S_IFLNK => Self::Symlink,
            _ => Self::Other,
        }
    }
}

/// Turns a seconds and nanoseconds pair into a `SystemTime`
///
/// Anything that can't be represented settles on the epoch
fn stamp(secs: libc::time_t, nanos: libc::c_long) -> SystemTime {
    let nanos = nanos.clamp(0, 999_999_999) as u32;

    if secs >= 0 {
        return UNIX_EPOCH
            .checked_add(Duration::new(secs as u64, nanos))
            .unwrap_or(UNIX_EPOCH);
    }

    UNIX_EPOCH
        .checked_sub(Duration::from_secs(secs.unsigned_abs()))
        .and_then(|time| time.checked_add(Duration::from_nanos(nanos as u64)))
        .unwrap_or(UNIX_EPOCH)
}
