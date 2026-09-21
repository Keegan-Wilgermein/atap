//! # Open file
//! A file held open, and the tasks that work on it through the
//! one descriptor

use crate::{
    RuntimeError,
    constants::{INLINE_PAYLOAD, LOCK_POLL},
    executor,
    futures::{
        file::{
            file_task::{directory, hint, open_at, read_range, write_all},
            metadata::Metadata,
        },
        task::{
            Nothing, Task,
            sealed::{self},
        },
    },
    modules::{c_path::c_path, fd::Fd, input::Token, int_check::IntCheck, retried::retried},
};
use std::{ffi::CString, fmt, mem, path::Path, sync::Arc, thread};

const _: () = assert!(mem::size_of::<Result<OpenFile, RuntimeError>>() <= INLINE_PAYLOAD);

/// Which kind of advisory lock to take
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockKind {
    /// Held by one open file at a time
    Exclusive,

    /// Held by any number at once, while nobody holds an exclusive one
    Shared,
}

/// Opens a file and keeps it open
///
/// ## Returns
/// The [`OpenFile`]
///
/// Read only until told otherwise. Every setting keeps the last
/// value it was given
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct OpenTask {
    /// The file to open
    path: Option<CString>,

    read: bool,
    write: bool,
    append: bool,
    create: bool,
    create_new: bool,
    truncate: bool,
}

impl OpenTask {
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            read: true,
            write: false,
            append: false,
            create: false,
            create_new: false,
            truncate: false,
        }
    }

    /// Whether the file can be read
    pub fn read(mut self, read: bool) -> Self {
        self.read = read;
        self
    }

    /// Whether the file can be written
    pub fn write(mut self, write: bool) -> Self {
        self.write = write;
        self
    }

    /// Whether every append goes on the end, even with other writers
    ///
    /// Implies `write`
    pub fn append(mut self, append: bool) -> Self {
        self.append = append;
        self
    }

    /// Makes the file if it isn't there
    ///
    /// Needs `write` or `append`
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Makes the file, and fails with `EEXIST` if it is already
    /// there
    ///
    /// Needs `write` or `append`
    pub fn create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Empties the file as it opens
    ///
    /// Needs `write`
    pub fn truncate(mut self, truncate: bool) -> Self {
        self.truncate = truncate;
        self
    }

    /// The flags `open` is given
    ///
    /// ## Returns
    /// [`RuntimeError::BadArgument`] for a mix that can't mean
    /// anything
    fn flags(&self) -> Result<libc::c_int, RuntimeError> {
        let writes = self.write || self.append;

        let mut flags = match (self.read, writes) {
            (true, false) => libc::O_RDONLY,
            (false, true) => libc::O_WRONLY,
            (true, true) => libc::O_RDWR,
            (false, false) => return Err(RuntimeError::BadArgument),
        };

        if (self.create || self.create_new) && !writes {
            return Err(RuntimeError::BadArgument);
        }

        if self.truncate && (!self.write || self.append) {
            return Err(RuntimeError::BadArgument);
        }

        if self.append {
            flags |= libc::O_APPEND;
        }

        if self.create_new {
            flags |= libc::O_CREAT | libc::O_EXCL;
        } else if self.create {
            flags |= libc::O_CREAT;
        }

        if self.truncate {
            flags |= libc::O_TRUNC;
        }

        Ok(flags)
    }
}

/// A file held open
///
/// ## Behaviour
/// Cloning it shares the one descriptor, which closes with the last
/// clone. Its methods build tasks that work on it. Reads and writes
/// always say where, so clones on different threads never move
/// anything under each other
///
/// ```no_run
/// # use atap::{Runtime, fs::File};
/// # fn main() -> Result<(), atap::RuntimeError> {
/// let file = Runtime::block(File::open("log.txt").write(true).create(true))?;
///
/// Runtime::block(file.write_at(0, b"hello".as_slice()))?;
/// Runtime::block(file.append(b" world".as_slice()))?;
///
/// assert_eq!(Runtime::block(file.read_at(6, 5))?, b"world");
/// # Ok(())
/// # }
/// ```
///
/// #### Note
/// A lock is the file's, not the handle's. Every clone shares it,
/// and it goes when the last clone does
#[derive(Clone)]
pub struct OpenFile {
    fd: Arc<Fd>,

    /// Whether it was opened to append
    appends: bool,
}

impl OpenFile {
    /// Reads `len` bytes from `offset`, or fewer at the end
    pub fn read_at(&self, offset: u64, len: usize) -> FileReadTask {
        FileReadTask {
            file: self.clone(),
            offset,
            len,
        }
    }

    /// Writes all of `data` at `offset`
    ///
    /// ## Returns
    /// The number of bytes written, which is all of them
    ///
    /// #### Note
    /// On a file opened to append, every write goes on the end
    /// whatever `offset` says
    pub fn write_at(&self, offset: u64, data: impl Into<Arc<[u8]>>) -> FileWriteTask {
        FileWriteTask {
            file: self.clone(),
            data: data.into(),
            at: Some(offset),
        }
    }

    /// Writes all of `data` on the end of the file
    ///
    /// ## Returns
    /// The number of bytes written, which is all of them
    ///
    /// #### Note
    /// Only a file opened to append is sure to put it at the end
    /// when something else writes there at the same time
    pub fn append(&self, data: impl Into<Arc<[u8]>>) -> FileWriteTask {
        FileWriteTask {
            file: self.clone(),
            data: data.into(),
            at: None,
        }
    }

    /// Makes the file exactly `len` bytes long
    pub fn set_len(&self, len: u64) -> FileOpTask {
        self.op(FileOp::SetLen(len))
    }

    /// Waits for everything written to reach the disk
    pub fn sync(&self) -> FileOpTask {
        self.op(FileOp::Sync)
    }

    /// Takes a lock on the file, waiting for anything else holding
    /// one that conflicts
    ///
    /// ## Behaviour
    /// Advisory: it only keeps out others who ask for a lock too.
    /// A lock already held by this file is changed to `kind`
    pub fn lock(&self, kind: LockKind) -> FileOpTask {
        self.op(FileOp::Lock(kind, true))
    }

    /// Takes a lock on the file if nothing conflicts right now
    ///
    /// ## Returns
    /// [`RuntimeError::NotReady`] if something else holds one
    ///
    /// [`RuntimeError::NotReady`]: crate::RuntimeError::NotReady
    pub fn try_lock(&self, kind: LockKind) -> FileOpTask {
        self.op(FileOp::Lock(kind, false))
    }

    /// Lets go of the file's lock
    pub fn unlock(&self) -> FileOpTask {
        self.op(FileOp::Unlock)
    }

    /// Asks what the file is, through the descriptor
    pub fn metadata(&self) -> FileMetadataTask {
        FileMetadataTask { file: self.clone() }
    }

    fn op(&self, op: FileOp) -> FileOpTask {
        FileOpTask {
            file: self.clone(),
            op,
        }
    }
}

impl fmt::Debug for OpenFile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OpenFile")
            .field("fd", &self.fd.raw())
            .finish_non_exhaustive()
    }
}

/// Which call a `FileOpTask` makes
#[derive(Debug, Clone, Copy)]
enum FileOp {
    /// `ftruncate`
    SetLen(u64),

    /// `fsync`
    Sync,

    /// `flock`, and whether to wait
    Lock(LockKind, bool),

    /// `flock` with `LOCK_UN`
    Unlock,
}

/// Reads part of an open file
///
/// ## Returns
/// The bytes read. A range past the end comes back empty
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct FileReadTask {
    file: OpenFile,
    offset: u64,
    len: usize,
}

/// Writes to an open file
///
/// ## Returns
/// The number of bytes written
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct FileWriteTask {
    file: OpenFile,
    data: Arc<[u8]>,

    /// Where, or `None` for the end
    at: Option<u64>,
}

/// Changes an open file, or its lock
///
/// ## Returns
/// Nothing once it is done
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct FileOpTask {
    file: OpenFile,
    op: FileOp,
}

/// Asks what an open file is
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct FileMetadataTask {
    file: OpenFile,
}

impl sealed::Sealed for OpenTask {}
impl sealed::Sealed for FileReadTask {}
impl sealed::Sealed for FileWriteTask {}
impl sealed::Sealed for FileOpTask {}
impl sealed::Sealed for FileMetadataTask {}

impl Task for OpenTask {
    type Output = Result<OpenFile, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;
        let fd = open_at(path, self.flags()?, 0o666)?;

        if (self.write || self.append) && directory(&fd)? {
            return Err(RuntimeError::CheckError(Some(libc::EISDIR)));
        }

        Ok(OpenFile {
            fd: Arc::new(fd),
            appends: self.append,
        })
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for FileReadTask {
    type Output = Result<Vec<u8>, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        read_range(&self.file.fd, self.offset, self.len)
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for FileWriteTask {
    type Output = Result<usize, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let fd = &self.file.fd;

        let at = match (self.at, self.file.appends) {
            // The kernel puts every write on the end
            (_, true) => None,
            (Some(at), false) => Some(at),
            (None, false) => Some(hint(fd)? as u64),
        };

        write_all(fd, &self.data, at)
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for FileOpTask {
    type Output = Result<(), RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let fd = self.file.fd.raw();

        match self.op {
            FileOp::SetLen(len) => {
                let len = libc::off_t::try_from(len)
                    .map_err(|_| RuntimeError::CheckError(Some(libc::EINVAL)))?;

                retried(|| unsafe { libc::ftruncate(fd, len) })?;
            }

            FileOp::Sync => {
                retried(|| unsafe { libc::fsync(fd) })?;
            }

            FileOp::Unlock => {
                retried(|| unsafe { libc::flock(fd, libc::LOCK_UN) })?;
            }

            FileOp::Lock(kind, wait) => lock(fd, kind, wait)?,
        }

        Ok(())
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for FileMetadataTask {
    type Output = Result<Metadata, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let mut raw: libc::stat = unsafe { mem::zeroed() };

        retried(|| unsafe { libc::fstat(self.file.fd.raw(), &mut raw) })?;

        Ok(Metadata::from_stat(&raw))
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

/// Takes a lock, asking again until it is free if `wait` says to
///
/// Asked without blocking each time, so a cancel is seen between
/// asks
fn lock(fd: libc::c_int, kind: LockKind, wait: bool) -> Result<(), RuntimeError> {
    let operation = match kind {
        LockKind::Exclusive => libc::LOCK_EX,
        LockKind::Shared => libc::LOCK_SH,
    } | libc::LOCK_NB;

    loop {
        match unsafe { libc::flock(fd, operation) }.check() {
            Ok(_) => return Ok(()),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,

            Err(RuntimeError::CheckError(Some(libc::EWOULDBLOCK))) if !wait => {
                return Err(RuntimeError::NotReady);
            }

            Err(RuntimeError::CheckError(Some(libc::EWOULDBLOCK))) => {}
            Err(error) => return Err(error),
        }

        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        thread::sleep(LOCK_POLL);
    }
}
