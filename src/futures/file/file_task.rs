//! # File task
//! The tasks the `File` constructors return, and everything
//! they do once a thread picks them up

use crate::{
    RuntimeError,
    constants::{FILE_CHUNK, INLINE_PAYLOAD},
    executor,
    futures::{file::metadata::Metadata, task::Task, task::sealed},
    modules::int_check::IntCheck,
};
use std::{
    ffi::{CString, OsStr},
    io::Error,
    mem,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    slice,
    sync::Arc,
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Vec<u8>, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<usize, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Metadata, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Vec<PathBuf>, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<(), RuntimeError>>() <= INLINE_PAYLOAD);

/// How much of a file a read wants
#[derive(Debug, Clone, Copy)]
enum Extent {
    /// From the start to the end
    Whole,

    /// `len` bytes from `offset`, or fewer at the end of the
    /// file
    Range { offset: u64, len: usize },
}

/// Where a write puts its bytes
#[derive(Debug, Clone, Copy)]
enum WriteMode {
    /// Over whatever was there, from the start
    ///
    /// #### Note
    /// Truncates at the open, before the first place a cancel
    /// can land
    Truncate,

    /// On the end of whatever was there
    Append,

    /// At a byte offset, leaving the rest alone
    At(u64),
}

/// Which single syscall a `PathTask` is
#[derive(Debug, Clone, Copy)]
enum PathOp {
    /// `unlink`
    Remove,

    /// `rmdir`
    RemoveDir,

    /// `mkdir`
    CreateDir,

    /// `rename`
    Rename,
}

/// An open descriptor that closes itself
///
/// #### Note
/// Closing in `Drop` also keeps errno intact, since the guard
/// drops after the error value has been built
#[derive(Debug)]
pub(super) struct Fd(libc::c_int);

impl Fd {
    /// Takes ownership of a descriptor the kernel just handed out
    #[inline(always)]
    pub(super) fn new(fd: libc::c_int) -> Self {
        Self(fd)
    }

    /// The number, for handing to a syscall
    #[inline(always)]
    pub(super) fn raw(&self) -> libc::c_int {
        self.0
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// An open directory stream that closes itself
struct Dir(*mut libc::DIR);

impl Drop for Dir {
    fn drop(&mut self) {
        unsafe { libc::closedir(self.0) };
    }
}

/// Reads a file, or a range of one
///
/// ## Returns
/// The bytes read. A range that starts past the end of the file
/// comes back empty rather than as an error
#[derive(Debug, Clone)]
pub struct ReadTask {
    /// The file to read, already in the form the kernel takes
    ///
    /// `None` when the path had a zero byte in it, which is
    /// reported when the task runs
    path: Option<CString>,

    /// How much of it to read
    extent: Extent,
}

/// Writes bytes to a file
///
/// ## Returns
/// The number of bytes written, which is the length of the
/// input whenever it isn't an error
#[derive(Debug, Clone)]
pub struct WriteTask {
    /// The file to write to
    path: Option<CString>,

    /// The bytes to put in it
    ///
    /// An `Arc` so `.at_rate()` doesn't copy the buffer every run
    data: Arc<[u8]>,

    /// Where they go
    mode: WriteMode,
}

/// Asks what a path is
#[derive(Debug, Clone)]
pub struct MetadataTask {
    /// The path to ask about
    path: Option<CString>,

    /// Whether to follow a symbolic link to whatever it points
    /// at, or stop at the link
    follow: bool,
}

/// Lists what is in a directory
///
/// ## Returns
/// One path per entry, each joined onto the directory that was
/// asked for. `.` and `..` are left out
#[derive(Debug, Clone)]
pub struct ReadDirTask {
    /// The directory to list
    path: Option<CString>,
}

/// One of the operations that is a single syscall and no output
///
/// Not cancellable once it has started
#[derive(Debug, Clone)]
pub struct PathTask {
    /// The path acted on
    path: Option<CString>,

    /// Where a rename is going, `None` for everything else
    other: Option<CString>,

    /// Which call to make
    op: PathOp,
}

impl ReadTask {
    /// Reads from the start of the file to the end of it
    pub(crate) fn whole(path: impl AsRef<Path>) -> Self {
        Self {
            path: as_c_path(path),
            extent: Extent::Whole,
        }
    }

    /// Reads `len` bytes from `offset`, or fewer at the end
    pub(crate) fn range(path: impl AsRef<Path>, offset: u64, len: usize) -> Self {
        Self {
            path: as_c_path(path),
            extent: Extent::Range { offset, len },
        }
    }
}

impl WriteTask {
    /// Replaces whatever was in the file
    pub(crate) fn truncate(path: impl AsRef<Path>, data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            path: as_c_path(path),
            data: data.into(),
            mode: WriteMode::Truncate,
        }
    }

    /// Adds to the end of whatever was in the file
    pub(crate) fn append(path: impl AsRef<Path>, data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            path: as_c_path(path),
            data: data.into(),
            mode: WriteMode::Append,
        }
    }

    /// Writes at a byte offset, leaving the rest of the file
    pub(crate) fn at(path: impl AsRef<Path>, offset: u64, data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            path: as_c_path(path),
            data: data.into(),
            mode: WriteMode::At(offset),
        }
    }
}

impl MetadataTask {
    /// Follows a symbolic link before looking
    pub(crate) fn following(path: impl AsRef<Path>) -> Self {
        Self {
            path: as_c_path(path),
            follow: true,
        }
    }

    /// Stops at the link rather than following it
    pub(crate) fn link(path: impl AsRef<Path>) -> Self {
        Self {
            path: as_c_path(path),
            follow: false,
        }
    }
}

impl ReadDirTask {
    /// Lists the directory at `path`
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: as_c_path(path),
        }
    }
}

impl PathTask {
    /// Removes a file
    pub(crate) fn remove(path: impl AsRef<Path>) -> Self {
        Self::one(path, PathOp::Remove)
    }

    /// Removes an empty directory
    pub(crate) fn remove_dir(path: impl AsRef<Path>) -> Self {
        Self::one(path, PathOp::RemoveDir)
    }

    /// Creates a directory
    pub(crate) fn create_dir(path: impl AsRef<Path>) -> Self {
        Self::one(path, PathOp::CreateDir)
    }

    /// Moves a path to another one
    pub(crate) fn rename(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Self {
        Self {
            path: as_c_path(from),
            other: as_c_path(to),
            op: PathOp::Rename,
        }
    }

    /// The shape every op but `rename` has
    fn one(path: impl AsRef<Path>, op: PathOp) -> Self {
        Self {
            path: as_c_path(path),
            other: None,
            op,
        }
    }
}

impl sealed::Sealed for ReadTask {}
impl sealed::Sealed for WriteTask {}
impl sealed::Sealed for MetadataTask {}
impl sealed::Sealed for ReadDirTask {}
impl sealed::Sealed for PathTask {}

impl Task for ReadTask {
    type Output = Result<Vec<u8>, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;
        let fd = open_at(path, libc::O_RDONLY, 0)?;

        // A directory opens for reading and then refuses every read
        // with a code that says nothing about why
        if directory(&fd)? {
            return Err(RuntimeError::CheckError(Some(libc::EISDIR)));
        }

        match self.extent {
            Extent::Whole => read_whole(&fd),
            Extent::Range { offset, len } => read_range(&fd, offset, len),
        }
    }

    /// Held for its whole duration, so it goes to a sleep thread
    fn blocking(&self) -> bool {
        true
    }
}

impl Task for WriteTask {
    type Output = Result<usize, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

        // `O_APPEND` would move a positional write to the end
        let flags = match self.mode {
            WriteMode::Truncate => libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            WriteMode::Append => libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
            WriteMode::At(_) => libc::O_WRONLY | libc::O_CREAT,
        };

        let fd = open_at(path, flags, 0o666)?;

        let at = match self.mode {
            WriteMode::At(offset) => Some(offset),
            _ => None,
        };

        write_all(&fd, &self.data, at)
    }

    fn blocking(&self) -> bool {
        true
    }
}

impl Task for MetadataTask {
    type Output = Result<Metadata, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;
        let mut raw: libc::stat = unsafe { mem::zeroed() };

        retried(|| match self.follow {
            true => unsafe { libc::stat(path.as_ptr(), &mut raw) },
            false => unsafe { libc::lstat(path.as_ptr(), &mut raw) },
        })?;

        Ok(Metadata::from_stat(&raw))
    }

    fn blocking(&self) -> bool {
        true
    }
}

impl Task for ReadDirTask {
    type Output = Result<Vec<PathBuf>, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

        let raw = unsafe { libc::opendir(path.as_ptr()) };

        // `opendir` reports failure with a null, so the errno has to
        // be read directly
        if raw.is_null() {
            return Err(RuntimeError::CheckError(
                Error::last_os_error().raw_os_error(),
            ));
        }

        let dir = Dir(raw);
        let parent = PathBuf::from(OsStr::from_bytes(path.as_bytes()));
        let mut found = Vec::new();

        loop {
            if executor::cancelled() {
                return Err(RuntimeError::Cancelled);
            }

            // `readdir` reports the end and a failure with the same null,
            // so errno is cleared first and read back on a null
            unsafe { *libc::__error() = 0 };

            let entry = unsafe { libc::readdir(dir.0) };

            if entry.is_null() {
                let failed = Error::last_os_error().raw_os_error().unwrap_or(0);

                if failed != 0 {
                    return Err(RuntimeError::CheckError(Some(failed)));
                }

                break;
            }

            // `d_namlen` rather than looking for the zero, so a name that
            // fills the array is still measured right
            let name = unsafe {
                slice::from_raw_parts(
                    (*entry).d_name.as_ptr().cast::<u8>(),
                    (*entry).d_namlen as usize,
                )
            };

            if name == b"." || name == b".." {
                continue;
            }

            found.push(parent.join(OsStr::from_bytes(name)));
        }

        Ok(found)
    }

    fn blocking(&self) -> bool {
        true
    }
}

impl Task for PathTask {
    type Output = Result<(), RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

        // Resolved first, so a rename with nowhere to go answers
        // `BadPath`
        let to = match self.op {
            PathOp::Rename => Some(self.other.as_ref().ok_or(RuntimeError::BadPath)?),
            _ => None,
        };

        retried(|| match self.op {
            PathOp::Remove => unsafe { libc::unlink(path.as_ptr()) },
            PathOp::RemoveDir => unsafe { libc::rmdir(path.as_ptr()) },
            PathOp::CreateDir => unsafe { libc::mkdir(path.as_ptr(), 0o777) },

            PathOp::Rename => match to {
                Some(to) => unsafe { libc::rename(path.as_ptr(), to.as_ptr()) },
                None => -1,
            },
        })?;

        Ok(())
    }

    fn blocking(&self) -> bool {
        true
    }
}

/// Turns a path into the form the kernel takes
///
/// ## Returns
/// `None` when the path has a zero byte in it, since passing
/// the part before it would act on a different file
pub(super) fn as_c_path(path: impl AsRef<Path>) -> Option<CString> {
    CString::new(path.as_ref().as_os_str().as_bytes()).ok()
}

/// Opens a path, always closing on exec
///
/// `mode` is only used when the flags create the file, but
/// `open` is variadic so it is always passed
fn open_at(path: &CString, flags: libc::c_int, mode: libc::c_int) -> Result<Fd, RuntimeError> {
    loop {
        // Nothing registers on a queue, so this and the chunk checks
        // are the only places a cancel can land. A `FIFO` with no
        // writer never reaches a chunk
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        // Or the descriptor is inherited by anything the user forks
        let raw = unsafe { libc::open(path.as_ptr(), flags | libc::O_CLOEXEC, mode) }.check();

        match raw {
            Ok(fd) => return Ok(Fd::new(fd)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        }
    }
}

/// Runs a syscall until it says something other than `EINTR`
pub(super) fn retried(mut call: impl FnMut() -> libc::c_int) -> Result<libc::c_int, RuntimeError> {
    loop {
        match call().check() {
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            other => return other,
        }
    }
}

/// Whether the open descriptor is a directory
fn directory(fd: &Fd) -> Result<bool, RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::fstat(fd.0, &mut raw) })?;

    Ok(raw.st_mode & libc::S_IFMT == libc::S_IFDIR)
}

/// The largest read that still gets its slack handed back
const SHRINK_CEILING: usize = 64 * FILE_CHUNK;

/// Reads from wherever the descriptor is to the end of the file
///
/// #### Note
/// The size a `stat` reports is only used to reserve. Only a
/// read returning zero ends this
fn read_whole(fd: &Fd) -> Result<Vec<u8>, RuntimeError> {
    let mut found = Vec::new();

    // `reserve` aborts on a size the filesystem lied about, which
    // `catch_unwind` can't contain
    if let Ok(size) = hint(fd) {
        let _ = found.try_reserve(size);
    }

    loop {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        found.reserve(FILE_CHUNK);

        let read = unsafe {
            libc::read(
                fd.0,
                found.spare_capacity_mut().as_mut_ptr().cast::<libc::c_void>(),
                FILE_CHUNK,
            )
        }
        .check();

        let got = match read {
            Ok(got) => got as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        };

        if got == 0 {
            // The read that finds the end leaves a chunk of slack, handed
            // back only while the copy behind a shrink is cheap
            let spare = found.capacity() - found.len();

            if spare >= FILE_CHUNK && found.len() <= SHRINK_CEILING {
                found.shrink_to_fit();
            }

            return Ok(found);
        }

        // The kernel just wrote `got` bytes into the reserved capacity
        unsafe { found.set_len(found.len() + got) };
    }
}

/// Reads `len` bytes from `offset`, or fewer at the end
///
/// ## Returns
/// Up to `len` bytes. Short means the file ended, which is an
/// answer rather than a failure
fn read_range(fd: &Fd, offset: u64, len: usize) -> Result<Vec<u8>, RuntimeError> {
    let mut found = Vec::new();

    while found.len() < len {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let want = (len - found.len()).min(FILE_CHUNK);
        let at = seek_to(offset.saturating_add(found.len() as u64))?;

        found.reserve(want);

        let read = unsafe {
            libc::pread(
                fd.0,
                found.spare_capacity_mut().as_mut_ptr().cast::<libc::c_void>(),
                want,
                at,
            )
        }
        .check();

        let got = match read {
            Ok(got) => got as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        };

        // The end of the file
        if got == 0 {
            break;
        }

        unsafe { found.set_len(found.len() + got) };
    }

    Ok(found)
}

/// Writes the whole buffer, however many calls that takes
///
/// `at` is `Some` for a positional write and `None` for one
/// that follows the descriptor
fn write_all(fd: &Fd, data: &[u8], at: Option<u64>) -> Result<usize, RuntimeError> {
    let mut done = 0;

    while done < data.len() {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let want = (data.len() - done).min(FILE_CHUNK);
        let from = unsafe { data.as_ptr().add(done) }.cast::<libc::c_void>();

        let written = match at {
            None => unsafe { libc::write(fd.0, from, want) }.check(),
            Some(offset) => {
                let to = seek_to(offset.saturating_add(done as u64))?;

                unsafe { libc::pwrite(fd.0, from, want, to) }.check()
            }
        };

        let put = match written {
            Ok(put) => put as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        };

        // Can't happen for a regular file and a non empty buffer, and
        // carrying on would never end
        if put == 0 {
            return Err(RuntimeError::CheckError(Some(libc::ENOSPC)));
        }

        done += put;
    }

    Ok(done)
}

/// Turns a byte position into the signed offset the kernel
/// takes
///
/// ## Returns
/// `EINVAL` for anything past `i64::MAX`, the same as the
/// syscall answers a negative offset with
fn seek_to(offset: u64) -> Result<libc::off_t, RuntimeError> {
    libc::off_t::try_from(offset).map_err(|_| RuntimeError::CheckError(Some(libc::EINVAL)))
}

/// How much to reserve before the first read
fn hint(fd: &Fd) -> Result<usize, RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::fstat(fd.0, &mut raw) })?;

    Ok(raw.st_size.max(0) as usize)
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Every file task says it holds its thread
    #[test]
    fn every_file_task_says_it_blocks() {
        assert!(ReadTask::whole("a").blocking(), "read");
        assert!(ReadTask::range("a", 0, 1).blocking(), "read_at");
        assert!(WriteTask::truncate("a", b"b".as_slice()).blocking(), "write");
        assert!(WriteTask::append("a", b"b".as_slice()).blocking(), "append");
        assert!(WriteTask::at("a", 0, b"b".as_slice()).blocking(), "write_at");
        assert!(MetadataTask::following("a").blocking(), "metadata");
        assert!(MetadataTask::link("a").blocking(), "symlink_metadata");
        assert!(ReadDirTask::new("a").blocking(), "read_dir");
        assert!(PathTask::remove("a").blocking(), "remove");
        assert!(PathTask::remove_dir("a").blocking(), "remove_dir");
        assert!(PathTask::create_dir("a").blocking(), "create_dir");
        assert!(PathTask::rename("a", "b").blocking(), "rename");
    }

    /// A path with a zero byte in it doesn't convert
    #[test]
    fn a_path_with_a_zero_byte_does_not_convert() {
        assert!(as_c_path("a\0b").is_none(), "a zero byte must not convert");
        assert!(as_c_path("ab").is_some(), "an ordinary path must convert");
    }
}
