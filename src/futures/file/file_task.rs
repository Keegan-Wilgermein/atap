//! # File task
//! The tasks the `File` constructors return, and everything
//! they do once a thread picks them up

use crate::modules::input::Token;
use crate::{
    RuntimeError,
    constants::{FILE_CHUNK, INLINE_PAYLOAD},
    executor,
    futures::{
        file::{
            dir_entry::DirEntry,
            metadata::{FileKind, Metadata},
        },
        task::sealed,
        task::{Nothing, Task},
    },
    modules::{c_path::c_path, fd::Fd, int_check::IntCheck, retried::retried},
};
use std::{
    ffi::{CStr, CString, OsStr},
    io::Error,
    mem,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
    ptr, slice,
    sync::Arc,
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Vec<u8>, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<usize, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Metadata, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Vec<DirEntry>, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<(), RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<PathBuf, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<u64, RuntimeError>>() <= INLINE_PAYLOAD);

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

    /// `symlink`, with the link at the path and the target in `other`
    Symlink,

    /// `link`, with the new name in `other`
    HardLink,

    /// `chmod`
    SetPermissions(u32),

    /// `truncate`
    SetLen(u64),

    /// `mkdir` on every missing directory down to the path
    CreateDirAll,

    /// Everything under the path, then the path itself
    RemoveDirAll,
}

/// What a `PathBufTask` asks for
#[derive(Debug, Clone, Copy)]
enum Resolve {
    /// Where a symbolic link points
    ReadLink,

    /// The absolute path with every link and `..` taken out
    Canonical,
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
#[must_use = "a task does nothing until it is run or spawned"]
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
#[must_use = "a task does nothing until it is run or spawned"]
pub struct WriteTask {
    /// The file to write to
    path: Option<CString>,

    /// The bytes to put in it
    data: Arc<[u8]>,

    /// Where they go
    mode: WriteMode,
}

/// Asks what a path is
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
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
/// One entry per thing in it, each path joined onto the directory
/// that was asked for. `.` and `..` are left out
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ReadDirTask {
    /// The directory to list
    path: Option<CString>,
}

/// An operation on a path with no output
///
/// Everything but the two that walk a tree is a single syscall, and
/// can't be cancelled once it has started. The tree walks check
/// between entries
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct PathTask {
    /// The path acted on
    path: Option<CString>,

    /// Where a rename is going, `None` for everything else
    other: Option<CString>,

    /// Which call to make
    op: PathOp,
}

/// Asks the filesystem for a path
///
/// ## Returns
/// The path it answered with
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct PathBufTask {
    /// The path asked about
    path: Option<CString>,

    /// What is asked
    resolve: Resolve,
}

/// Copies a file's contents and metadata to another path
///
/// ## Returns
/// The number of bytes in the copy
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct CopyTask {
    /// The file copied
    from: Option<CString>,

    /// Where the copy goes
    to: Option<CString>,
}

impl PathBufTask {
    /// Reads where a link points
    pub(crate) fn read_link(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            resolve: Resolve::ReadLink,
        }
    }

    /// Resolves a path to its canonical form
    pub(crate) fn canonical(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            resolve: Resolve::Canonical,
        }
    }
}

impl CopyTask {
    /// Copies `from` to `to`
    pub(crate) fn new(from: impl AsRef<Path>, to: impl AsRef<Path>) -> Self {
        Self {
            from: c_path(from),
            to: c_path(to),
        }
    }
}

impl ReadTask {
    /// Reads from the start of the file to the end of it
    pub(crate) fn whole(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            extent: Extent::Whole,
        }
    }

    /// Reads `len` bytes from `offset`, or fewer at the end
    pub(crate) fn range(path: impl AsRef<Path>, offset: u64, len: usize) -> Self {
        Self {
            path: c_path(path),
            extent: Extent::Range { offset, len },
        }
    }
}

impl WriteTask {
    /// Replaces whatever was in the file
    pub(crate) fn truncate(path: impl AsRef<Path>, data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            path: c_path(path),
            data: data.into(),
            mode: WriteMode::Truncate,
        }
    }

    /// Adds to the end of whatever was in the file
    pub(crate) fn append(path: impl AsRef<Path>, data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            path: c_path(path),
            data: data.into(),
            mode: WriteMode::Append,
        }
    }

    /// Writes at a byte offset, leaving the rest of the file
    pub(crate) fn at(path: impl AsRef<Path>, offset: u64, data: impl Into<Arc<[u8]>>) -> Self {
        Self {
            path: c_path(path),
            data: data.into(),
            mode: WriteMode::At(offset),
        }
    }
}

impl MetadataTask {
    /// Follows a symbolic link before looking
    pub(crate) fn following(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            follow: true,
        }
    }

    /// Stops at the link rather than following it
    pub(crate) fn link(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            follow: false,
        }
    }
}

impl ReadDirTask {
    /// Lists the directory at `path`
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self { path: c_path(path) }
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
            path: c_path(from),
            other: c_path(to),
            op: PathOp::Rename,
        }
    }

    /// Makes a symbolic link at `link` pointing at `target`
    pub(crate) fn symlink(target: impl AsRef<Path>, link: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(link),
            other: c_path(target),
            op: PathOp::Symlink,
        }
    }

    /// Gives `existing` another name, `new`
    pub(crate) fn hard_link(existing: impl AsRef<Path>, new: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(existing),
            other: c_path(new),
            op: PathOp::HardLink,
        }
    }

    /// Sets a path's permission bits
    pub(crate) fn set_permissions(path: impl AsRef<Path>, mode: u32) -> Self {
        Self::one(path, PathOp::SetPermissions(mode))
    }

    /// Makes a file exactly `len` bytes long
    pub(crate) fn set_len(path: impl AsRef<Path>, len: u64) -> Self {
        Self::one(path, PathOp::SetLen(len))
    }

    /// Creates a directory and every missing one above it
    pub(crate) fn create_dir_all(path: impl AsRef<Path>) -> Self {
        Self::one(path, PathOp::CreateDirAll)
    }

    /// Removes a directory and everything in it
    pub(crate) fn remove_dir_all(path: impl AsRef<Path>) -> Self {
        Self::one(path, PathOp::RemoveDirAll)
    }

    /// The shape every op with one path has
    fn one(path: impl AsRef<Path>, op: PathOp) -> Self {
        Self {
            path: c_path(path),
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
impl sealed::Sealed for PathBufTask {}
impl sealed::Sealed for CopyTask {}

impl Task for ReadTask {
    type Output = Result<Vec<u8>, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
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
    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for WriteTask {
    type Output = Result<usize, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
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

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for MetadataTask {
    type Output = Result<Metadata, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;
        let mut raw: libc::stat = unsafe { mem::zeroed() };

        retried(|| match self.follow {
            true => unsafe { libc::stat(path.as_ptr(), &mut raw) },
            false => unsafe { libc::lstat(path.as_ptr(), &mut raw) },
        })?;

        Ok(Metadata::from_stat(&raw))
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for ReadDirTask {
    type Output = Result<Vec<DirEntry>, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
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

            let path = parent.join(OsStr::from_bytes(name));

            let kind = match unsafe { (*entry).d_type } {
                libc::DT_REG => FileKind::File,
                libc::DT_DIR => FileKind::Dir,
                libc::DT_LNK => FileKind::Symlink,
                libc::DT_UNKNOWN => kind_of(&path)?,
                _ => FileKind::Other,
            };

            found.push(DirEntry::new(path, kind));
        }

        Ok(found)
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for PathTask {
    type Output = Result<(), RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

        // Resolved first, so an op with nowhere to go answers `BadPath`
        let other = match self.op {
            PathOp::Rename | PathOp::Symlink | PathOp::HardLink => {
                Some(self.other.as_ref().ok_or(RuntimeError::BadPath)?)
            }
            _ => None,
        };

        match self.op {
            PathOp::CreateDirAll => return create_dir_all(path),
            PathOp::RemoveDirAll => return remove_dir_all(path),
            _ => {}
        }

        let mode = match self.op {
            PathOp::SetPermissions(mode) if mode <= 0o7777 => mode as libc::mode_t,
            PathOp::SetPermissions(_) => return Err(RuntimeError::BadArgument),
            _ => 0,
        };

        let len = match self.op {
            PathOp::SetLen(len) => seek_to(len)?,
            _ => 0,
        };

        retried(|| match (self.op, other) {
            (PathOp::Remove, _) => unsafe { libc::unlink(path.as_ptr()) },
            (PathOp::RemoveDir, _) => unsafe { libc::rmdir(path.as_ptr()) },
            (PathOp::CreateDir, _) => unsafe { libc::mkdir(path.as_ptr(), 0o777) },
            (PathOp::SetPermissions(_), _) => unsafe { libc::chmod(path.as_ptr(), mode) },
            (PathOp::SetLen(_), _) => unsafe { libc::truncate(path.as_ptr(), len) },
            (PathOp::Rename, Some(to)) => unsafe { libc::rename(path.as_ptr(), to.as_ptr()) },
            (PathOp::Symlink, Some(target)) => unsafe {
                libc::symlink(target.as_ptr(), path.as_ptr())
            },
            (PathOp::HardLink, Some(new)) => unsafe { libc::link(path.as_ptr(), new.as_ptr()) },
            _ => -1,
        })?;

        Ok(())
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for PathBufTask {
    type Output = Result<PathBuf, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

        match self.resolve {
            Resolve::ReadLink => read_link(path),
            Resolve::Canonical => canonical(path),
        }
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for CopyTask {
    type Output = Result<u64, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let from = self.from.as_ref().ok_or(RuntimeError::BadPath)?;
        let to = self.to.as_ref().ok_or(RuntimeError::BadPath)?;

        copy(from, to)
    }

    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

/// What a path is, without following a link, for a filesystem
/// that doesn't say in its listing
fn kind_of(path: &Path) -> Result<FileKind, RuntimeError> {
    let path = c_path(path).ok_or(RuntimeError::BadPath)?;
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::lstat(path.as_ptr(), &mut raw) })?;

    Ok(FileKind::from_mode(raw.st_mode))
}

/// Reads where a symbolic link points
fn read_link(path: &CStr) -> Result<PathBuf, RuntimeError> {
    let mut buffer = vec![0u8; libc::PATH_MAX as usize];

    loop {
        let read = unsafe {
            libc::readlink(
                path.as_ptr(),
                buffer.as_mut_ptr().cast::<libc::c_char>(),
                buffer.len(),
            )
        }
        .check();

        match read {
            // Filled, so the target may have been cut short
            Ok(read) if read as usize == buffer.len() => buffer.resize(buffer.len() * 2, 0),

            Ok(read) => {
                buffer.truncate(read as usize);

                return Ok(PathBuf::from(OsStr::from_bytes(&buffer)));
            }

            Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}
            Err(error) => return Err(error),
        }
    }
}

/// Resolves a path to the absolute one it names
fn canonical(path: &CStr) -> Result<PathBuf, RuntimeError> {
    let resolved = unsafe { libc::realpath(path.as_ptr(), ptr::null_mut()) };

    if resolved.is_null() {
        return Err(RuntimeError::CheckError(
            Error::last_os_error().raw_os_error(),
        ));
    }

    let found = PathBuf::from(OsStr::from_bytes(
        unsafe { CStr::from_ptr(resolved) }.to_bytes(),
    ));

    unsafe { libc::free(resolved.cast::<libc::c_void>()) };

    Ok(found)
}

/// Makes every directory down to `path` that isn't there yet
fn create_dir_all(path: &CStr) -> Result<(), RuntimeError> {
    let bytes = path.to_bytes();

    if bytes.is_empty() {
        return Err(RuntimeError::CheckError(Some(libc::ENOENT)));
    }

    // The end of every component, the root aside
    let ends = bytes
        .iter()
        .enumerate()
        .skip(1)
        .filter(|(at, byte)| **byte == b'/' && bytes[at - 1] != b'/')
        .map(|(at, _)| at)
        .chain([bytes.len()]);

    for end in ends {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let prefix = CString::new(&bytes[..end]).map_err(|_| RuntimeError::BadPath)?;

        match retried(|| unsafe { libc::mkdir(prefix.as_ptr(), 0o777) }) {
            Ok(_) => {}

            // Fine only if what is there is a directory
            Err(RuntimeError::CheckError(Some(libc::EEXIST))) => {
                let mut raw: libc::stat = unsafe { mem::zeroed() };

                retried(|| unsafe { libc::stat(prefix.as_ptr(), &mut raw) })?;

                if raw.st_mode & libc::S_IFMT != libc::S_IFDIR {
                    return Err(RuntimeError::CheckError(Some(libc::ENOTDIR)));
                }
            }

            Err(error) => return Err(error),
        }
    }

    Ok(())
}

/// A directory being emptied, and the entries in it still to go
struct Emptying {
    /// The open directory
    fd: Fd,

    /// Its name in the directory above it
    name: CString,

    /// Entries not yet removed
    left: Vec<CString>,
}

/// Removes `path`, and everything under it if it is a directory
///
/// ## Behaviour
/// A symbolic link is removed, never followed. Each directory is
/// opened with `O_NOFOLLOW` and worked on through its descriptor,
/// so a directory swapped for a link part way through isn't entered
fn remove_dir_all(path: &CStr) -> Result<(), RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::lstat(path.as_ptr(), &mut raw) })?;

    if raw.st_mode & libc::S_IFMT != libc::S_IFDIR {
        retried(|| unsafe { libc::unlink(path.as_ptr()) })?;

        return Ok(());
    }

    let root = open_dir(libc::AT_FDCWD, path)?;
    let left = entries(&root)?;

    let mut stack = vec![Emptying {
        fd: root,
        name: CString::default(),
        left,
    }];

    while let Some(top) = stack.last_mut() {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let Some(name) = top.left.pop() else {
            let done = stack.pop();

            // The root itself goes by its path, below
            if let (Some(parent), Some(done)) = (stack.last(), done) {
                retried(|| unsafe {
                    libc::unlinkat(parent.fd.raw(), done.name.as_ptr(), libc::AT_REMOVEDIR)
                })?;
            }

            continue;
        };

        let dir = top.fd.raw();
        let mut raw: libc::stat = unsafe { mem::zeroed() };

        retried(|| unsafe {
            libc::fstatat(dir, name.as_ptr(), &mut raw, libc::AT_SYMLINK_NOFOLLOW)
        })?;

        if raw.st_mode & libc::S_IFMT != libc::S_IFDIR {
            retried(|| unsafe { libc::unlinkat(dir, name.as_ptr(), 0) })?;
            continue;
        }

        let fd = open_dir(dir, &name)?;
        let left = entries(&fd)?;

        stack.push(Emptying { fd, name, left });
    }

    retried(|| unsafe { libc::rmdir(path.as_ptr()) })?;

    Ok(())
}

/// Opens a directory under `at` without following a link
fn open_dir(at: libc::c_int, name: &CStr) -> Result<Fd, RuntimeError> {
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC;

    loop {
        match unsafe { libc::openat(at, name.as_ptr(), flags) }.check() {
            Ok(fd) => return Ok(Fd::new(fd)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}
            Err(error) => return Err(error),
        }
    }
}

/// Every entry in an open directory, `.` and `..` aside
///
/// Read in full before anything is removed, since removing entries
/// while reading can skip some
fn entries(fd: &Fd) -> Result<Vec<CString>, RuntimeError> {
    let copy = retried(|| unsafe { libc::fcntl(fd.raw(), libc::F_DUPFD_CLOEXEC, 0) })?;
    let raw = unsafe { libc::fdopendir(copy) };

    if raw.is_null() {
        let failed = Error::last_os_error().raw_os_error();

        unsafe { libc::close(copy) };

        return Err(RuntimeError::CheckError(failed));
    }

    // The stream is read from the start, whoever read the descriptor
    unsafe { libc::rewinddir(raw) };

    let dir = Dir(raw);
    let mut found = Vec::new();

    loop {
        unsafe { *libc::__error() = 0 };

        let entry = unsafe { libc::readdir(dir.0) };

        if entry.is_null() {
            let failed = Error::last_os_error().raw_os_error().unwrap_or(0);

            if failed != 0 {
                return Err(RuntimeError::CheckError(Some(failed)));
            }

            return Ok(found);
        }

        let name = unsafe {
            slice::from_raw_parts(
                (*entry).d_name.as_ptr().cast::<u8>(),
                (*entry).d_namlen as usize,
            )
        };

        if name == b"." || name == b".." {
            continue;
        }

        found.push(CString::new(name).map_err(|_| RuntimeError::BadPath)?);
    }
}

/// Copies a file, as a clone where the filesystem allows
///
/// ## Returns
/// The size of the copy
fn copy(from: &CStr, to: &CStr) -> Result<u64, RuntimeError> {
    let source = open_at(from, libc::O_RDONLY, 0)?;

    if directory(&source)? {
        return Err(RuntimeError::CheckError(Some(libc::EISDIR)));
    }

    let size = hint(&source)? as u64;

    // Instant on APFS, and refused when `to` exists or the volume
    // can't clone
    if retried(|| unsafe { libc::fclonefileat(source.raw(), libc::AT_FDCWD, to.as_ptr(), 0) })
        .is_ok()
    {
        return Ok(size);
    }

    let target = open_at(to, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC, 0o666)?;

    let state = unsafe { libc::copyfile_state_alloc() };

    if state.is_null() {
        return Err(RuntimeError::CheckError(Some(libc::ENOMEM)));
    }

    let progress: extern "C" fn(
        libc::c_int,
        libc::c_int,
        libc::copyfile_state_t,
        *const libc::c_char,
        *const libc::c_char,
        *mut libc::c_void,
    ) -> libc::c_int = copy_progress;

    unsafe {
        libc::copyfile_state_set(
            state,
            libc::COPYFILE_STATE_STATUS_CB as u32,
            progress as *const libc::c_void,
        )
    };

    let copied = retried(|| unsafe {
        libc::fcopyfile(
            source.raw(),
            target.raw(),
            state,
            libc::COPYFILE_METADATA | libc::COPYFILE_DATA,
        )
    });

    unsafe { libc::copyfile_state_free(state) };

    match copied {
        Ok(_) => Ok(size),
        Err(RuntimeError::CheckError(Some(libc::ECANCELED))) => Err(RuntimeError::Cancelled),
        Err(error) => Err(error),
    }
}

/// Stops a copy between chunks once its task is stopped
extern "C" fn copy_progress(
    _what: libc::c_int,
    _stage: libc::c_int,
    _state: libc::copyfile_state_t,
    _from: *const libc::c_char,
    _to: *const libc::c_char,
    _context: *mut libc::c_void,
) -> libc::c_int {
    match executor::cancelled() {
        true => libc::COPYFILE_QUIT,
        false => libc::COPYFILE_CONTINUE,
    }
}

/// Opens a path, always closing on exec
///
/// `mode` is only used when the flags create the file, but
/// `open` is variadic so it is always passed
pub(crate) fn open_at(
    path: &CStr,
    flags: libc::c_int,
    mode: libc::c_int,
) -> Result<Fd, RuntimeError> {
    loop {
        // Only here and between chunks can a cancel land
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

/// Whether the open descriptor is a directory
pub(crate) fn directory(fd: &Fd) -> Result<bool, RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::fstat(fd.raw(), &mut raw) })?;

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
                fd.raw(),
                found
                    .spare_capacity_mut()
                    .as_mut_ptr()
                    .cast::<libc::c_void>(),
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
pub(crate) fn read_range(fd: &Fd, offset: u64, len: usize) -> Result<Vec<u8>, RuntimeError> {
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
                fd.raw(),
                found
                    .spare_capacity_mut()
                    .as_mut_ptr()
                    .cast::<libc::c_void>(),
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
pub(crate) fn write_all(fd: &Fd, data: &[u8], at: Option<u64>) -> Result<usize, RuntimeError> {
    let mut done = 0;

    while done < data.len() {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let want = (data.len() - done).min(FILE_CHUNK);
        let from = unsafe { data.as_ptr().add(done) }.cast::<libc::c_void>();

        let written = match at {
            None => unsafe { libc::write(fd.raw(), from, want) }.check(),
            Some(offset) => {
                let to = seek_to(offset.saturating_add(done as u64))?;

                unsafe { libc::pwrite(fd.raw(), from, want, to) }.check()
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
pub(crate) fn hint(fd: &Fd) -> Result<usize, RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::fstat(fd.raw(), &mut raw) })?;

    Ok(raw.st_size.max(0) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::input::token;

    /// Every file task says it holds its thread
    #[test]
    fn every_file_task_says_it_blocks() {
        assert!(ReadTask::whole("a").blocking(token()), "read");
        assert!(ReadTask::range("a", 0, 1).blocking(token()), "read_at");
        assert!(
            WriteTask::truncate("a", b"b".as_slice()).blocking(token()),
            "write"
        );
        assert!(
            WriteTask::append("a", b"b".as_slice()).blocking(token()),
            "append"
        );
        assert!(
            WriteTask::at("a", 0, b"b".as_slice()).blocking(token()),
            "write_at"
        );
        assert!(MetadataTask::following("a").blocking(token()), "metadata");
        assert!(
            MetadataTask::link("a").blocking(token()),
            "symlink_metadata"
        );
        assert!(ReadDirTask::new("a").blocking(token()), "read_dir");
        assert!(PathTask::remove("a").blocking(token()), "remove");
        assert!(PathTask::remove_dir("a").blocking(token()), "remove_dir");
        assert!(PathTask::create_dir("a").blocking(token()), "create_dir");
        assert!(PathTask::rename("a", "b").blocking(token()), "rename");
    }
}
