//! # File task
//! The tasks the `File` constructors return, and everything
//! they do once a thread picks them up
//!
//! Five types rather than one per operation, grouped by what
//! they hand back. A caller reading a file gets a `Vec<u8>` or
//! an error and nothing else to match on, which is the whole
//! reason these aren't one task with an output enum

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

// The whole reason `Metadata` is a hand written struct rather
// than a `libc::stat`. Crossing this line doesn't cost an
// allocation, it costs a page mapping per task
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
/// ## Behaviour
/// Every file task has several ways out — an error part way
/// through, a cancel between chunks, or a panic unwinding
/// through `catch_unwind` — and a descriptor leaked from a
/// sleep thread is leaked for the life of the process
///
/// #### Note
/// Closing in `Drop` rather than by hand is also what keeps
/// errno intact. `check` reads the errno the last call set, so
/// a `close` between a failed read and its check would report
/// the close's success instead of the read's failure. A guard
/// drops after the error value has already been built
struct Fd(libc::c_int);

impl Drop for Fd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// An open directory stream that closes itself
///
/// The same bargain as `Fd`, for the one operation that gets a
/// `DIR *` rather than a descriptor
struct Dir(*mut libc::DIR);

impl Drop for Dir {
    fn drop(&mut self) {
        unsafe { libc::closedir(self.0) };
    }
}

/// Reads a file, or a range of one
///
/// ## Behaviour
/// Opens, reads to the end or to the length asked for, and
/// closes, all inside one run. Reading is done in
/// `FILE_CHUNK` pieces with a cancellation check between them
///
/// ## Returns
/// The bytes read. A range that starts past the end of the file
/// comes back empty rather than as an error, the same as the
/// syscall underneath it
#[derive(Debug, Clone)]
pub struct ReadTask {
    /// The file to read, already in the form the kernel takes
    ///
    /// `None` when the path had a zero byte in it and could not
    /// be converted, which is reported when the task runs
    path: Option<CString>,

    /// How much of it to read
    extent: Extent,
}

/// Writes bytes to a file
///
/// ## Behaviour
/// Creates the file if it isn't there, writes in `FILE_CHUNK`
/// pieces with a cancellation check between them, and closes.
/// A short write is looped rather than reported, so the count
/// that comes back is the whole buffer or an error
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
    /// #### Note
    /// `Arc<[u8]>` rather than `Vec<u8>` because `.at_rate()`
    /// clones the whole task once per run. A `Vec` would copy
    /// the buffer every period to write the same bytes again
    data: Arc<[u8]>,

    /// Where they go
    mode: WriteMode,
}

/// Asks what a path is
///
/// ## Returns
/// A [`Metadata`], which is the fields of a `stat` worth
/// keeping rather than the `stat` itself
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
/// ## Behaviour
/// Not cancellable. There is no loop to check in — the call is
/// made and it either works or it doesn't
///
/// #### Note
/// Spawning one of these costs more than the call it makes.
/// They earn a place anyway, because on a network mount or a
/// cold directory the call is not cheap at all, and because a
/// caller pipelining file work wants all of it going the same
/// way. `Runtime::block` is usually the better call
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

        // A directory opens for reading and then refuses every
        // read with a code that says nothing about why. Asking
        // first turns that into an answer
        if directory(&fd)? {
            return Err(RuntimeError::CheckError(Some(libc::EISDIR)));
        }

        match self.extent {
            Extent::Whole => read_whole(&fd),
            Extent::Range { offset, len } => read_range(&fd, offset, len),
        }
    }

    /// Held for its whole duration, so it goes to a thread that
    /// exists to be held
    fn blocking(&self) -> bool {
        true
    }
}

impl Task for WriteTask {
    type Output = Result<usize, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

        // `O_APPEND` moves every write to the end, which is the
        // one thing a positional write must not do, so the two
        // are never set together
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

        let asked = match self.follow {
            true => unsafe { libc::stat(path.as_ptr(), &mut raw) },
            false => unsafe { libc::lstat(path.as_ptr(), &mut raw) },
        };

        asked.check()?;

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

        // `opendir` reports failure with a null rather than a
        // negative, so there is no status code for `check` to
        // look at and the errno has to be read directly
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

            let entry = unsafe { libc::readdir(dir.0) };

            if entry.is_null() {
                break;
            }

            // `d_namlen` rather than looking for the zero, so a
            // name that fills the array is still measured right
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

        let done = match self.op {
            PathOp::Remove => unsafe { libc::unlink(path.as_ptr()) },
            PathOp::RemoveDir => unsafe { libc::rmdir(path.as_ptr()) },
            PathOp::CreateDir => unsafe { libc::mkdir(path.as_ptr(), 0o777) },
            PathOp::Rename => {
                let to = self.other.as_ref().ok_or(RuntimeError::BadPath)?;

                unsafe { libc::rename(path.as_ptr(), to.as_ptr()) }
            }
        };

        done.check()?;

        Ok(())
    }

    fn blocking(&self) -> bool {
        true
    }
}

/// Turns a path into the form the kernel takes
///
/// ## Returns
/// `None` when the path has a zero byte in it. The kernel reads
/// a path as bytes up to the first zero, so a path containing
/// one has no faithful form to be passed in — and passing the
/// part before it would act on a different file
///
/// #### Note
/// Done once, here, rather than on every run. A repeat puts the
/// same task back in the same slot, and converting the same
/// path again every time it comes round is work with a known
/// answer
fn as_c_path(path: impl AsRef<Path>) -> Option<CString> {
    CString::new(path.as_ref().as_os_str().as_bytes()).ok()
}

/// Opens a path, always closing on exec
///
/// `mode` is only looked at when the flags create the file, but
/// it is passed either way — `open` is variadic, and leaving an
/// argument off a variadic call is worse than passing one that
/// is ignored
fn open_at(path: &CString, flags: libc::c_int, mode: libc::c_int) -> Result<Fd, RuntimeError> {
    loop {
        // `O_CLOEXEC` on every open, or the descriptor is
        // inherited by anything the user forks
        let raw = unsafe { libc::open(path.as_ptr(), flags | libc::O_CLOEXEC, mode) }.check();

        match raw {
            Ok(fd) => return Ok(Fd(fd)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        }
    }
}

/// Whether the open descriptor is a directory
fn directory(fd: &Fd) -> Result<bool, RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    unsafe { libc::fstat(fd.0, &mut raw) }.check()?;

    Ok(raw.st_mode & libc::S_IFMT == libc::S_IFDIR)
}

/// Reads from wherever the descriptor is to the end of the file
///
/// ## Behaviour
/// One `FILE_CHUNK` at a time, with a cancellation check
/// between chunks, growing the buffer as it goes
///
/// #### Note
/// The size a `stat` reports is used to reserve and for nothing
/// else. It is a hint that was true when it was read — files
/// grow and shrink underneath a reader, and some nodes report
/// nothing while having plenty. Only a read returning zero ends
/// this
fn read_whole(fd: &Fd) -> Result<Vec<u8>, RuntimeError> {
    let mut found = Vec::new();

    if let Ok(size) = hint(fd) {
        found.reserve(size);
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
            // The read that reports the end still had to have
            // somewhere to land, so even a file whose length
            // was guessed exactly right finishes holding a
            // chunk of slack. Given back rather than carried,
            // because this output lives as long as its handle
            // does and a reader asked for a file, not for room
            // for one and a half of them
            if found.capacity() - found.len() >= FILE_CHUNK {
                found.shrink_to_fit();
            }

            return Ok(found);
        }

        // Sound because the kernel just wrote `got` bytes into
        // the spare capacity that `reserve` guaranteed
        unsafe { found.set_len(found.len() + got) };
    }
}

/// Reads `len` bytes from `offset`, or fewer at the end
///
/// ## Behaviour
/// `pread`, so the descriptor's own offset is never touched and
/// two runs of the same series can read the same file at once
/// without moving each other along
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

        // The end of the file, which is how a range that runs
        // off the end comes back short rather than failing
        if got == 0 {
            break;
        }

        unsafe { found.set_len(found.len() + got) };
    }

    Ok(found)
}

/// Writes the whole buffer, however many calls that takes
///
/// ## Behaviour
/// A write is allowed to take less than it was offered, so the
/// count is looped until the buffer is spent. Treating one
/// short write as done is how a file ends up quietly truncated
///
/// `at` is `Some` for a positional write and `None` for one
/// that follows the descriptor, which is what makes append work
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

        // Nothing written and nothing said about why. It can't
        // happen for a regular file and a non empty buffer, and
        // carrying on would be a loop that never ends, so it is
        // read as the one thing that would explain it
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
/// `EINVAL`, which is what the syscall itself answers a
/// negative offset with. Anything past `i64::MAX` would arrive
/// as exactly that
fn seek_to(offset: u64) -> Result<libc::off_t, RuntimeError> {
    libc::off_t::try_from(offset).map_err(|_| RuntimeError::CheckError(Some(libc::EINVAL)))
}

/// How much to reserve before the first read
///
/// Wrong as often as it is right, and only ever used to size an
/// allocation, never to decide when to stop
fn hint(fd: &Fd) -> Result<usize, RuntimeError> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    unsafe { libc::fstat(fd.0, &mut raw) }.check()?;

    Ok(raw.st_size.max(0) as usize)
}


#[cfg(test)]
mod tests {
    use super::*;

    /// Every file task holds its thread for as long as its
    /// work takes, and has to say so
    ///
    /// Asserted here rather than by watching the pool. What the
    /// `Executor` does with the answer is already covered by
    /// the blocking sleeps in the suite, and the file specific
    /// half is only ever this one bool — which a read of a warm
    /// file finishes far too quickly for any sampling loop to
    /// catch it in the act
    ///
    /// A task that forgot would sit on a worker for the length
    /// of a syscall, which is exactly what the sleep threads
    /// exist to prevent
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

    /// A path the kernel can't be given doesn't convert
    ///
    /// The failure that matters isn't the refusal, it is what
    /// would happen without one: the kernel reads a path up to
    /// its first zero, so passing this through would quietly
    /// act on a different file
    #[test]
    fn a_path_with_a_zero_byte_does_not_convert() {
        assert!(as_c_path("a\0b").is_none(), "a zero byte must not convert");
        assert!(as_c_path("ab").is_some(), "an ordinary path must convert");
    }
}
