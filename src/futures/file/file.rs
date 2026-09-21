//! # File
//! The constructors every file task is started from

use crate::futures::file::{
    file_task::{CopyTask, MetadataTask, PathBufTask, PathTask, ReadDirTask, ReadTask, WriteTask},
    open_file::OpenTask,
    watch_task::WatchTask,
};
use std::{path::Path, sync::Arc};

/// The base struct
///
/// It doesn't implement `Task` so it can't be passed into a
/// runtime function directly without calling a method on it
/// that returns something that does
///
/// ## Behaviour
/// Every task here but [`File::watch`] holds a thread for as
/// long as its work takes, so a spawned one runs on a sleep
/// thread rather than a worker. Past `cores * 8` of them, file
/// tasks queue and wait their turn
///
/// A watch is the exception. It is nearly all waiting, so it
/// parks the way a socket task does and holds no thread at all
///
/// ## Cancelling
/// A read, a write or a copy is cancellable before it opens the
/// file and between chunks, not during one. The tree walks,
/// [`File::create_dir_all`] and [`File::remove_dir_all`], check
/// between entries. Everything else here is a single syscall and
/// can't be cancelled once it has started
///
/// **A cancelled write does not undo itself.** `write`
/// truncates the file when it opens it, and a cancel part way
/// through leaves whatever chunks had landed. A write cancelled
/// before it opens the file leaves it untouched
pub struct File;

impl File {
    /// Reads a whole file
    ///
    /// ## Behaviour
    /// Reads in 64 KiB pieces until the file ends, checking
    /// between them whether the task has been cancelled
    ///
    /// ## Returns
    /// Every byte in the file. An empty file gives an empty
    /// `Vec` rather than an error
    ///
    /// #### Note
    /// The whole file lands in memory at once. [`File::read_at`]
    /// reads a large one in pieces
    pub fn read<P>(path: P) -> ReadTask
    where
        P: AsRef<Path>,
    {
        ReadTask::whole(path)
    }

    /// Reads part of a file
    ///
    /// ## Behaviour
    /// Reads `len` bytes from `offset` using a positional read,
    /// so two reads of one file can't disturb each other
    ///
    /// ## Returns
    /// Up to `len` bytes. Fewer means the file ended first,
    /// which is an answer rather than a failure, and an offset
    /// past the end gives an empty `Vec`
    ///
    /// #### Note
    /// This is how a file too large to hold is read. Call it in
    /// a loop, advancing the offset by what came back and
    /// stopping when a read comes back empty
    pub fn read_at<P>(path: P, offset: u64, len: usize) -> ReadTask
    where
        P: AsRef<Path>,
    {
        ReadTask::range(path, offset, len)
    }

    /// Writes bytes to a file, replacing what was there
    ///
    /// ## Behaviour
    /// Creates the file if it doesn't exist and truncates it if
    /// it does. Writes in 64 KiB pieces, checking between them
    /// whether the task has been cancelled
    ///
    /// ## Returns
    /// The number of bytes written, which is the length of the
    /// input whenever this isn't an error
    ///
    /// #### Note
    /// Runs of an `.at_rate()` series overlap, and two truncating
    /// writes to one path at once race each other.
    /// `.repeat().every(gap)` runs them one at a time
    pub fn write<P>(path: P, data: impl Into<Arc<[u8]>>) -> WriteTask
    where
        P: AsRef<Path>,
    {
        WriteTask::truncate(path, data)
    }

    /// Adds bytes to the end of a file
    ///
    /// ## Behaviour
    /// Creates the file if it doesn't exist and leaves whatever
    /// is in it alone. The kernel puts every write at the end
    /// as it happens, so this is the one write that is safe for
    /// two runs to do at once
    ///
    /// ## Returns
    /// The number of bytes written
    pub fn append<P>(path: P, data: impl Into<Arc<[u8]>>) -> WriteTask
    where
        P: AsRef<Path>,
    {
        WriteTask::append(path, data)
    }

    /// Writes bytes at a byte offset
    ///
    /// ## Behaviour
    /// Creates the file if it doesn't exist and leaves every
    /// byte outside the written range alone, including the ones
    /// before `offset`. Writing past the end leaves a hole
    ///
    /// ## Returns
    /// The number of bytes written
    pub fn write_at<P>(path: P, offset: u64, data: impl Into<Arc<[u8]>>) -> WriteTask
    where
        P: AsRef<Path>,
    {
        WriteTask::at(path, offset, data)
    }

    /// Asks what a path is
    ///
    /// ## Behaviour
    /// Follows a symbolic link and reports whatever is on the
    /// far end of it. [`File::symlink_metadata`] is the one
    /// that stops at the link
    ///
    /// ## Returns
    /// The fields of a `stat` worth keeping: length, mode,
    /// kind, and the three timestamps
    pub fn metadata<P>(path: P) -> MetadataTask
    where
        P: AsRef<Path>,
    {
        MetadataTask::following(path)
    }

    /// Asks what a path is, without following a link
    ///
    /// ## Returns
    /// The same as [`File::metadata`], except that a symbolic
    /// link reports itself rather than its target
    pub fn symlink_metadata<P>(path: P) -> MetadataTask
    where
        P: AsRef<Path>,
    {
        MetadataTask::link(path)
    }

    /// Lists what is in a directory
    ///
    /// ## Returns
    /// One [`DirEntry`] per thing in it, with its path joined onto
    /// the directory that was asked for and what kind of thing it
    /// is, in whatever order the filesystem gives them. `.` and
    /// `..` are left out
    ///
    /// [`DirEntry`]: crate::fs::DirEntry
    pub fn read_dir<P>(path: P) -> ReadDirTask
    where
        P: AsRef<Path>,
    {
        ReadDirTask::new(path)
    }

    /// Removes a file
    ///
    /// #### Note
    /// One syscall, so spawning it costs more than doing it.
    /// `Runtime::block(File::remove(p))` is usually the better
    /// call
    pub fn remove<P>(path: P) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::remove(path)
    }

    /// Removes a directory
    ///
    /// ## Behaviour
    /// The directory has to be empty
    pub fn remove_dir<P>(path: P) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::remove_dir(path)
    }

    /// Creates a directory
    ///
    /// ## Behaviour
    /// The parent has to exist already. Creating an existing
    /// directory is an error rather than nothing
    pub fn create_dir<P>(path: P) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::create_dir(path)
    }

    /// Waits for a path to change
    ///
    /// ## Behaviour
    /// Settles the moment anything happens to the path, saying
    /// what. A `.repeat()` of one is a watcher: it reports every
    /// change, including any that land between runs
    ///
    /// ```no_run
    /// # use atap::{Runtime, fs::File};
    /// # fn main() -> Result<(), atap::RuntimeError> {
    /// # let path = "/tmp/watched";
    /// // the next change, once
    /// let change = Runtime::block(File::watch(&path))?;
    ///
    /// // every change, for as long as the program runs
    /// let changes = Runtime::task(File::watch(&path)).repeat().spawn();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A directory works as readily as a file, and reports a
    /// write when an entry comes or goes. [`WatchTask::only`]
    /// narrows what counts
    ///
    /// ## Returns
    /// What changed. Several parts of a [`Change`] can be true at
    /// once, since one write can be both a write and a growth
    ///
    /// A path that isn't there is an error rather than a wait,
    /// unless the watch is told to [`WatchTask::appear`]
    ///
    /// The watch counts from when the task first runs, the same
    /// as a signal does: a change that happened beforehand is not
    /// waited for
    ///
    /// #### Note
    /// The watch follows the **file**, not the name. It opens the
    /// path once and holds that descriptor, so a file moved out
    /// from under its name is still watched, and reported as
    /// [`Change::renamed`]. A removal is the last thing a watch
    /// can report
    ///
    /// A spawned watch holds no thread, unlike every other task
    /// here, so any number of paths can be watched at once
    ///
    /// [`WatchTask::only`]: crate::fs::WatchTask::only
    /// [`WatchTask::appear`]: crate::fs::WatchTask::appear
    /// [`Change`]: crate::fs::Change
    /// [`Change::renamed`]: crate::fs::Change::renamed
    pub fn watch<P>(path: P) -> WatchTask
    where
        P: AsRef<Path>,
    {
        WatchTask::new(path)
    }

    /// Moves a path to another one
    ///
    /// ## Behaviour
    /// Replaces `to` if something is already there. Both paths
    /// have to be on the same filesystem
    pub fn rename<P, Q>(from: P, to: Q) -> PathTask
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        PathTask::rename(from, to)
    }

    /// Copies a file to another path
    ///
    /// ## Behaviour
    /// Clones the file where the volume allows, which takes no time
    /// and no space until either copy changes. Otherwise copies the
    /// bytes, and the permissions and extended attributes with them.
    /// Replaces `to` if something is already there
    ///
    /// ## Returns
    /// The size of the copy in bytes
    ///
    /// #### Note
    /// A directory can't be copied, and gives `EISDIR`. A link is
    /// followed, so its target is what gets copied
    pub fn copy<P, Q>(from: P, to: Q) -> CopyTask
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        CopyTask::new(from, to)
    }

    /// Makes a symbolic link at `link` that points at `target`
    ///
    /// ## Behaviour
    /// `target` is stored as written, and doesn't have to exist.
    /// A relative one is read from the link's own directory
    pub fn symlink<P, Q>(target: P, link: Q) -> PathTask
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        PathTask::symlink(target, link)
    }

    /// Gives an existing file a second name
    ///
    /// ## Behaviour
    /// Both names are the same file afterwards. `new` must not
    /// exist yet, and both have to be on the same filesystem
    pub fn hard_link<P, Q>(existing: P, new: Q) -> PathTask
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        PathTask::hard_link(existing, new)
    }

    /// Reads where a symbolic link points
    ///
    /// ## Returns
    /// The target as it was stored, which may be relative. A path
    /// that isn't a link gives `EINVAL`
    pub fn read_link<P>(path: P) -> PathBufTask
    where
        P: AsRef<Path>,
    {
        PathBufTask::read_link(path)
    }

    /// Resolves a path to the one it really names
    ///
    /// ## Returns
    /// An absolute path with every link, `.` and `..` resolved.
    /// The path has to exist
    pub fn canonicalize<P>(path: P) -> PathBufTask
    where
        P: AsRef<Path>,
    {
        PathBufTask::canonical(path)
    }

    /// Sets a path's permission bits
    ///
    /// ## Behaviour
    /// `mode` is the usual octal form, like `0o644`. Anything
    /// above `0o7777` gives [`RuntimeError::BadArgument`]. A link is
    /// followed
    ///
    /// [`RuntimeError::BadArgument`]: crate::RuntimeError::BadArgument
    pub fn set_permissions<P>(path: P, mode: u32) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::set_permissions(path, mode)
    }

    /// Makes a file exactly `len` bytes long
    ///
    /// ## Behaviour
    /// Cuts off whatever is past `len`, or adds zeros up to it
    pub fn set_len<P>(path: P, len: u64) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::set_len(path, len)
    }

    /// Creates a directory and every missing one above it
    ///
    /// ## Behaviour
    /// A directory already there is fine. Something else in the
    /// way gives `ENOTDIR`
    pub fn create_dir_all<P>(path: P) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::create_dir_all(path)
    }

    /// Removes a directory and everything in it
    ///
    /// ## Behaviour
    /// A symbolic link inside is removed, never followed, so
    /// nothing outside the directory is touched. A path that is
    /// itself a file or a link is removed like [`File::remove`]
    ///
    /// #### Note
    /// A cancel part way leaves whatever wasn't removed yet
    pub fn remove_dir_all<P>(path: P) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::remove_dir_all(path)
    }

    /// Opens a file and keeps it open
    ///
    /// ## Behaviour
    /// Read only unless told otherwise:
    ///
    /// ```no_run
    /// # use atap::fs::File;
    /// let task = File::open("data.bin").read(true).write(true).create(true);
    /// ```
    ///
    /// ## Returns
    /// An [`OpenFile`], whose methods work on the one descriptor.
    /// A mix of settings that means nothing, like `create` without
    /// `write`, gives [`RuntimeError::BadArgument`]
    ///
    /// [`OpenFile`]: crate::fs::OpenFile
    /// [`RuntimeError::BadArgument`]: crate::RuntimeError::BadArgument
    pub fn open<P>(path: P) -> OpenTask
    where
        P: AsRef<Path>,
    {
        OpenTask::new(path)
    }
}
