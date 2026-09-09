//! # File
//! The constructors every file task is started from
//!
//! `File` is a name to hang them on and nothing else, the same
//! way `Sleep` is. It holds no state and implements no traits,
//! so it can't be handed to a runtime function itself — calling
//! one of these is the only way past it

use crate::futures::file::file_task::{
    MetadataTask, PathTask, ReadDirTask, ReadTask, WriteTask,
};
use std::{path::Path, sync::Arc};

/// The base struct
///
/// It doesn't implement `Task` so it can't be passed into a
/// runtime function directly without calling a method on it
/// that returns something that does
///
/// ## Behaviour
/// Every task here holds a thread for as long as its work takes
/// and says so, so a spawned one is handed to a sleep thread
/// rather than run on a worker. The kernel has no readiness to
/// report for an ordinary file — a kqueue asked about one says
/// ready every time — so there is nothing to wait on and
/// nothing for the `Reactor` to do. The syscall is made, and it
/// comes back when it comes back
///
/// That pool holds `cores * 8` threads. Past that, file tasks
/// queue and wait their turn, which is backpressure rather than
/// an error — but it does mean a burst of reads is bounded by
/// the storage rather than by anything the runtime decides
///
/// ## Cancelling
/// A read or a write is cancellable between chunks, not during
/// one. Everything else here is a single syscall and can't be
/// cancelled once it has started
///
/// #### Note
/// Each task carries its own path and opens its own descriptor,
/// so nothing is shared between runs and two runs of the same
/// series never move each other's file offset along. The cost
/// is an open and a close per run
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
    /// The whole file lands in memory at once, so a very large
    /// one is a very large allocation. [`File::read_at`] is the
    /// way round that, and carries the pattern for reading a
    /// file in pieces
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
    /// so the descriptor's own offset is never moved and two
    /// reads of one file can't disturb each other
    ///
    /// ## Returns
    /// Up to `len` bytes. Fewer means the file ended first,
    /// which is an answer rather than a failure, and an offset
    /// past the end gives an empty `Vec`
    ///
    /// #### Note
    /// This is how a file too large to hold is read. Call it in
    /// a loop, advancing the offset by what came back and
    /// stopping when a read comes back empty — or spawn several
    /// at once for offsets ahead of where you are and join them
    /// in order, which reads ahead without ever holding more
    /// than those pieces
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
    /// A write is allowed to take fewer bytes than it was
    /// offered, so the count is looped until the buffer is
    /// spent. A count that came back short would be a file
    /// quietly truncated
    ///
    /// ## Returns
    /// The number of bytes written, which is the length of the
    /// input whenever this isn't an error
    ///
    /// #### Note
    /// `data` becomes an `Arc<[u8]>`, which is why `.at_rate()`
    /// on a write is cheap to clone — but two runs of a series
    /// overlap by design, and two truncating writes to one path
    /// at once race each other. `.repeat().every(gap)` is the
    /// sequential one
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
    /// The fields of a `stat` worth keeping — length, mode,
    /// kind, and the three timestamps
    ///
    /// #### Note
    /// A snapshot of the moment the task ran. Nothing holds the
    /// file still while it is read, so a length that was right
    /// then may not be right now
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
    /// One path per entry, each joined onto the directory that
    /// was asked for, in whatever order the filesystem gives
    /// them. `.` and `..` are left out
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
    /// call, and this exists for the times it isn't — a slow
    /// mount, or a caller that wants all of its file work going
    /// the same way
    pub fn remove<P>(path: P) -> PathTask
    where
        P: AsRef<Path>,
    {
        PathTask::remove(path)
    }

    /// Removes a directory
    ///
    /// ## Behaviour
    /// The directory has to be empty. Nothing here removes a
    /// tree, because a task that recurses is a task that can't
    /// say how long it will hold its thread for
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

    /// Moves a path to another one
    ///
    /// ## Behaviour
    /// Replaces `to` if something is already there, as the
    /// syscall underneath does. Both paths have to be on the
    /// same filesystem — a rename is not a copy, and the kernel
    /// refuses rather than falling back to one
    pub fn rename<P, Q>(from: P, to: Q) -> PathTask
    where
        P: AsRef<Path>,
        Q: AsRef<Path>,
    {
        PathTask::rename(from, to)
    }
}
