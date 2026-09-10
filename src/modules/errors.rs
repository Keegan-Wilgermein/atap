//! # Errors
//! Errors that the crate can return

use std::{error::Error, fmt, io};

/// A collection of all the errors
/// that can occur, that the user can see
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeError {
    /// CheckErrors occur
    /// when a `.check()`
    /// fails on a `libc`
    /// status code
    CheckError(Option<i32>),

    /// AddressLock errors occur
    /// when a call to `libc::os_sync_wait_on_address()`
    /// returns an error value
    AddressLock,

    /// Runtime has already been previously initialised
    AlreadyInit,

    /// The runtime has been shut down
    ///
    /// Shutting down is final for the life of the process, so
    /// this is the answer to initialising again rather than
    /// something that can be waited out
    ShutDown,

    /// The output was already moved out
    /// by a call to `take()`
    ///
    /// Another run may still publish one. On a repeat this is
    /// a race lost rather than an ending — the series is
    /// between runs and reading again finds the next output.
    /// `Finished` is the one that means no more are coming
    AlreadyTaken,

    /// A bounded series ran out, and its output has been taken
    ///
    /// The ending `AlreadyTaken` can't express. A series given
    /// a `count`, a `for_duration` or an `until` reached it,
    /// which is how a bounded series *succeeds* — so this is
    /// the end of a loop rather than a failure, and reading
    /// again will never find anything
    ///
    /// #### Note
    /// Only ever a repeat. A task that was only going to run
    /// once says `AlreadyTaken`, because "somebody beat you to
    /// it" is the useful thing to know there and there was
    /// never another run for this to rule out
    Finished,

    /// The task was cancelled by
    /// one of its listeners
    Cancelled,

    /// There is no task behind this handle
    ///
    /// The slot the id points at is empty, so there is nothing
    /// to read, wait on or cancel
    ///
    /// #### Note
    /// Nothing to do with the health of the runtime. It means
    /// the handle never had a task in the first place — the
    /// table had no slot to give when it was spawned — or that
    /// the task it did have is long finished and every listener
    /// on it has gone
    NoSuchTask,

    /// Nothing is going to produce an output for this task
    ///
    /// Covers every way that can happen: the task panicked, the
    /// thread running it died holding it, there was nothing
    /// left to run it, or a repeat couldn't be put back on the
    /// clock
    ///
    /// A task that panicked or whose thread died was already
    /// taken out of its slot by the run that came apart, so
    /// there is nothing left to run again. Every other task
    /// that thread was holding is handed to another worker and
    /// comes back normally
    TaskFailed,

    /// The task hasn't settled yet
    ///
    /// Not a failure, just an answer that
    /// isn't there yet. The handle is still
    /// good and the task is still coming
    ///
    /// Also what a timed read gives back when its
    /// timeout ran out first
    NotReady,

    /// The table is too close to the number
    /// of tasks alive in it to give any of
    /// it back
    StillInUse,

    /// A path could not be handed to the kernel
    ///
    /// The kernel takes a path as bytes ending at the first
    /// zero, so a path with a zero of its own inside it has no
    /// faithful form to be passed in — the call would silently
    /// act on the part before it, which is a different file
    ///
    /// #### Note
    /// Not a `CheckError`. No syscall was made and no errno was
    /// set, and dressing this up as one would put a number in
    /// the message that the kernel never said
    BadPath,

    /// An argument could not be handed to the kernel
    ///
    /// The kernel takes an argument as bytes ending at the
    /// first zero, the same way it takes a path, so an entry
    /// with a zero of its own inside it has no faithful form to
    /// be passed in — the program would run with the part
    /// before it, which is a different argument
    ///
    /// #### Note
    /// Its own variant rather than a `BadPath`. The two fail
    /// for the same reason, but an argument is not a path, and
    /// a message saying it was would send a reader looking at
    /// the wrong half of the call
    BadArgument,
}

impl fmt::Display for RuntimeError {
    /// One line, saying what went wrong rather than what to do
    /// about it
    ///
    /// #### Note
    /// `CheckError` renders whatever the kernel said, since the
    /// errno is the only part of it that carries any
    /// information and a bare "a syscall failed" wastes the one
    /// useful thing it is holding
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CheckError(Some(errno)) => {
                write!(formatter, "system call failed: {}", io::Error::from_raw_os_error(*errno))
            }
            Self::CheckError(None) => write!(formatter, "system call failed"),
            Self::AddressLock => write!(formatter, "the kernel refused a wait on an address"),
            Self::AlreadyInit => write!(formatter, "the runtime is already initialised"),
            Self::ShutDown => write!(formatter, "the runtime has been shut down"),
            Self::AlreadyTaken => write!(formatter, "the output was already taken"),
            Self::Finished => write!(formatter, "the series ran out and its output was taken"),
            Self::Cancelled => write!(formatter, "the task was cancelled"),
            Self::NoSuchTask => write!(formatter, "there is no task behind this handle"),
            Self::TaskFailed => write!(formatter, "the task will never produce an output"),
            Self::NotReady => write!(formatter, "the task has not settled yet"),
            Self::StillInUse => write!(formatter, "too much of the task table is in use to trim"),
            Self::BadPath => write!(formatter, "the path contains a zero byte"),
            Self::BadArgument => write!(formatter, "an argument contains a zero byte"),
        }
    }
}

impl Error for RuntimeError {}
