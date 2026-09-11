//! # Errors
//! Errors that the crate can return

use std::{error::Error, fmt, io};

/// A collection of all the errors
/// that can occur, that the user can see
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RuntimeError {
    /// A `libc` call failed, carrying its `errno` if there was one
    CheckError(Option<i32>),

    /// The kernel refused a wait on an address
    AddressLock,

    /// Runtime has already been previously initialised
    AlreadyInit,

    /// The runtime has been shut down, and can't be initialised
    /// again
    ShutDown,

    /// The output was already moved out by `take()`
    ///
    /// On a repeat, the next run may still publish another.
    /// `Finished` is the one that means no more are coming
    AlreadyTaken,

    /// A bounded repeat reached its end, and its last output has
    /// been taken
    ///
    /// Nothing more is coming. Only repeats return this — a one
    /// shot says `AlreadyTaken`
    Finished,

    /// The task was cancelled by one of its listeners
    Cancelled,

    /// There is no task behind this handle
    ///
    /// The spawn found no slot to put it in, or the handle came
    /// from a `join_first` over nothing
    NoSuchTask,

    /// Nothing is going to produce an output for this task
    ///
    /// The task panicked, the thread running it died, nothing was
    /// left to run it, or a repeat couldn't be rescheduled
    TaskFailed,

    /// The task hasn't settled yet
    ///
    /// Also what a timed read returns when its timeout runs out
    NotReady,

    /// The table is too close to the number of tasks alive in it
    /// to give any of it back
    StillInUse,

    /// A path contained a zero byte, so it couldn't be handed to
    /// the kernel
    BadPath,

    /// A program argument contained a zero byte, so it couldn't be
    /// handed to the kernel
    BadArgument,

    /// An environment variable couldn't be handed to the kernel
    ///
    /// Its name was empty or contained `=`, or either half
    /// contained a zero byte. An `=` in the value is fine
    BadVariable,

    /// A working directory was relative or contained a zero byte
    ///
    /// One that doesn't exist is reported as `ENOENT` instead
    BadDirectory,
}

impl fmt::Display for RuntimeError {
    /// One line saying what went wrong, with the kernel's own
    /// message for a `CheckError`
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
            Self::BadVariable => {
                write!(formatter, "an environment variable cannot be passed on as written")
            }
            Self::BadDirectory => {
                write!(formatter, "the working directory is not an absolute path without zero bytes")
            }
        }
    }
}

impl Error for RuntimeError {}
