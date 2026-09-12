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
    ///
    /// For a Unix socket, also a path that is empty or longer than
    /// the 103 bytes the kernel has room for
    BadPath,

    /// An argument couldn't be handed to the kernel as written
    ///
    /// A program argument with a zero byte in it, or a process id
    /// that names a group rather than one process
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

    /// A socket task's timeout ran out before it finished
    ///
    /// A receive that times out puts back what it had read, so the
    /// connection can still be read from
    TimedOut,

    /// An address didn't parse, or a name lookup found nothing for
    /// it
    BadAddress,

    /// The other side closed the connection before a receive had
    /// everything it was waiting for
    Closed,

    /// A `recv_until` read as much as it was allowed without
    /// finding its delimiter
    TooLong,

    /// A certificate was refused
    ///
    /// The other side's didn't check out: an untrusted issuer, the
    /// wrong name, or out of date. Or this side's own certificate
    /// or key file didn't parse, or the two didn't match
    ///
    /// Only TLS tasks return this, which need the `tls` feature
    BadCertificate,

    /// TLS failed for a reason other than a certificate
    ///
    /// The handshake broke down, a record didn't decrypt, or the
    /// other side sent an alert
    ///
    /// Only TLS tasks return this, which need the `tls` feature
    TlsFailed,

    /// The signal isn't one the kernel will take
    ///
    /// A number that isn't a signal at all, or `SIGKILL` or
    /// `SIGSTOP` where one could be caught: those two can be sent,
    /// but never taken over
    BadSignal,
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
            Self::AlreadyTaken => write!(formatter, "the output was already taken"),
            Self::Finished => write!(formatter, "the series ran out and its output was taken"),
            Self::Cancelled => write!(formatter, "the task was cancelled"),
            Self::NoSuchTask => write!(formatter, "there is no task behind this handle"),
            Self::TaskFailed => write!(formatter, "the task will never produce an output"),
            Self::NotReady => write!(formatter, "the task has not settled yet"),
            Self::StillInUse => write!(formatter, "too much of the task table is in use to trim"),
            Self::BadPath => write!(formatter, "the path can't be handed to the kernel"),
            Self::BadArgument => write!(formatter, "an argument contains a zero byte"),
            Self::BadVariable => {
                write!(formatter, "an environment variable cannot be passed on as written")
            }
            Self::BadDirectory => {
                write!(formatter, "the working directory is not an absolute path without zero bytes")
            }
            Self::TimedOut => write!(formatter, "the task ran out of time"),
            Self::BadAddress => write!(formatter, "the address could not be parsed or found"),
            Self::Closed => write!(formatter, "the other side closed the connection"),
            Self::TooLong => write!(formatter, "the delimiter was not found within the limit"),
            Self::BadCertificate => write!(formatter, "a certificate was refused"),
            Self::TlsFailed => write!(formatter, "the TLS session failed"),
            Self::BadSignal => write!(formatter, "the signal cannot be used that way"),
        }
    }
}

impl Error for RuntimeError {}
