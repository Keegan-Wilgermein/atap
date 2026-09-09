//! # Errors
//! Errors that the crate can return

/// A collection of all the errors
/// that can occur, that the user can see
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
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

    /// The output was already moved out
    /// by a call to `take()`
    AlreadyTaken,

    /// The task was cancelled by
    /// one of its listeners
    Cancelled,

    /// The `Executor` gave up before it
    /// could finish the task, so no result
    /// is ever going to arrive
    ExecutorDead,
}
