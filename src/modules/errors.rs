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

    /// The `Executor` crashed for some reason
    ///
    /// The `Executor` restarts itself
    /// and you can start a new task
    /// without it, it'll just sacrifice
    /// worker adaptation until it recovers
    ///
    /// Tasks aren't
    /// bound to the `Executor` so they
    /// will continue like normal
    ExecutorDead,

    /// The thread running this task died
    /// part way through it
    ///
    /// The task was already taken out of
    /// its slot by the thread that died, so
    /// there is nothing left to run again
    ///
    /// Every other task that thread was
    /// holding is handed to another worker
    /// and comes back normally. This is the
    /// only one that can't
    TaskFailed,

    /// The task hasn't settled yet
    ///
    /// Not a failure, just an answer that
    /// isn't there yet. The handle is still
    /// good and the task is still coming
    NotReady,

    /// The table is too close to the number
    /// of tasks alive in it to give any of
    /// it back
    StillInUse,
}
