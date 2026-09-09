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
}
