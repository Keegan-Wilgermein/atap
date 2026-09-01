//! Errors
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

    /// Runtime has already been previously initialised
    AlreadyInit,
}
