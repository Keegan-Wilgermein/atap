//! Errors
//! Errors that the crate can return

use std::sync::{TryLockError};

/// A collection of all the errors
/// that can occur, that the user can see
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuntimeError {
    /// Lock errors occur when
    /// a lock fails on some data
    LockError,
}

impl<T> From<TryLockError<T>> for RuntimeError {
    fn from(_: TryLockError<T>) -> Self {
        Self::LockError
    }
}
