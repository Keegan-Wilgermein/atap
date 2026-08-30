//! Errors
//! Errors that the crate can return

use std::sync::{TryLockError};

pub enum RuntimeError {
    ReadError,
    WriteError,
    LockError,
}

impl<T> From<TryLockError<T>> for RuntimeError {
    fn from(_: TryLockError<T>) -> Self {
        Self::LockError
    }
}
