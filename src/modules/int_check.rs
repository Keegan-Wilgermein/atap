//! # IntCheck
//! Checks the integer status codes syscalls return

use crate::RuntimeError;
use std::{fmt::Display, io::Error};

pub(crate) trait IntCheck
where
    Self: Sized + PartialOrd + Display,
{
    /// The value below which a status is a failure
    ///
    /// One per type, so an `isize` result is never compared
    /// through a truncating cast
    const ZERO: Self;

    /// Turns a negative status into an error carrying `errno`
    fn check(self) -> Result<Self, RuntimeError> {
        if self < Self::ZERO {
            let error = Error::last_os_error().raw_os_error();

            Err(RuntimeError::CheckError(error))
        } else {
            Ok(self)
        }
    }
}

impl IntCheck for i32 {
    const ZERO: Self = 0;
}

impl IntCheck for isize {
    const ZERO: Self = 0;
}
