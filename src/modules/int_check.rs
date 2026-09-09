//! # IntCheck
//! The entire purpose of this trait is to
//! check functions that return integer status codes

use crate::RuntimeError;
use std::{fmt::Display, io::Error};

pub(crate) trait IntCheck
where
    Self: Sized + PartialOrd + Display,
{
    /// The value everything below is a failure
    ///
    /// Carried by the trait rather than compared against a
    /// literal, because the widths don't mix. `read` and its
    /// family return `ssize_t`, and `isize` has no
    /// `PartialOrd<i32>` to compare against a bare `0` with.
    /// Casting to make one fit would truncate a large read
    /// into a negative, which is to say into an error
    const ZERO: Self;

    /// Check an status code integer to be equal to 0,
    /// panicking if it's not
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
