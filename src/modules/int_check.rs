//! # IntCheck
//! The entire purpose of this trait is to
//! check functions that return integer status codes

use std::{fmt::Display, io::Error};
use crate::RuntimeError;

pub(crate) trait IntCheck
where
    Self: Sized + PartialOrd<i32> + Display,
{
    /// Check an status code integer to be equal to 0,
    /// panicking if it's not
    fn check(self) -> Result<Self, RuntimeError> {
        if self < 0 {
            let error = Error::last_os_error().raw_os_error();

            Err(RuntimeError::CheckError(error))
        } else {
            Ok(self)
        }
    }
}

impl IntCheck for i32 {}
