//! # IntCheck
//! The entire purpose of this trait is to
//! check functions that return integer status codes

use std::fmt::Display;

#[allow(unused)]
pub(crate) trait IntCheck
where
    Self: Sized + PartialOrd<i32> + Display,
{
    /// Check an status code integer to be equal to 0,
    /// panicking if it's not
    fn check(self) -> Self {
        if self < 0 {
            panic!("Operation failed with status: {}", self);
        } else {
            self
        }
    }

    /// Same implementation as `check()`
    /// but with a custom panic message
    fn check_message(self, message: &str) -> Self {
        if self < 0 {
            panic!("{}", message);
        } else {
            self
        }
    }
}

impl IntCheck for i32 {}
