//! # IntCheck
//! The entire purpose of this trait is to make sure
//! I don't forget to check functions that return integer status codes

pub(crate) trait IntCheck
where
    Self: Sized + PartialEq<i32>,
{
    /// Check an status code integer to be equal to 0,
    /// panicking if it's not
    fn check(self) -> Self {
        if self != 0 {
            panic!("Operation failed");
        } else {
            self
        }
    }

    /// Same implementation as `check()`
    /// but with a custom panic message
    fn check_message(self, message: &str) -> Self {
        if self != 0 {
            panic!("{}", message);
        } else {
            self
        }
    }
}

impl IntCheck for i32 {}
