//! # Interest
//! Just an enum for differentiating between read and write requests

use libc::{EVFILT_READ, EVFILT_WRITE};

/// `kevent` registration interest
/// 
/// - `Self::Read`
/// - `Self::Write`
pub(crate) enum Interest {
    Read,
    Write,
}

impl From<i16> for Interest {
    fn from(value: i16) -> Self {
        match value {
            EVFILT_READ => Self::Read,
            EVFILT_WRITE => Self::Write,
            _ => unreachable!("There are no other kevent filters other than read and write"),
        }
    }
}

impl Into<i16> for Interest {
    fn into(self) -> i16 {
        match self {
            Self::Read => EVFILT_READ,
            Self::Write => EVFILT_WRITE,
        }
    }
}
