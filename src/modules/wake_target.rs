//! # Wake Target
//! How the `Reactor` reaches the thread waiting on an event,
//! packed into a `kevent`'s one word of user data

use crate::modules::waiter::Waiter;
use libc::c_void;

/// What to do when an event comes back
pub(crate) enum WakeTarget {
    /// Nobody is waiting on this event
    None,

    /// Raise a trigger on this kqueue
    Queue(i32),

    /// Set this waiter's flag and unpark its thread
    Parked(*mut Waiter),
}

impl WakeTarget {
    /// Packs a target into a `kevent`'s user data
    ///
    /// A queue is stored one higher, so descriptor 0 doesn't read
    /// back as `None`
    pub(crate) fn encode(self) -> *mut c_void {
        match self {
            Self::None => std::ptr::null_mut::<c_void>(),
            Self::Queue(queue) => (((queue as usize) + 1) << 1) as *mut c_void,
            Self::Parked(waiter) => ((waiter as usize) | 1) as *mut c_void,
        }
    }

    /// Reads a target back out of a `kevent`'s user data
    ///
    /// The low bit marks a `Waiter`, which is always at least word
    /// aligned
    pub(crate) fn decode(udata: *mut c_void) -> Self {
        let raw = udata as usize;

        if raw == 0 {
            return Self::None;
        }

        if raw & 1 == 1 {
            return Self::Parked((raw & !1) as *mut Waiter);
        }

        Self::Queue(((raw >> 1) - 1) as i32)
    }
}
