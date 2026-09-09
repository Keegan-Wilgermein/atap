//! # Wake Target
//! How the `Reactor` reaches the thread waiting on an event
//!
//! A `kevent` carries one word of user data, and that word has
//! to say both how to wake the waiter and where it is. Tagging
//! the low bit covers both in the space available, with no
//! allocation and nothing for either side to free

use crate::modules::waiter::Waiter;
use libc::c_void;

/// What to do when an event comes back
pub(crate) enum WakeTarget {
    /// Nobody is waiting on this event
    None,

    /// Raise a trigger on this kqueue
    ///
    /// The cheap path. The waiting thread is sitting in a
    /// `kevent` call on a queue of its own, so waking it is
    /// one syscall and no handoff
    Queue(i32),

    /// Set this waiter's flag and unpark its thread
    ///
    /// The fallback, for a thread that couldn't get a queue
    Parked(*mut Waiter),
}

impl WakeTarget {
    /// Packs a target into the one word a `kevent` gives it
    ///
    /// #### Note
    /// The descriptor is stored one higher than it is, so that
    /// a real kqueue on descriptor 0 doesn't encode to the
    /// same word as having no waiter at all
    pub(crate) fn encode(self) -> *mut c_void {
        match self {
            Self::None => std::ptr::null_mut::<c_void>(),
            Self::Queue(queue) => (((queue as usize) + 1) << 1) as *mut c_void,
            Self::Parked(waiter) => ((waiter as usize) | 1) as *mut c_void,
        }
    }

    /// Reads a target back out of an event
    ///
    /// A `Waiter` is at least word aligned wherever it lives,
    /// so its low bit is free to mark it apart from a queue
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
