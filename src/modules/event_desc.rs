//! # Event Descriptor
//! Describes events for creating `kevent` calls

use crate::constants::EV_UDATA_SPECIFIC;

/// A `kevent` description
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventDesc {
    pub(crate) filter: i16,
    pub(crate) flags: u16,
    pub(crate) fflags: u32,
}

impl EventDesc {
    /// Creates a new custom `EventDesc`
    pub fn new(filter: i16, flags: u16, fflags: u32) -> Self {
        Self {
            filter,
            flags,
            fflags,
        }
    }

    /// Returns the `kevent` flags
    /// required to make a new timer
    pub(crate) fn new_timer() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
        }
    }

    /// Returns the `kevent` flags required to take a
    /// timer back off a queue
    pub(crate) fn new_timer_delete() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to make
    /// a timer that keeps firing
    pub(crate) fn new_interval() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ENABLE,
            fflags: libc::NOTE_NSECONDS,
        }
    }

    /// Returns the `kevent` flags required to
    /// raise a user triggered event
    ///
    /// Registers the event and fires it in one syscall
    pub(crate) fn new_user_trigger() -> Self {
        Self {
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_TRIGGER,
        }
    }

    /// Returns the `kevent` flags required to wait
    /// for a child process to exit, registered at its pid
    pub(crate) fn new_proc_exit() -> Self {
        Self {
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
        }
    }

    /// Returns the `kevent` flags required to take a
    /// process watch back off a queue
    ///
    /// Also removes an exit already queued, which a later task on
    /// the same thread could otherwise take for its own child
    pub(crate) fn new_proc_delete() -> Self {
        Self {
            filter: libc::EVFILT_PROC,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to watch a
    /// descriptor for something to read
    ///
    /// Level triggered, so the descriptor stays ready while it has
    /// anything on it and one blocking `read` per wake is safe
    pub(crate) fn new_read() -> Self {
        Self {
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to take a
    /// read watch back off a queue
    ///
    /// A descriptor at its end stays readable forever, so a watch
    /// left on one turns a wait into a spin
    pub(crate) fn new_read_delete() -> Self {
        Self {
            filter: libc::EVFILT_READ,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to watch a
    /// descriptor for room to write
    ///
    /// Level triggered, and the room may be a single byte, so the
    /// descriptor must be `O_NONBLOCK`
    pub(crate) fn new_write() -> Self {
        Self {
            filter: libc::EVFILT_WRITE,
            flags: libc::EV_ADD,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to take a write
    /// watch back off a queue
    ///
    /// Comes off before the descriptor is closed. Closing frees the
    /// number for reuse while the kernel is still taking the watch
    /// down
    pub(crate) fn new_write_delete() -> Self {
        Self {
            filter: libc::EVFILT_WRITE,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to wake a parked task
    /// once, when its socket is ready for `filter`
    ///
    /// Unique to the task, so another task parked on the same
    /// socket keeps its own watch
    pub(crate) fn new_park(filter: i16) -> Self {
        Self {
            filter,
            flags: libc::EV_ADD | libc::EV_ONESHOT | EV_UDATA_SPECIFIC,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to take one task's park
    /// back off, leaving any other task's alone
    pub(crate) fn new_park_delete(filter: i16) -> Self {
        Self {
            filter,
            flags: libc::EV_DELETE | EV_UDATA_SPECIFIC,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to wait once for a
    /// socket to be ready for `filter`, on a thread's own queue
    pub(crate) fn new_ready(filter: i16) -> Self {
        Self {
            filter,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to take a readiness
    /// wait back off a thread's own queue
    pub(crate) fn new_ready_delete(filter: i16) -> Self {
        Self {
            filter,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }
}
