//! # Event Descriptor
//! Describes events for creating `kevent` calls

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
    ///
    /// Used to cancel a sleep that hasn't fired yet. Without
    /// it the timer stays armed and goes off into a queue
    /// nobody is waiting on it in any more
    pub(crate) fn new_timer_delete() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to make
    /// a timer that keeps firing
    ///
    /// Registered once and left alone, unlike `new_timer`,
    /// which is armed for a single shot and dropped by the
    /// kernel once it has been delivered
    ///
    /// #### Note
    /// `NOTE_CRITICAL` is deliberately absent. This drives
    /// the manager's policy pass, which does not care about
    /// microseconds and has no business asking the kernel to
    /// treat it as though it does
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
    /// `EVFILT_USER` is the filter that exists purely so
    /// userspace can wake a kqueue on demand, rather than
    /// waiting on a timer or a descriptor
    ///
    /// #### Note
    /// Carrying `NOTE_TRIGGER` on the `EV_ADD` registers the
    /// event and fires it in the same syscall, so waking the
    /// `Executor` costs one call rather than two
    pub(crate) fn new_user_trigger() -> Self {
        Self {
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_TRIGGER,
        }
    }
}
