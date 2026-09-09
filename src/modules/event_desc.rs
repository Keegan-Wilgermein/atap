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
