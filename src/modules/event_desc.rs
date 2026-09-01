//! Event Descriptor
//! Describes events for creating `kevent` calls

/// A `kevent` description
pub(crate) struct EventDesc {
    pub(crate) filter: i16,
    pub(crate) flags: u16,
    pub(crate) fflags: u32,
}

impl EventDesc {
    /// Creates a new custom `EventDesc`
    pub(crate) fn new(filter: i16, flags: u16, fflags: u32) -> Self {
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
}
