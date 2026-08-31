//! Event type
//! Description of an event that this runtime supports

use libc::c_void;

/// All the supported events the runtime supports
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum EventType {
    /// Sleep for x time events
    Sleep,

    /// Unknown event
    /// 
    /// These will panic if
    /// anything tries to create an
    /// event with them
    Unknown,
}

impl From<i16> for EventType {
    fn from(value: i16) -> Self {
        match value {
            libc::EVFILT_TIMER => Self::Sleep,
            _ => Self::Unknown,
        }
    }
}

impl EventType {
    /// Creates a new `libc::kevent` struct
    ///
    /// Returned by value so the caller can hold it
    /// on the stack for the duration of the syscall
    #[inline(always)]
    pub(crate) fn create(&self, data: libc::intptr_t, udata: *mut c_void) -> libc::kevent {
        match self {
            Self::Sleep => sleep_event(data, udata),
            Self::Unknown => unreachable!("A task that creates an unknown filter can't exist"),
        }
    }
}

/// Creates a `kevent` for sleep tasks
/// 
/// `data` is the sleep duration
#[inline(always)]
fn sleep_event(data: libc::intptr_t, udata: *mut c_void) -> libc::kevent {
    libc::kevent {
        ident: 1,                                               // Timer id, is unique across threads so it's fine
        filter: libc::EVFILT_TIMER,
        flags: libc::EV_ADD | libc::EV_ONESHOT,
        fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
        data,                                                   // The sleep duration in ns
        udata,                                                  // Thread handle as *mut c_void
    }
}
