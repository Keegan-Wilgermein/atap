//! Event type
//! Description of an event that this runtime supports

use libc::c_void;

/// All the supported events the runtime supports
pub enum EventType {
    /// Sleep for x time events
    Sleep,

    /// Unknown event
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
    #[inline(always)]
    pub(crate) fn create(&self, data: libc::intptr_t, udata: *mut c_void) -> *const libc::kevent {
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
fn sleep_event(data: libc::intptr_t, udata: *mut c_void) -> *const libc::kevent {
    Box::leak(Box::new(
        libc::kevent {
            ident: 1,                                               // Timer id, needs to be unique to prevent overwrites
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_NSECONDS,
            data,                                                   // The sleep duration in ns
            udata,                                                  // Thread handle as *mut c_void
        }
    ))
}
