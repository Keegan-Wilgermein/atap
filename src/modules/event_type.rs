//! Event type
//! Description of an event that this runtime supports

use std::ptr;

/// All the supported events the runtime supports
pub enum EventType {
    Sleep,
}

impl EventType {
    pub(crate) fn create(&self, data: libc::intptr_t) -> *const libc::kevent {
        match self {
            Self::Sleep => sleep_event(data),
        }
    }
}

/// Creates a `kevent` for sleep tasks
fn sleep_event(data: libc::intptr_t) -> *const libc::kevent {
    Box::leak(
        Box::new(
            libc::kevent {
                ident: 1,                                   // Timer id, needs to be unique to prevent overwrites
                filter: libc::EVFILT_TIMER,
                flags: libc::EV_ADD | libc::EV_ONESHOT,
                fflags: libc::NOTE_NSECONDS,
                data,                                       // The sleep duration in ns
                udata: ptr::null_mut(),                     // Will be something later but for testing
            }
        )
    )
}
