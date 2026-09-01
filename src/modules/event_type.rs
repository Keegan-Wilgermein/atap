//! # Event type
//! Description of an event that this runtime supports

use libc::c_void;

use crate::modules::event_desc::EventDesc;

/// All the events the runtime supports
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
    pub(crate) fn create(
        &self,
        id: usize,
        data: libc::intptr_t,
        udata: *mut c_void,
    ) -> libc::kevent {
        match self {
            Self::Sleep => sleep_event(id, data, udata, EventDesc::new_timer()),
            Self::Unknown => unreachable!("A task that creates an unknown filter can't exist"),
        }
    }
}

/// Creates a `kevent` for sleep tasks
///
/// `data` is the sleep duration
#[inline(always)]
fn sleep_event(
    id: usize,
    data: libc::intptr_t,
    udata: *mut c_void,
    desc: EventDesc,
) -> libc::kevent {
    libc::kevent {
        ident: id,                  // Timer id, is unique across threads so it's fine
        filter: desc.filter,
        flags: desc.flags,
        fflags: desc.fflags,
        data,                       // The sleep duration in ns
        udata,                      // Thread handle as *mut c_void
    }
}
