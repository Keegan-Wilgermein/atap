//! # KEvent
//! Generates kevent syscalls from a tasks functionality

use std::ptr;

use crate::modules::event_type::EventType;

/// Generates kevent syscalls and passes back their ID
pub(crate) struct KEvent;

impl KEvent {
    /// Registers a new `kevent` with the kernel
    pub(crate) unsafe fn register(
        id: i32,
        event: EventType,
        data: libc::intptr_t,
    ) -> i32 {
        unsafe  {
            libc::kevent(
                id, // kqueue id
                event.create(data), // Events to register
                1, // Number of events to register
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        }
    }


    pub(crate) unsafe fn listen(id: i32) -> i32 {
        unsafe {
            libc::kevent(
                id,
                ptr::null(),
                0,
                eventlist().as_mut_ptr(),
                eventlist().len() as i32,
                ptr::null(),
            )
        }
    }
}

/// Creates the eventlist at compile time
const fn eventlist() -> [libc::kevent; 32] {
    [ libc::kevent {
        ident: 0,
        filter: 0,
        flags: 0,
        fflags: 0,
        data: 0,
        udata: ptr::null_mut(),
    }; 32 ]
}
