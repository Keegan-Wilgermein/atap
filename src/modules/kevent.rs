//! # KEvent
//! Generates kevent syscalls from a tasks functionality

use std::{mem, ptr};
use libc::c_void;

use crate::{constants::KEVENT_COUNT, modules::event_type::EventType};

/// Generates kevent syscalls and passes back their ID
pub(crate) struct KEvent;

impl KEvent {
    /// Registers a new `kevent` with the kernel
    #[inline(always)]
    pub(crate) unsafe fn register(
        id: i32,
        event: EventType,
        data: libc::intptr_t,
        udata: *mut c_void,
    ) -> i32 {
        let event_c = event.create(data, udata);
        unsafe  {
            libc::kevent(
                id,                             // kqueue id
                event_c,                // Events to register
                1,                        // Number of events to register
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        }
    }


    #[inline(always)]
    pub(crate) unsafe fn listen(
        id: i32,
        event_list: &mut [libc::kevent; KEVENT_COUNT],
    ) -> i32 {
        unsafe {
            libc::kevent(
                id,
                ptr::null(),
                0,
                event_list.as_mut_ptr(),
                event_list.len() as i32,
                ptr::null(),
            )
        }
    }
}

/// Creates the eventlist at compile time
#[inline(always)]
pub(crate) const fn eventlist() -> [libc::kevent; KEVENT_COUNT] {
    unsafe { mem::zeroed() }
}
