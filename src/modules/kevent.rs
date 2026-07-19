//! Alias for `kevent` so the compiler doesn't
//! get confused betwwen the struct and the function

use std::{ffi::c_void, task::Waker};
use libc::{EV_ADD, EV_ONESHOT, STDIN_FILENO, kevent};
use crate::{modules::interest::Interest};

/// Alias for `libc::kevent`
pub(crate) type KEvent = kevent;

/// Creates a new `KEvent` for registration
pub(crate) fn new_kevent(interest: Interest, waker: Waker) -> KEvent {
    KEvent {
        ident: STDIN_FILENO as usize,
        filter: interest.into(),
        flags: EV_ADD | EV_ONESHOT,
        fflags: 0,
        data: 0,
        udata: &waker as *const Waker as *mut c_void,
    }
}
