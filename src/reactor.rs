//! # Reactor
//! 
//! The reactor reacts to events from the kernel
//! and propogates them to the correct task

use std::{io::Error, mem, ptr, task::Waker, thread};
use libc::{kevent};
use crate::{modules::{kevent::KEvent}};

pub(crate) struct Reactor {
    kqueue: i32,
}

impl Reactor {
    pub(crate) fn new(
        kqueue: i32,
    ) -> Self {
        reactor_thread(kqueue);
        Self {
            kqueue,
        }
    }
}

fn reactor_thread(
    kqueue: i32,
) {
    thread::spawn(move || {
        let mut event_list: [KEvent; 64];

        loop {
            event_list = unsafe { mem::zeroed() };

            // Block until event occurs
            let num_events = unsafe {
                kevent(
                    kqueue,
                    ptr::null(),
                    0,
                    event_list.as_mut_ptr(),
                    event_list.len() as i32,
                    ptr::null(),
                )
            };

            if num_events < 0 {
                status_error_check();
                continue;
            } else if num_events == 0 {
                unreachable!("Reactor returned 0 from kevent() call");
            } else {
                event_list[..num_events as usize]
                .iter()
                .for_each(|event| {
                    if event.flags & libc::EV_ERROR != 0 { // Something went wrong but was reported correct

                    } else { // Wakes executor to continue task
                        let waker = unsafe { &*(event.udata as *mut Waker) };
                        waker.clone().wake();
                    }
                });
            }
        }
    });
}

/// Checks the last os error in std_err
/// and panics if it's not normal
pub(crate) fn status_error_check() {
    let error = Error::last_os_error();

    match error.raw_os_error() {
        // kevent() block interrupt
        Some(libc::EINTR) => (),
        Some(libc::EBADF) => panic!("Reactor: bad file descriptor: {error}"),
        Some(libc::EINVAL) => panic!("Reactor: invalid argument to kevent: {error}"),
        Some(libc::ENOMEM) => panic!("Reactor: kernel out of memory: {error}"),
        Some(libc::EACCES) => panic!("Reactor: permission denied: {error}"),
        Some(other_error) => panic!("Reactor: unexpected kevent error {other_error}: {error}"),
        None => unreachable!("last_os_error() should always return a Some() value but returned: {error}"),
    }
}
