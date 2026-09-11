//! # Reactor
//! Reacts to kevents from the kernel and passes them back to
//! whoever is waiting on them

use crate::{
    EventDesc, RuntimeError,
    constants::WAKE_IDENT,
    modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        wake_target::WakeTarget,
    },
};
use std::{ptr, sync::mpsc::Sender, thread};

/// Reacts to kevents from the kernel
pub(crate) struct Reactor;

impl Reactor {
    /// Initialises the `Reactor`
    pub(crate) fn init(id: i32, tx: Sender<i32>) {
        reactor_loop(id, tx);
    }
}

/// The loop the `Reactor` follows
#[inline(always)]
fn reactor_loop(id: i32, tx: Sender<i32>) {
    thread::spawn(move || {
        let mut events = eventlist();

        loop {
            let count = match unsafe { KEvent::listen(id, &mut events) }.check() {
                Ok(count) => count as usize,
                Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
                Err(_) => break,
            };

            for event in events.iter().take(count) {
                if event.flags & libc::EV_ERROR != 0 {
                    continue;
                }

                match WakeTarget::decode(event.udata) {
                    // Nobody to wake
                    WakeTarget::None => continue,

                    // The waiter is blocked in `kevent` on its own queue
                    WakeTarget::Queue(queue) => {
                        let _ = unsafe {
                            KEvent::register(
                                queue,
                                WAKE_IDENT,
                                0,
                                ptr::null_mut(),
                                EventDesc::new_user_trigger(),
                            )
                        }
                        .check();
                    }

                    // The pointer is into the waiting thread's stack, which stays
                    // live until it has been woken
                    WakeTarget::Parked(waiter) => unsafe { (*waiter).wake() },
                }
            }
        }

        let _ = tx.send(id);
    });
}
