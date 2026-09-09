//! # Reactor
//! Reacts to kevents from the kernal
//! and propogates them back to the caller

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
                    // Nobody registered a way back, so there
                    // is nobody to tell about it
                    WakeTarget::None => continue,

                    // The waiter is sitting in a `kevent` call
                    // on a queue of its own, and one trigger
                    // both registers and fires
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

                    // The waiter is parked. Its flag goes up
                    // before the unpark, so it finds the event
                    // however it came out of `park`
                    //
                    // The pointer is borrowed from the waiting
                    // thread's own stack, which is live for as
                    // long as it is waiting, so nothing here
                    // owns it or frees it
                    WakeTarget::Parked(waiter) => unsafe { (*waiter).wake() },
                }
            }
        }

        let _ = tx.send(id);
    });
}
