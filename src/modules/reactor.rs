//! Reactor
//! Reacts to kevents from the kernal
//! and propogates them back to the caller

use std::{sync::mpsc::Sender, thread::{self, Thread}};
use crate::{RuntimeError, modules::{event_type::EventType, int_check::IntCheck, kevent::{KEvent, eventlist}}};

/// Reacts to kevents from the kernel
pub(crate) struct Reactor;

impl Reactor {
    /// Initialises the `Reactor`
    ///
    /// The id it was given is sent back down `tx` when the loop
    /// gives up on it, still open, for the caller to replace and close
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

                match event.filter.into() {
                    EventType::Sleep => {
                        let raw = event.udata as *mut Thread;

                        if raw.is_null() {
                            continue;
                        }

                        unsafe { Box::from_raw(raw).unpark() };
                    },
                    EventType::Unknown => (),
                }
            }
        }

        let _ = tx.send(id);
    });
}
