//! # Reactor
//! Reacts to kevents from the kernal
//! and propogates them back to the caller

use crate::{
    RuntimeError,
    modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
    },
};
use std::{
    sync::mpsc::Sender,
    thread::{self, Thread},
};

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

                let raw = event.udata as *mut Thread;

                if raw.is_null() {
                    continue;
                }

                unsafe { Box::from_raw(raw).unpark() };
            }
        }

        let _ = tx.send(id);
    });
}
