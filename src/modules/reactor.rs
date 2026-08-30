//! Reactor
//! Reacts to kevents from the kernal
//! and propogates them back to the caller

use std::thread::{self, Thread};
use crate::modules::{event_type::EventType, int_check::IntCheck, kevent::{KEvent, eventlist}};

/// Reacts to kevents from the kernel
pub(crate) struct Reactor;

impl Reactor {
    /// Initialises the `Reactor`
    pub(crate) fn init(id: i32) {
        reactor_loop(id);
    }
}

/// The loop the `Reactor` follows
#[inline(always)]
fn reactor_loop(id: i32) {
    thread::spawn(move || {
        let mut events = eventlist();
        
        loop {
            let count = unsafe { KEvent::listen(id, &mut events) }.check();

            if count < 0 {
                continue;
            }

            events.iter()
            .take(count as usize)
            .for_each(|event| {
                match event.filter.into() {
                    EventType::Sleep => {
                        let raw = event.udata as *mut Thread;
                        unsafe { Box::from_raw(raw).unpark() };
                    },
                    EventType::Unknown => (),
                }
            });
        }
    });
}
