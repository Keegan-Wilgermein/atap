//! Reactor
//! Reacts to kevents from the kernal
//! and propogates them back to the caller

use std::{thread, time::Instant};
use crate::modules::{int_check::IntCheck, kevent::KEvent};

/// Reacts to kevents from the kernel
pub(crate) struct Reactor;

impl Reactor {
    /// Initialises the `Reactor`
    pub(crate) fn init(id: i32) {
        reactor_loop(id);
    }
}

/// The loop the `Reactor` follows
fn reactor_loop(id: i32) {
    thread::spawn(move || {
        loop {
            let event = unsafe { KEvent::listen(id) }.check();

            let now = Instant::now();
            println!("Detected {} events at time: {:?}", event, now);
        }
    });
}
