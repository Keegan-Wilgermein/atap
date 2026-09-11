//! # Waiter
//! A parked thread waiting for the `Reactor`, used when a
//! thread couldn't get a kqueue of its own

use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::{self, Thread},
};

/// A thread waiting for the `Reactor` to wake it
///
/// Lives on the waiting thread's stack. The `Reactor` only
/// holds a pointer to it while the thread is waiting
pub(crate) struct Waiter {
    /// The thread to unpark
    thread: Thread,

    /// Whether the event has arrived
    ///
    /// Separates a real wake from `park` returning spuriously
    fired: AtomicBool,
}

impl Waiter {
    /// A waiter for the calling thread
    pub(crate) fn new() -> Self {
        Self {
            thread: thread::current(),
            fired: AtomicBool::new(false),
        }
    }

    /// Blocks until the event arrives
    pub(crate) fn wait(&self) {
        while !self.fired.load(Ordering::Acquire) {
            thread::park();
        }
    }

    /// Says the event arrived, and wakes the waiter
    ///
    /// The flag is set before the unpark, so a waiter always
    /// finds it
    pub(crate) fn wake(&self) {
        self.fired.store(true, Ordering::Release);
        self.thread.unpark();
    }
}
