//! # Waiter
//! A parked thread and the flag that says whether the event
//! it is waiting for has actually arrived
//!
//! Only used when a thread can't get a kqueue of its own to
//! wait on. Everything else waits the cheaper way, in a
//! `kevent` call the `Reactor` triggers

use std::{
    sync::atomic::{AtomicBool, Ordering},
    thread::{self, Thread},
};

/// A thread waiting for the `Reactor` to wake it
///
/// Lives on the waiting thread's own stack rather than in a
/// box. A waiter is blocked for as long as it is waiting, so
/// its frame outlives every use the `Reactor` can make of a
/// pointer to it, and nothing has to be allocated or handed
/// over to be freed by whoever gets there last
pub(crate) struct Waiter {
    /// The thread to unpark
    thread: Thread,

    /// Whether the event has arrived
    ///
    /// `park` is allowed to come back without anyone having
    /// unparked, so the flag is the only thing that separates
    /// a real wake from a spurious one. Without it a thread
    /// takes the first twitch for its event and carries on as
    /// though the kernel had answered
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
    /// The flag is set first so that a thread coming out of
    /// `park` always finds it, whether it was this unpark
    /// that woke it or something else entirely
    pub(crate) fn wake(&self) {
        self.fired.store(true, Ordering::Release);
        self.thread.unpark();
    }
}
