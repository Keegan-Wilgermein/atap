//! Sleep task
//! The tasks associated with sleeping
//! 
//! Performs sleep functions defined by the `Sleep`
//! struct

use std::{time::{Duration, Instant}};

/// The struct that implements `Task`
/// 
/// It can be passed into async functions
/// and contains info on its functionality
/// 
/// All it's runtime functions output `()`
/// as it only needs to notify when it's done
/// 
/// Any other functions are for convenience
pub struct SleepTask {
    /// How long to sleep for
    pub(crate) sleep_for: Duration,

    /// Whether to trade cpu for precision
    ///
    /// On, the thread is promoted into the realtime band and
    /// the last stretch is spun rather than slept. Off, every
    /// sleep is handed to the kernel and whatever comes back
    /// is the answer
    pub(crate) p_mode: bool,
}

impl SleepTask {
    /// Creates a new `SleepTask`
    pub(crate) fn new(time: Duration, p_mode: bool) -> Self {
        Self {
            sleep_for: time,
            p_mode,
        }
    }

    /// Spins the cpu until hitting the given time parameter
    pub(crate) fn spinlock(&self, until: Instant) {
        while Instant::now() < until {}
    }
}
