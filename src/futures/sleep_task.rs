//! # Sleep task
//! The tasks associated with sleeping
//!
//! Performs sleep functions defined by the `Sleep`
//! struct

use std::time::{Duration, Instant};

/// The version of `Sleep` that implements `Task`
///
/// It can be passed into async functions
/// and contains info on its functionality
///
/// All it's runtime functions output `Duration`
/// describing the time it took for the function
/// to run in it's entirety
///
/// #### Note
/// `Clone` because `Runtime::every` makes a fresh copy of its
/// task for every run it starts. A copy carries the duration
/// and the mode across and nothing else that matters — the
/// start time is overwritten by `prepare` before the copy is
/// ever run, so each run is timed from its own beginning
#[derive(Clone)]
pub struct SleepTask {
    /// How long to sleep for
    pub(crate) sleep_for: Duration,

    /// When the task started execution
    pub(crate) created: Instant,

    /// Whether to trade cpu for precision
    ///
    /// On, the last stretch of the wait is spun rather than
    /// slept. Off, every sleep is handed to the kernel and
    /// whatever comes back is the answer
    pub(crate) p_mode: bool,
}

impl SleepTask {
    /// Creates a new `SleepTask`
    pub(crate) fn new(time: Duration, p_mode: bool) -> Self {
        Self {
            sleep_for: time,
            created: Instant::now(),
            p_mode,
        }
    }

    /// Spins the cpu until hitting the given time parameter
    pub(crate) fn spinlock(&self, until: Instant) {
        while Instant::now() < until {}
    }
}
