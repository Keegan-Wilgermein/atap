//! Sleep task
//! The tasks associated with sleeping
//! 
//! Performs sleep functions defined by the `Sleep`
//! struct

use std::time::{Duration};

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
}

impl SleepTask {
    /// Creates a new `SleepTask`
    pub(crate) fn new(time: Duration) -> Self {
        Self {
            sleep_for: time,
        }
    }
}
