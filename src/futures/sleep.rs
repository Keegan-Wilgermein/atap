//! # Sleep
//! The `Sleep` future waits for a set time
//! then continues

use std::time::{Duration};
use crate::futures::sleep_task::SleepTask;

/// The base struct
/// 
/// It doesn't implement `Task`
/// so it can't be passed into a
/// runtime function directly
/// without calling a method on it
/// that returns something that does
pub struct Sleep;

impl Sleep {
    /// Creates a new task that will be
    /// executed when passed into a runtime
    /// 
    /// ### Behaviour
    /// Checks the length of the `Duration`
    /// and if it's too short to warrant
    /// the overhead of a syscall, just pauses
    /// the current thread until finished
    /// 
    /// The crossover for this is
    /// approximately 250ns
    /// 
    /// #### Note
    /// If called inside `block_on()` this
    /// will block the thread for it's entire
    /// duration regardless
    pub fn sleep(time: Duration) -> SleepTask {
        SleepTask::new(time)
    }
}
