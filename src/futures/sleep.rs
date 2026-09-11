//! # Sleep
//! Tasks that wait for a set time

use crate::futures::sleep_task::SleepTask;
use std::time::Duration;

/// Builds sleep tasks
///
/// Doesn't implement `Task` itself. `Sleep::sleep` returns one
pub struct Sleep;

impl Sleep {
    /// Creates a task that sleeps for `time`
    ///
    /// ## `p_mode`
    /// Trades CPU time for precision
    ///
    /// On, a sleep shorter than about 500 microseconds is spun
    /// entirely, and a longer one spins its last 500 microseconds.
    /// Either way a core is busy for up to 500 microseconds
    ///
    /// Off, every sleep goes to the kernel however short it is,
    /// and whatever the kernel returns is the answer. No core is
    /// burnt
    ///
    /// ## Accuracy
    /// Measured on Apple silicon, for targets from 400 nanoseconds
    /// to 30 seconds
    ///
    /// With `p_mode`, around 200 nanoseconds over at every
    /// duration — about 5200x more accurate than `thread::sleep()`
    ///
    /// Without, around 4 microseconds over on short waits and
    /// around 60 on waits of a few milliseconds or more — about
    /// 37x more accurate than `thread::sleep()`
    ///
    /// ## Returns
    /// How long it actually took, from start to finish
    pub fn sleep(time: Duration, p_mode: bool) -> SleepTask {
        SleepTask::new(time, p_mode)
    }
}
