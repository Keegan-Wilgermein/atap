//! # Sleep
//! Tasks that wait for a set time

use crate::futures::sleep_task::SleepTask;
use std::time::Duration;

/// What a sleep trades for accuracy
///
/// Set with [`SleepTask::mode`], and `Precise` without it
///
/// [`SleepTask::mode`]: crate::SleepTask::mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SleepMode {
    /// Spins the last stretch of the wait rather than sleeping it
    ///
    /// A wait shorter than about 500 microseconds is spun entirely,
    /// and a longer one spins its last 500 microseconds. Either way
    /// a core is busy for up to 500 microseconds
    #[default]
    Precise,

    /// Leaves the whole wait to the kernel
    ///
    /// Whatever the kernel gives back is the answer, however short
    /// the wait. No core is burnt
    Relaxed,
}

/// Builds sleep tasks
///
/// Doesn't implement `Task` itself. `Sleep::sleep` returns one
pub struct Sleep;

impl Sleep {
    /// Creates a task that sleeps for `time`
    ///
    /// ## Behaviour
    /// Precise by default, which spins the last stretch of the wait
    /// for accuracy. [`SleepTask::mode`] trades that back for a core
    /// that stays idle:
    ///
    /// ```ignore
    /// Sleep::sleep(time).mode(SleepMode::Relaxed)
    /// ```
    ///
    /// ## Returns
    /// How long it actually took, from start to finish
    ///
    /// [`SleepTask::mode`]: crate::SleepTask::mode
    pub fn sleep(time: Duration) -> SleepTask {
        SleepTask::new(time)
    }
}
