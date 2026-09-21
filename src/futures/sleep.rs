//! # Sleep
//! Tasks that wait for a set time

use crate::futures::sleep_task::SleepTask;
use std::time::{Duration, Instant};

/// What a sleep trades for accuracy
///
/// Set with [`SleepTask::mode`], and `Precise` without it
///
/// [`SleepTask::mode`]: crate::sleep::SleepTask::mode
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
    /// ```no_run
    /// # use atap::sleep::{Sleep, SleepMode};
    /// # let time = std::time::Duration::from_millis(1);
    /// let task = Sleep::sleep(time).mode(SleepMode::Relaxed);
    /// ```
    ///
    /// ## Returns
    /// How long it actually took, from start to finish
    ///
    /// [`SleepTask::mode`]: crate::sleep::SleepTask::mode
    pub fn sleep(time: Duration) -> SleepTask {
        SleepTask::new(time)
    }

    /// Creates a task that sleeps until `when`
    ///
    /// ## Behaviour
    /// The same as [`Sleep::sleep`] for however long is left when the
    /// task starts. A moment already past returns at once
    ///
    /// ## Returns
    /// How long it actually took, from start to finish
    ///
    /// #### Note
    /// `when` doesn't move, so a run that starts after it returns at
    /// once. A repeat, or a task that waits for gives, only sleeps on
    /// its first run
    pub fn until(when: Instant) -> SleepTask {
        SleepTask::until(when)
    }
}
