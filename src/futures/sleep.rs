//! # Sleep
//! The `Sleep` future waits for a set time
//! then continues

use crate::futures::sleep_task::SleepTask;
use std::time::Duration;

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
    /// ## Behaviour
    /// Checks the length of the `Duration`
    /// and if it's too short to warrant
    /// the overhead of a syscall, starts
    /// a spinlock until the duration is over
    ///
    /// When `p_mode` is `false` it will always
    /// create a new `kevent` even if this means
    /// waiting longer than the duration specifies
    ///
    /// The crossover for this is
    /// approximately 500 microseconds
    ///
    /// ## `p_mode`
    /// Trades cpu time for precision
    ///
    /// On, the last stretch of the wait is spun
    /// rather than slept, which burns a core for
    /// up to the crossover duration on every call
    ///
    /// Off, every sleep is handed to the kernel
    /// no matter how short it is, and whatever
    /// the kernel returns is the answer. No core
    /// is burnt
    ///
    /// #### Note
    /// A spawned sleep with `p_mode` off, or one
    /// long enough to pass the crossover either
    /// way, is handed to a sleep thread rather
    /// than run on a worker, so it never holds a
    /// worker up. A spinning sleep stays where it
    /// is, because it never gives the thread up
    ///
    /// ## Accuracy
    /// Measured on apple silicon across
    /// targets from 400 nanoseconds up to
    /// 30 seconds
    ///
    /// #### With p_mode
    /// Around 200 nanoseconds over, at every
    /// duration in that range
    ///
    /// Measured around 5200x more accurate
    /// that `thread::sleep()`
    ///
    /// #### Without p_mode
    /// Around 4 microseconds over on short
    /// waits, and around 60 once the wait is
    /// a few milliseconds or more
    ///
    /// Measured around 37x more accurate
    /// that `thread::sleep()`
    ///
    /// ## Returns
    /// The total time the function ran for
    /// from start to finish
    pub fn sleep(time: Duration, p_mode: bool) -> SleepTask {
        SleepTask::new(time, p_mode)
    }
}
