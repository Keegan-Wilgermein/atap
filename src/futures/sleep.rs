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
    /// ## Behaviour
    /// Checks the length of the `Duration`
    /// and if it's too short to warrant
    /// the overhead of a syscall, just pauses
    /// the current thread until finished
    /// 
    /// The crossover for this is
    /// approximately 1 millisecond
    /// 
    /// #### Note
    /// If called inside `block_on()` this
    /// will block the thread for it's entire
    /// duration regardless
    /// 
    /// ## p_mode
    /// Trades cpu time for precision
    /// 
    /// On, the calling thread is promoted into
    /// the realtime band and the last stretch of
    /// the wait is spun rather than slept, which
    /// burns a core for up to the crossover
    /// duration on every call
    /// 
    /// Off, every sleep is handed to the kernel
    /// no matter how short it is, and whatever
    /// the kernel returns is the answer. No core
    /// is burnt and no thread priority is touched
    /// 
    /// #### Note
    /// The realtime promotion lasts for the life
    /// of the thread. Passing `false` on a later
    /// call from the same thread doesn't undo it
    /// 
    /// ## Returns
    /// The total time the function ran for
    /// from start to finish
    pub fn sleep(time: Duration, p_mode: bool) -> SleepTask {
        SleepTask::new(time, p_mode)
    }
}
