//! # Worker Stats
//! What one worker looked like at the moment it was asked

use std::fmt;

/// A snapshot of one worker
///
/// #### Note
/// A snapshot and not a lock. Every number in here was true
/// when it was read and may not be by the time it is looked
/// at, because the worker it describes carries on working
/// throughout. Useful for tuning and for watching the pool
/// behave, not for deciding anything that has to be exact
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerStats {
    /// Whether the worker was inside a task
    pub busy: bool,

    /// Tasks waiting in the worker's own queue
    pub backlog: usize,

    /// Tasks this worker has finished since it started
    /// 
    /// Worker 0 is never cleared unless it crashes on a task
    /// and restarts
    pub completed: u64,
}

impl fmt::Display for WorkerStats {
    /// One worker on one line
    ///
    /// Says nothing about which worker it is, because a
    /// `WorkerStats` doesn't know — it is a snapshot of a
    /// worker, not a place in the pool. `PoolStats` numbers
    /// them, since it is the thing holding them in order
    ///
    /// Reads the way it would be said out loud: what it was
    /// doing, what was waiting for it, and how much it has got
    /// through since it started
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}, {} queued, {} done",
            match self.busy {
                true => "busy",
                false => "idle",
            },
            self.backlog,
            self.completed,
        )
    }
}
