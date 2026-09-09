//! # Worker Stats
//! What one worker looked like at the moment it was asked

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
    pub completed: u64,
}
