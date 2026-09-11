//! # Worker Stats
//! What one worker looked like at the moment it was asked

use std::fmt;

/// A snapshot of one worker
///
/// #### Note
/// A snapshot, not a lock. The worker carries on while this is
/// read, so treat the numbers as approximate
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerStats {
    busy: bool,
    backlog: usize,
    completed: u64,
}

impl WorkerStats {
    pub(crate) fn new(busy: bool, backlog: usize, completed: u64) -> Self {
        Self {
            busy,
            backlog,
            completed,
        }
    }

    /// Whether the worker was inside a task
    pub fn busy(&self) -> bool {
        self.busy
    }

    /// Tasks waiting in the worker's own queue
    pub fn backlog(&self) -> usize {
        self.backlog
    }

    /// Tasks this worker has finished since it started
    ///
    /// Reset when the worker is restarted
    pub fn completed(&self) -> u64 {
        self.completed
    }
}

impl fmt::Display for WorkerStats {
    /// One worker on one line: busy or idle, its backlog, and what
    /// it has finished
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
