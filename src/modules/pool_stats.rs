//! # Pool Stats
//! What the whole worker pool looked like at the moment it
//! was asked

use crate::modules::worker_stats::WorkerStats;

/// A snapshot of the pool
///
/// #### Note
/// A snapshot and not a lock, for the same reason `WorkerStats`
/// is. The pool grows, shrinks and moves work around while this
/// is being read, so treat it as a look at the pool rather than
/// a statement about it
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolStats {
    /// Tasks waiting in the shared queue, behind every
    /// worker's own
    pub queued: usize,

    /// Tasks that said they would block, waiting for a sleep
    /// thread to be free
    pub blocking_queued: usize,

    /// Every worker that was running when this was taken
    pub workers: Vec<WorkerStats>,

    /// Threads currently held open for blocking tasks
    pub sleep_threads: usize,

    /// How many of those were inside a task
    pub sleep_busy: usize,

    /// Task slots the table has ever handed out
    ///
    /// Only ever climbs, and only when no retired slot could
    /// be reused, so it is the peak number of tasks that have
    /// been alive at once rather than the number ever spawned
    pub slots: usize,
}

impl PoolStats {
    /// Workers running when this was taken
    pub fn len(&self) -> usize {
        self.workers.len()
    }

    /// Whether the pool had no workers at all
    pub fn is_empty(&self) -> bool {
        self.workers.is_empty()
    }

    /// Workers that were inside a task
    pub fn busy(&self) -> usize {
        self.workers.iter().filter(|worker| worker.busy).count()
    }

    /// Everything waiting anywhere, shared queue and worker
    /// queues and sleep threads alike
    pub fn backlog(&self) -> usize {
        let workers: usize = self.workers.iter().map(|worker| worker.backlog).sum();

        self.queued + self.blocking_queued + workers
    }

    /// Whether the pool had anything at all to do
    ///
    /// Covers both halves of running and both halves of
    /// waiting: a task on a worker, a task on a sleep thread,
    /// and anything queued in front of either of them
    ///
    /// #### Note
    /// `false` says the pool had nothing left at the moment
    /// this was taken, not that nothing is coming. A task
    /// spawned a moment later is still a task, so this answers
    /// what the pool was doing rather than whether it is
    /// finished
    pub fn has_any_task(&self) -> bool {
        self.busy() > 0 || self.sleep_busy > 0 || self.backlog() > 0
    }
}
