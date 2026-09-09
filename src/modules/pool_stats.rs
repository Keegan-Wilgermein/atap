//! # Pool Stats
//! What the whole worker pool looked like at the moment it
//! was asked

use crate::modules::worker_stats::WorkerStats;
use std::fmt;

/// A snapshot of the pool
///
/// #### Note
/// A snapshot and not a lock, for the same reason `WorkerStats`
/// is. The pool grows, shrinks and moves work around while this
/// is being read, so treat it as a look at the pool rather than
/// a statement about it
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolStats {
    /// Tasks holding a slot in the table right now, whether
    /// queued, running or waiting to be read
    pub live: usize,

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

impl fmt::Display for PoolStats {
    /// The whole pool, with its workers listed out
    ///
    /// Three parts, in the order the work moves through them.
    /// The threads that exist come first, then what is waiting
    /// for them, then a line for every worker, and last the
    /// table underneath the lot — so reading down it goes from
    /// the pool, through the queue, to where the tasks
    /// themselves live
    ///
    /// #### Note
    /// Several lines rather than one, which is unusual for a
    /// `Display`. A pool with thirty two workers has thirty two
    /// things to say and saying them on one line says none of
    /// them. Use the `Debug` form where a single line is what
    /// is wanted
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "{} workers ({} busy), {} sleep threads ({} busy)",
            self.len(),
            self.busy(),
            self.sleep_threads,
            self.sleep_busy,
        )?;

        writeln!(
            formatter,
            "{} queued, {} blocking, {} backlog",
            self.queued,
            self.blocking_queued,
            self.backlog(),
        )?;

        // Numbered to the width of the highest, so the names
        // line up and the columns after them do too. A pool of
        // ten reads as `worker 9` and one of a hundred as
        // `worker  9`, rather than the list stepping sideways
        // as it passes each power of ten
        let width = match self.workers.len() {
            0 => 1,
            count => (count - 1).to_string().len(),
        };

        for (index, worker) in self.workers.iter().enumerate() {
            writeln!(formatter, "  worker {index:>width$}: {worker}")?;
        }

        write!(formatter, "{} live, {} slots", self.live, self.slots)
    }
}
