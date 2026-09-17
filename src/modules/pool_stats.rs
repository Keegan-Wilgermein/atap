//! # Pool Stats
//! What the whole worker pool looked like at the moment it
//! was asked

use crate::modules::worker_stats::WorkerStats;
use std::fmt;

/// A snapshot of the worker pool
///
/// #### Note
/// A snapshot, not a lock. The pool carries on while this is
/// read, so treat the numbers as approximate
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PoolStats {
    live: usize,
    queued: usize,
    blocking_queued: usize,
    workers: Vec<WorkerStats>,
    sleep_threads: usize,
    sleep_busy: usize,
    slots: usize,
    peak_slots: usize,
    target: usize,
    ceiling: usize,
    sleep_target: usize,
    peak_workers: usize,
    peak_sleep_threads: usize,
    deaths: usize,
    recovering: usize,
}

impl PoolStats {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        live: usize,
        queued: usize,
        blocking_queued: usize,
        workers: Vec<WorkerStats>,
        sleep_threads: usize,
        sleep_busy: usize,
        slots: usize,
        peak_slots: usize,
        target: usize,
        ceiling: usize,
        sleep_target: usize,
        peak_workers: usize,
        peak_sleep_threads: usize,
        deaths: usize,
        recovering: usize,
    ) -> Self {
        Self {
            live,
            queued,
            blocking_queued,
            workers,
            sleep_threads,
            sleep_busy,
            slots,
            peak_slots,
            target,
            ceiling,
            sleep_target,
            peak_workers,
            peak_sleep_threads,
            deaths,
            recovering,
        }
    }

    /// Tasks holding a slot in the table right now, whether
    /// queued, running or waiting to be read
    pub fn live(&self) -> usize {
        self.live
    }

    /// Tasks waiting in the shared queue
    pub fn queued(&self) -> usize {
        self.queued
    }

    /// Blocking tasks waiting for a sleep thread
    pub fn blocking_queued(&self) -> usize {
        self.blocking_queued
    }

    /// Every worker that was running when this was taken
    pub fn workers(&self) -> &[WorkerStats] {
        &self.workers
    }

    /// Threads currently held open for blocking tasks
    pub fn sleep_threads(&self) -> usize {
        self.sleep_threads
    }

    /// How many of those were inside a task
    pub fn sleep_busy(&self) -> usize {
        self.sleep_busy
    }

    /// Task slots the table spans right now
    ///
    /// ## Behaviour
    /// Grows when a spawn finds no free slot to reuse, and shrinks
    /// when the table is trimmed. The runtime trims itself when it
    /// has been idle for a while, so this can fall without anything
    /// being asked of it
    pub fn slots(&self) -> usize {
        self.slots
    }

    /// The most task slots the table has ever spanned at once
    ///
    /// ## Behaviour
    /// Never comes down, not even when the table is trimmed. It is
    /// the peak number of tasks alive at once, not the number ever
    /// spawned
    pub fn peak_slots(&self) -> usize {
        self.peak_slots
    }

    /// Workers the pool settles around under load
    ///
    /// A target rather than a limit. The pool passes it on overload or
    /// real need
    pub fn target(&self) -> usize {
        self.target
    }

    /// The most threads of either kind the pool will ever run
    ///
    /// The lower of the pool's own bound and what the kernel lets the
    /// process hold, less a reserve for every other thread
    pub fn ceiling(&self) -> usize {
        self.ceiling
    }

    /// Sleep threads the pool settles around under load, a target in
    /// the same way as [`target`](PoolStats::target)
    pub fn sleep_target(&self) -> usize {
        self.sleep_target
    }

    /// The most workers ever running at once
    ///
    /// Never comes down, the same as `peak_slots`
    pub fn peak_workers(&self) -> usize {
        self.peak_workers
    }

    /// The most sleep threads ever running at once
    ///
    /// Never comes down, the same as `peak_slots`
    pub fn peak_sleep_threads(&self) -> usize {
        self.peak_sleep_threads
    }

    /// Workers and sleep threads that have died since the process
    /// started
    ///
    /// A thread only dies to a fault in the runtime itself, never to a
    /// task that panics. The pool recovers from each one
    pub fn deaths(&self) -> usize {
        self.deaths
    }

    /// Threads that have died and haven't been recovered yet
    ///
    /// Comes back to zero on its own
    pub fn recovering(&self) -> usize {
        self.recovering
    }

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
        self.workers.iter().filter(|worker| worker.busy()).count()
    }

    /// Everything waiting anywhere: the shared queue, the blocking
    /// queue and every worker's own
    pub fn backlog(&self) -> usize {
        let workers: usize = self.workers.iter().map(|worker| worker.backlog()).sum();

        self.queued + self.blocking_queued + workers
    }

    /// Whether the pool had anything at all to do
    ///
    /// #### Note
    /// Says the pool was idle when this was taken, not that nothing
    /// is coming
    pub fn has_any_task(&self) -> bool {
        self.busy() > 0 || self.sleep_busy > 0 || self.backlog() > 0
    }
}

impl fmt::Display for PoolStats {
    /// The whole pool over several lines: its threads, what is
    /// queued, one line per worker, then the table
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
            "workers target {} (peak {}), sleep threads target {} (peak {}), ceiling {}, {} deaths ({} recovering)",
            self.target,
            self.peak_workers,
            self.sleep_target,
            self.peak_sleep_threads,
            self.ceiling,
            self.deaths,
            self.recovering,
        )?;

        writeln!(
            formatter,
            "{} queued, {} blocking, {} backlog",
            self.queued,
            self.blocking_queued,
            self.backlog(),
        )?;

        let width = match self.workers.len() {
            0 => 1,
            count => (count - 1).to_string().len(),
        };

        for (index, worker) in self.workers.iter().enumerate() {
            writeln!(formatter, "  worker {index:>width$}: {worker}")?;
        }

        write!(
            formatter,
            "{} live, {} slots in the table, {} at its peak",
            self.live, self.slots, self.peak_slots,
        )
    }
}
