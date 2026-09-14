//! # Sleep Thread
//! Threads that run blocking tasks, so no worker is ever held
//! inside a long wait
//!
//! They all pull from one shared queue, so whichever is free
//! next takes the next blocking task

use crate::{
    executor,
    modules::{
        exit_guard::ExitGuard,
        faults,
        thread_slot::{PoolThread, ThreadSlot},
        worker_pool::POOL,
        worker_state::WorkerState,
    },
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// One thread that exists to be blocked
#[repr(C)]
pub(crate) struct SleepThread {
    /// Where the thread is, the task it is holding, and how long it
    /// has been idle
    slot: ThreadSlot,

    /// Tasks finished since the thread started
    completed: AtomicUsize,

    /// What `completed` read when the manager last looked
    watched: AtomicUsize,
}

impl SleepThread {
    /// A slot with no thread behind it yet
    pub(crate) const fn new() -> Self {
        Self {
            slot: ThreadSlot::new(),
            completed: AtomicUsize::new(0),
            watched: AtomicUsize::new(0),
        }
    }

    /// Puts a thread behind this slot
    ///
    /// ## Returns
    /// Whether the thread started. If it didn't, the slot is freed
    /// again
    pub(crate) fn start(&'static self) -> bool {
        self.slot.start("atap-sleep", None, self, Self::run)
    }

    /// Whether this thread has finished anything since the manager
    /// last looked, recording that it has now looked
    #[inline(always)]
    pub(crate) fn moved(&self) -> bool {
        let completed = self.completed.load(Ordering::Relaxed);

        self.watched.swap(completed, Ordering::Relaxed) != completed
    }

    /// Empties the slot, giving back the task it was running, if
    /// any
    pub(crate) fn recover(&self) -> usize {
        let stranded = self.slot.take_held();

        self.slot.busied();
        self.slot.empty();

        stranded
    }

    /// The loop the thread follows
    ///
    /// The guard marks the slot dead if a panic unwinds through
    /// here
    fn run(&'static self) {
        let mut guard = ExitGuard::new(self);

        self.slot.started();

        loop {
            if self.slot.state() == WorkerState::Stopping {
                break;
            }

            faults::sleep_thread_dies();

            let Some(id) = POOL.blocking().pop() else {
                self.park();
                continue;
            };

            self.slot.hold(id);

            // Lost to a stop, so the task goes back to the queue
            if !self.slot.begin_task() {
                self.slot.put_down();

                if !POOL.blocking().push(id) {
                    executor::fail(id);
                }

                break;
            }

            faults::sleep_thread_dies();

            executor::run(id);

            self.slot.put_down();
            self.completed.fetch_add(1, Ordering::Relaxed);

            self.slot.end_task();
        }

        guard.mark_clean();
    }

    /// Blocks until there is something to do or somebody says to
    /// stop
    ///
    /// An offload queues before it checks for parked threads, so a
    /// task can't slip between the park and the last look
    fn park(&self) {
        self.slot.park(
            || POOL.sleep_parked_in(),
            || POOL.sleep_parked_out(),
            || !POOL.blocking().is_empty(),
        );
    }
}

impl PoolThread for SleepThread {
    #[inline(always)]
    fn slot(&self) -> &ThreadSlot {
        &self.slot
    }

    /// Empties the slot and gives the thread's place back
    fn left(&'static self) {
        self.recover();
        POOL.sleep_left();
    }
}
