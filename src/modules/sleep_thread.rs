//! # Sleep Thread
//! Threads that run blocking tasks, so no worker is ever held
//! inside a long wait
//!
//! They all pull from one shared queue, so whichever is free
//! next takes the next blocking task

use crate::{
    constants::NO_TASK,
    executor,
    modules::{address_lock, worker_pool::POOL, worker_state::WorkerState},
};
use std::{
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
    thread,
};

/// One thread that exists to be blocked
#[repr(C)]
pub(crate) struct SleepThread {
    /// Where the thread is, and the address it parks on
    state: AtomicU32,

    /// The task being run right now, or `NO_TASK`
    ///
    /// Kept here so a thread that dies leaves a note of which task
    /// went with it
    current: AtomicUsize,

    /// Tasks finished since the thread started
    completed: AtomicUsize,

    /// Manager ticks this thread has been idle for
    idle_ticks: AtomicU32,
}

impl SleepThread {
    /// A slot with no thread behind it yet
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU32::new(WorkerState::Empty as u32),
            current: AtomicUsize::new(NO_TASK),
            completed: AtomicUsize::new(0),
            idle_ticks: AtomicU32::new(0),
        }
    }

    /// The current state
    #[inline(always)]
    pub(crate) fn state(&self) -> WorkerState {
        WorkerState::from_u32(self.state.load(Ordering::Acquire))
    }

    /// Whether a thread is behind this slot at all
    #[inline(always)]
    pub(crate) fn running(&self) -> bool {
        self.state().alive()
    }

    /// Whether it is inside a task right now
    #[inline(always)]
    pub(crate) fn busy(&self) -> bool {
        self.state().busy()
    }

    /// Notes another idle tick, and says how many in a row
    #[inline(always)]
    pub(crate) fn idled(&self) -> u32 {
        self.idle_ticks.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Forgets how long the thread has been idle
    #[inline(always)]
    pub(crate) fn busied(&self) {
        self.idle_ticks.store(0, Ordering::Relaxed);
    }

    /// Claims this slot so a thread can be started into it
    pub(crate) fn claim(&self) -> bool {
        self.state
            .compare_exchange(
                WorkerState::Empty as u32,
                WorkerState::Starting as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Puts a thread behind this slot
    ///
    /// ## Returns
    /// Whether the thread started. If it didn't, the slot is freed
    /// again
    pub(crate) fn start(&'static self) -> bool {
        if thread::Builder::new()
            .name(String::from("atap-sleep"))
            .spawn(move || self.run())
            .is_ok()
        {
            return true;
        }

        self.state
            .store(WorkerState::Empty as u32, Ordering::Release);

        false
    }

    /// Asks the thread to stop once it has put down whatever
    /// it is holding
    pub(crate) fn stop(&self) {
        if !self.state().alive() {
            return;
        }

        self.state
            .store(WorkerState::Stopping as u32, Ordering::Release);

        let _ = self.wake();
    }

    /// Wakes the thread if it is asleep
    ///
    /// The state leaves `Parked` before the wake goes out, so a
    /// thread about to sleep doesn't
    ///
    /// ## Returns
    /// Whether this caller took the thread out of its park. Only
    /// one caller can, per park
    #[inline(always)]
    pub(crate) fn wake(&self) -> bool {
        let claimed = self
            .state
            .compare_exchange(
                WorkerState::Parked as u32,
                WorkerState::Idle as u32,
                Ordering::SeqCst,
                Ordering::Relaxed,
            )
            .is_ok();

        address_lock::wake(address_lock::address(&self.state));

        claimed
    }

    /// Takes responsibility for clearing up after a dead thread
    ///
    /// ## Returns
    /// Whether this caller should do it. Only one caller ever gets
    /// `true` per death
    pub(crate) fn claim_recovery(&self) -> bool {
        self.state
            .compare_exchange(
                WorkerState::Dead as u32,
                WorkerState::Recovering as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Empties the slot, giving back the task it was running, if
    /// any
    pub(crate) fn recover(&self) -> usize {
        let stranded = self.current.swap(NO_TASK, Ordering::AcqRel);

        self.idle_ticks.store(0, Ordering::Relaxed);
        self.state
            .store(WorkerState::Empty as u32, Ordering::Release);

        stranded
    }

    /// The loop the thread follows
    ///
    /// The guard marks the slot dead if a panic unwinds through
    /// here
    fn run(&'static self) {
        let mut guard = Exit {
            thread: self,
            clean: false,
        };

        // Exchanged, so a stop that arrived before the thread was up
        // isn't lost
        let _ = self.state.compare_exchange(
            WorkerState::Starting as u32,
            WorkerState::Idle as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );

        loop {
            if self.state() == WorkerState::Stopping {
                break;
            }

            let Some(id) = POOL.blocking().pop() else {
                self.park();
                continue;
            };

            self.current.store(id, Ordering::Release);

            // Lost to a stop, so the task goes back to the queue
            if self
                .state
                .compare_exchange(
                    WorkerState::Idle as u32,
                    WorkerState::Running as u32,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .is_err()
            {
                self.current.store(NO_TASK, Ordering::Release);

                if !POOL.blocking().push(id) {
                    executor::fail(id);
                }

                break;
            }

            executor::run(id);

            self.current.store(NO_TASK, Ordering::Release);
            self.completed.fetch_add(1, Ordering::Relaxed);

            let _ = self.state.compare_exchange(
                WorkerState::Running as u32,
                WorkerState::Idle as u32,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }

        guard.clean = true;
    }

    /// Blocks until there is something to do or somebody says to
    /// stop
    ///
    /// Publishes that it is parking before its last look at the
    /// queue, and an offload queues before it checks for parked
    /// threads, so a task can't slip between the two
    fn park(&self) {
        POOL.sleep_parked_in();

        // Exchanged, so a stop that already spent its wake isn't
        // written over
        if self
            .state
            .compare_exchange(
                WorkerState::Idle as u32,
                WorkerState::Parked as u32,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            POOL.sleep_parked_out();

            return;
        }

        if !POOL.blocking().is_empty() {
            let _ = self.state.compare_exchange(
                WorkerState::Parked as u32,
                WorkerState::Idle as u32,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            POOL.sleep_parked_out();

            return;
        }

        let _ = address_lock::wait(
            address_lock::address(&self.state),
            WorkerState::Parked as u32,
        );

        POOL.sleep_parked_out();

        // Only back to idle if nothing else changed the state while
        // it slept
        let _ = self.state.compare_exchange(
            WorkerState::Parked as u32,
            WorkerState::Idle as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }
}

/// Marks the slot on the way out of the loop, including when a
/// panic unwinds through it
struct Exit {
    /// The thread being left
    thread: &'static SleepThread,

    /// Whether the loop broke rather than unwound
    clean: bool,
}

impl Drop for Exit {
    fn drop(&mut self) {
        if self.clean {
            self.thread.recover();
            POOL.sleep_left();

            return;
        }

        self.thread
            .state
            .store(WorkerState::Dead as u32, Ordering::Release);

        address_lock::wake(address_lock::address(&self.thread.state));
    }
}
