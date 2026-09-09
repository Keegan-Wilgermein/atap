//! # Sleep Thread
//! The threads that are allowed to sit inside a `kevent` call
//! for a whole second, so that no worker ever has to
//!
//! A `Task` runs to completion on the thread that starts it.
//! `execute` is an ordinary function call, so a native stack
//! can't be put down half way through one and picked up
//! elsewhere — which means the only way a worker avoids being
//! held by a long sleep is to not start it. It hands the whole
//! task over instead, and goes back to the queue
//!
//! #### Note
//! These pull from one shared queue rather than being owned by
//! a worker each. That distinction is the whole design. A
//! worker that owned its own sleep thread would put every
//! blocking task it happened to pop onto that one thread, and
//! since the first worker awake drains the queue before the
//! others have woken, one thread would end up running the lot
//! one after another. A shared queue means the next free
//! thread takes the next task, whichever worker found it

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
    /// Lives here rather than on the thread's stack, so that a
    /// thread going down leaves a note saying which task went
    /// with it
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
    /// Whether the thread started. A slot whose thread didn't
    /// is put straight back, so the next attempt can use it
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

        self.wake();
    }

    /// Wakes the thread if it is asleep
    ///
    /// The state is moved off `Parked` before the wake goes
    /// out, so a thread that had decided to sleep but hadn't
    /// yet finds its word already changed and doesn't sleep at
    /// all. Without that the wake lands on nobody and is then
    /// slept straight through
    #[inline(always)]
    pub(crate) fn wake(&self) {
        let _ = self.state.compare_exchange(
            WorkerState::Parked as u32,
            WorkerState::Idle as u32,
            Ordering::SeqCst,
            Ordering::Relaxed,
        );

        address_lock::wake(address_lock::address(&self.state));
    }

    /// The task this thread went down holding, if any
    ///
    /// There is no queue to give back any more. Everything
    /// this thread hadn't started is still in the shared queue
    /// where anybody can reach it, which is the point of the
    /// queue being shared
    pub(crate) fn recover(&self) -> usize {
        let stranded = self.current.swap(NO_TASK, Ordering::AcqRel);

        self.idle_ticks.store(0, Ordering::Relaxed);
        self.state
            .store(WorkerState::Empty as u32, Ordering::Release);

        stranded
    }

    /// The loop the thread follows
    ///
    /// The guard is what makes a panic recoverable. It runs on
    /// the way out either way, so a clean stop empties the slot
    /// and an unwind marks it dead for somebody else to clear
    fn run(&'static self) {
        let mut guard = Exit {
            thread: self,
            clean: false,
        };

        // Exchanged rather than stored, so a stop that arrived
        // before the thread was up isn't thrown away
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

            // Stamped before the task is claimed and cleared
            // after it is finished, so a thread that dies
            // inside one leaves a note saying which
            self.current.store(id, Ordering::Release);

            // Exchanged and not stored, for the same reason a
            // worker's is. Losing it means a stop landed while
            // this was reaching for the task, so the task goes
            // back to the shared queue rather than down with a
            // thread that is leaving
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
                POOL.blocking().push(id);

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

    /// Blocks until there is something to do or somebody says
    /// to stop
    ///
    /// The same store then load in both directions a worker
    /// parks with: this publishes that it is parking before it
    /// looks at the queue for the last time, and an offload
    /// queues before it reads the count of parked threads
    fn park(&self) {
        POOL.sleep_parked_in();

        self.state
            .store(WorkerState::Parked as u32, Ordering::SeqCst);

        if !POOL.blocking().is_empty() {
            self.state.store(WorkerState::Idle as u32, Ordering::SeqCst);
            POOL.sleep_parked_out();

            return;
        }

        let _ = address_lock::wait(
            address_lock::address(&self.state),
            WorkerState::Parked as u32,
        );

        POOL.sleep_parked_out();

        // Only back to idle if nothing asked for something
        // else while this was asleep
        let _ = self.state.compare_exchange(
            WorkerState::Parked as u32,
            WorkerState::Idle as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }
}

/// Marks the slot on the way out of the loop
///
/// A drop guard rather than a line at the end of `run`,
/// because the whole point is to run when `run` doesn't get to
/// its end. Rust unwinds a panic through this the same way it
/// would through any other frame
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
