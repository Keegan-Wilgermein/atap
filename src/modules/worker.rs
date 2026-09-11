//! # Worker
//! One thread that takes tasks and runs them, and the ring of
//! task ids waiting for it
//!
//! The ring lives in the pool's static array rather than on the
//! thread's stack, so a worker that dies leaves its queue
//! behind to be recovered

use crate::{
    constants::{LOCAL_QUEUE, LOCAL_QUEUE_MASK, NO_TASK},
    executor,
    modules::{address_lock, worker_pool::POOL, worker_state::WorkerState},
};
use std::{
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
    thread,
};

/// One worker and everything it is holding
#[repr(C)]
pub(crate) struct Worker {
    /// Where the worker is, and the address it parks on
    state: AtomicU32,

    /// The oldest task in the ring
    ///
    /// Moved by the owner and by thieves, so taking needs a compare
    /// exchange
    head: AtomicU32,

    /// Where the next push lands
    ///
    /// Only ever moved by the owner
    tail: AtomicU32,

    /// The task being run right now, or `NO_TASK`
    current: AtomicUsize,

    /// Tasks finished since the worker started
    ///
    /// How the manager tells a busy worker from a stuck one
    completed: AtomicU64,

    /// What `completed` read when the manager last looked
    watched: AtomicU64,

    /// Manager ticks this worker has been idle for
    idle_ticks: AtomicU32,

    /// Task ids waiting on this worker, each as its id plus one
    ring: [AtomicU32; LOCAL_QUEUE],
}

impl Worker {
    /// A worker with no thread behind it
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU32::new(WorkerState::Empty as u32),
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            current: AtomicUsize::new(NO_TASK),
            completed: AtomicU64::new(0),
            watched: AtomicU64::new(0),
            idle_ticks: AtomicU32::new(0),
            ring: [const { AtomicU32::new(0) }; LOCAL_QUEUE],
        }
    }

    /// The current state
    #[inline(always)]
    pub(crate) fn state(&self) -> WorkerState {
        WorkerState::from_u32(self.state.load(Ordering::Acquire))
    }

    /// Whether the worker is on a task right now
    #[inline(always)]
    pub(crate) fn busy(&self) -> bool {
        self.state().busy()
    }

    /// Tasks waiting in this worker's ring
    #[inline(always)]
    pub(crate) fn backlog(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);

        tail.wrapping_sub(head) as usize
    }

    /// Tasks finished since this worker started
    #[inline(always)]
    pub(crate) fn completed(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    /// Whether this worker has finished anything since the manager
    /// last looked, recording that it has now looked
    #[inline(always)]
    pub(crate) fn moved(&self) -> bool {
        let completed = self.completed.load(Ordering::Relaxed);

        self.watched.swap(completed, Ordering::Relaxed) != completed
    }

    /// Notes another idle tick, and says how many in a row
    #[inline(always)]
    pub(crate) fn idled(&self) -> u32 {
        self.idle_ticks.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Forgets how long the worker has been idle
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
            .name(String::from("atap-worker"))
            .spawn(move || self.run())
            .is_ok()
        {
            return true;
        }

        self.state
            .store(WorkerState::Empty as u32, Ordering::Release);

        false
    }

    /// Asks the worker to stop between tasks
    ///
    /// A task already running finishes normally
    pub(crate) fn stop(&self) {
        if !self.state().alive() {
            return;
        }

        self.state
            .store(WorkerState::Stopping as u32, Ordering::Release);

        let _ = self.wake();
    }

    /// Wakes the worker if it is asleep
    ///
    /// The state leaves `Parked` before the wake goes out, so a
    /// worker about to sleep doesn't
    ///
    /// ## Returns
    /// Whether this caller took the worker out of its park. Only
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

    /// Queues a task on this worker
    ///
    /// ## Returns
    /// Whether it fit in the ring
    ///
    /// ## Safety
    /// Only the worker that owns the ring may push, since the tail
    /// is moved with a plain store
    pub(crate) fn push(&self, id: usize) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail.wrapping_sub(head) >= LOCAL_QUEUE as u32 {
            return false;
        }

        // Safe without an exchange: the fullness check means every
        // thief has already moved past this cell
        self.ring[(tail & LOCAL_QUEUE_MASK) as usize].store(id as u32 + 1, Ordering::Release);

        // Published last, so a thief that sees the tail sees the id
        self.tail.store(tail.wrapping_add(1), Ordering::Release);

        true
    }

    /// Takes the task that has been waiting longest
    ///
    /// Races thieves for the same end of the ring, so it wins its
    /// way out with a compare exchange
    pub(crate) fn pop(&self) -> Option<usize> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);

            if head == tail {
                return None;
            }

            let raw = self.ring[(head & LOCAL_QUEUE_MASK) as usize].load(Ordering::Acquire);

            // Can't happen, since the tail is published after its cell,
            // but a zero would turn into a wild id
            if raw == 0 {
                continue;
            }

            if self
                .head
                .compare_exchange_weak(
                    head,
                    head.wrapping_add(1),
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                return Some(raw as usize - 1);
            }
        }
    }

    /// Moves up to half this worker's backlog onto `thief`, oldest
    /// first
    ///
    /// ## Returns
    /// How many moved
    pub(crate) fn steal_into(&self, thief: &Worker) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        let available = tail.wrapping_sub(head);

        if available == 0 {
            return 0;
        }

        let take = (available / 2).max(1).min((LOCAL_QUEUE / 2) as u32);

        let mut ids = [0u32; LOCAL_QUEUE / 2];

        for offset in 0..take {
            let cell = head.wrapping_add(offset) & LOCAL_QUEUE_MASK;
            ids[offset as usize] = self.ring[cell as usize].load(Ordering::Acquire);
        }

        // These only count if the exchange lands, proving the victim
        // hadn't already moved past them
        if self
            .head
            .compare_exchange(
                head,
                head.wrapping_add(take),
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .is_err()
        {
            return 0;
        }

        let mut moved = 0;

        for raw in ids.iter().take(take as usize) {
            if *raw == 0 {
                continue;
            }

            let id = *raw as usize - 1;

            // The thief's ring filled up, so the rest go back to the
            // shared queue
            if !thief.push(id) && !POOL.injector().push(id) {
                executor::fail(id);
                continue;
            }

            moved += 1;
        }

        moved
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

    /// Everything this worker was holding when it went down
    ///
    /// ## Returns
    /// The tasks still queued, which can be run, and the one it
    /// was running, which can't
    pub(crate) fn recover(&self) -> (Vec<usize>, usize) {
        let stranded = self.current.swap(NO_TASK, Ordering::AcqRel);
        let mut queued = Vec::new();

        while let Some(id) = self.pop() {
            queued.push(id);
        }

        (queued, stranded)
    }

    /// Hands the slot back to be claimed again, once `recover` has
    /// emptied it
    #[inline(always)]
    pub(crate) fn release(&self) {
        self.head.store(0, Ordering::Release);
        self.tail.store(0, Ordering::Release);
        self.completed.store(0, Ordering::Relaxed);
        self.watched.store(0, Ordering::Relaxed);
        self.idle_ticks.store(0, Ordering::Relaxed);

        self.state
            .store(WorkerState::Empty as u32, Ordering::Release);
    }

    /// The loop the worker follows
    ///
    /// The guard marks the slot dead if a panic unwinds through
    /// here
    fn run(&'static self) {
        let mut guard = Exit {
            worker: self,
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

            let Some(id) = POOL.find_work(self) else {
                // Also clears up after dead peers, so the pool recovers
                // without the manager
                POOL.sweep_one();
                self.park();
                continue;
            };

            // Recorded so a worker that dies inside the task leaves a note
            // of which one
            //
            // Nothing between the pop and this store may unwind, or the id
            // would be lost with the thread
            self.current.store(id, Ordering::Release);

            // Lost to a stop, so the task goes back to the shared queue
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

                // Refused only when the task's slot has already gone
                if !POOL.injector().push(id) {
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
    /// queue, and a submission queues before it checks for parked
    /// workers, so a task can't slip between the two
    fn park(&self) {
        POOL.parked_in();

        // Exchanged, so a stop that already spent its wake isn't
        // written over. `SeqCst`, as the handshake above needs
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
            POOL.parked_out();
            return;
        }

        if !POOL.injector().is_empty() || self.backlog() > 0 {
            // Compared, so a stop that landed in this window survives too
            let _ = self.state.compare_exchange(
                WorkerState::Parked as u32,
                WorkerState::Idle as u32,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            POOL.parked_out();
            return;
        }

        let _ = address_lock::wait(
            address_lock::address(&self.state),
            WorkerState::Parked as u32,
        );

        POOL.parked_out();

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
    /// The worker being left
    worker: &'static Worker,

    /// Whether the loop broke rather than unwound
    clean: bool,
}

impl Drop for Exit {
    fn drop(&mut self) {
        if self.clean {
            self.worker.release();
            POOL.left();

            return;
        }

        self.worker
            .state
            .store(WorkerState::Dead as u32, Ordering::Release);

        address_lock::wake(address_lock::address(&self.worker.state));
    }
}
