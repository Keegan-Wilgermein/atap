//! # Worker
//! One thread that takes tasks and runs them, and the ring of
//! task ids waiting for it

use crate::{
    constants::{HELP_DEPTH, LOCAL_QUEUE, LOCAL_QUEUE_MASK, NO_TASK, WORKER_STACK},
    executor,
    modules::{
        exit_guard::ExitGuard,
        faults, help,
        task_data::QUEUED_LOCAL,
        thread_slot::{PoolThread, ThreadSlot},
        worker_pool::POOL,
        worker_state::WorkerState,
    },
};
use std::{
    mem,
    sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
};

/// One worker and everything it is holding
#[repr(C)]
pub(crate) struct Worker {
    /// Where the thread is, the task it is holding, and how long it
    /// has been idle
    slot: ThreadSlot,

    /// The oldest task in the ring
    ///
    /// Moved by the owner and by thieves, so taking needs a compare
    /// exchange
    head: AtomicU32,

    /// Where the next push lands
    ///
    /// Only ever moved by the owner
    tail: AtomicU32,

    /// Tasks finished since the worker started
    ///
    /// How the manager tells a busy worker from a stuck one
    completed: AtomicU64,

    /// What `completed` read when the manager last looked
    watched: AtomicU64,

    /// The first task the running task spawned that nobody has taken
    /// yet, as its id plus one, taken before anything in the ring
    ///
    /// Taken with a swap, by the worker or a thief, so it only ever runs
    /// once
    lifo: AtomicU32,

    /// Tasks taken from `lifo` in a row
    streak: AtomicU32,

    /// The kernel's port for the thread, or zero with no thread
    port: AtomicU32,

    /// Waits on other tasks this worker is inside right now
    ///
    /// A worker in one looks for work to help with between short
    /// sleeps
    waits: AtomicU32,

    /// The tasks run while helping, each as its id plus one, by how
    /// deep it was run
    ///
    /// So a worker that dies part way through helping leaves a note of
    /// all of them
    nested: [AtomicUsize; HELP_DEPTH],

    /// Task ids waiting on this worker, each as its id plus one
    ring: [AtomicU32; LOCAL_QUEUE],
}

impl Worker {
    /// A worker with no thread behind it
    pub(crate) const fn new() -> Self {
        Self {
            slot: ThreadSlot::new(),
            head: AtomicU32::new(0),
            tail: AtomicU32::new(0),
            completed: AtomicU64::new(0),
            watched: AtomicU64::new(0),
            lifo: AtomicU32::new(0),
            streak: AtomicU32::new(0),
            port: AtomicU32::new(0),
            waits: AtomicU32::new(0),
            nested: [const { AtomicUsize::new(0) }; HELP_DEPTH],
            ring: [const { AtomicU32::new(0) }; LOCAL_QUEUE],
        }
    }

    /// Tasks waiting in this worker's ring and its LIFO slot
    #[inline(always)]
    pub(crate) fn backlog(&self) -> usize {
        let tail = self.tail.load(Ordering::Acquire);
        let head = self.head.load(Ordering::Acquire);

        tail.wrapping_sub(head) as usize + (self.lifo.load(Ordering::SeqCst) != 0) as usize
    }

    /// Whether the ring has room for another task
    ///
    /// Only the owner adds to the ring, so on the owner's thread a yes
    /// holds until it pushes
    #[inline(always)]
    pub(crate) fn has_room(&self) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        tail.wrapping_sub(head) < LOCAL_QUEUE as u32
    }

    /// Puts a task in the LIFO slot, if the slot is empty
    ///
    /// ## Returns
    /// Whether it went in
    #[inline(always)]
    pub(crate) fn put_lifo(&self, id: usize) -> bool {
        self.lifo
            .compare_exchange(0, id as u32 + 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Empties the LIFO slot
    ///
    /// Safe from any thread, since the swap hands it to one taker only
    ///
    /// ## Returns
    /// What the slot held, and whether this caller claimed its task. A
    /// task already run by a worker waiting on it isn't claimed
    #[inline(always)]
    pub(crate) fn take_lifo(&self) -> Option<(usize, bool)> {
        match self.lifo.swap(0, Ordering::SeqCst) {
            0 => None,
            raw => {
                let id = raw as usize - 1;
                let claimed =
                    executor::slot(id).is_some_and(|data| data.claim_queued(QUEUED_LOCAL));

                Some((id, claimed))
            }
        }
    }

    /// Tasks taken from the LIFO slot in a row
    #[inline(always)]
    pub(crate) fn streak(&self) -> u32 {
        self.streak.load(Ordering::Relaxed)
    }

    /// Counts a task taken from the LIFO slot, or starts the count again
    /// for one taken from anywhere else
    #[inline(always)]
    pub(crate) fn took(&self, from_lifo: bool) {
        match from_lifo {
            true => self.streak.fetch_add(1, Ordering::Relaxed),
            false => self.streak.swap(0, Ordering::Relaxed),
        };
    }

    /// Notes that a task this worker runs has started waiting on another
    #[inline(always)]
    pub(crate) fn wait_began(&self) {
        self.waits.fetch_add(1, Ordering::Relaxed);
    }

    /// Notes that a wait on another task is over
    #[inline(always)]
    pub(crate) fn wait_ended(&self) {
        self.waits.fetch_sub(1, Ordering::Relaxed);
    }

    /// Whether this worker is blocked in a call rather than waiting on
    /// another task, which it helps with work while it does
    #[inline(always)]
    pub(crate) fn blocked_in_a_call(&self) -> bool {
        self.waits.load(Ordering::Relaxed) == 0 && self.waiting_in_kernel()
    }

    /// Whether this worker's thread is waiting in the kernel rather
    /// than running, at this moment
    ///
    /// A worker stuck like that isn't using a core
    pub(crate) fn waiting_in_kernel(&self) -> bool {
        let port = self.port.load(Ordering::Relaxed);

        if port == 0 {
            return false;
        }

        let mut info: libc::thread_basic_info = unsafe { mem::zeroed() };
        let mut count = libc::THREAD_BASIC_INFO_COUNT;

        let read = unsafe {
            libc::thread_info(
                port,
                libc::THREAD_BASIC_INFO as _,
                (&mut info as *mut libc::thread_basic_info).cast(),
                &mut count,
            )
        };

        read == libc::KERN_SUCCESS && info.run_state == libc::TH_STATE_WAITING
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

    /// Puts a thread behind this slot
    ///
    /// ## Returns
    /// Whether the thread started. If it didn't, the slot is freed
    /// again
    pub(crate) fn start(&'static self) -> bool {
        self.slot
            .start("atap-worker", Some(WORKER_STACK), self, Self::run)
    }

    /// Records a task this worker is about to run while helping, `depth`
    /// runs down
    #[inline(always)]
    pub(crate) fn hold_nested(&self, depth: usize, id: usize) {
        self.nested[depth].store(id + 1, Ordering::Release);
    }

    /// Records that the task run `depth` down is over
    ///
    /// Counted as finished work, so the manager doesn't take a worker
    /// that is helping for one that is stuck
    #[inline(always)]
    pub(crate) fn put_down_nested(&self, depth: usize) {
        self.nested[depth].store(0, Ordering::Release);
        self.completed.fetch_add(1, Ordering::Relaxed);
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
                .is_err()
            {
                continue;
            }

            let id = raw as usize - 1;

            // Stepped over if a worker waiting on it already ran it
            if executor::slot(id).is_some_and(|data| data.claim_queued(QUEUED_LOCAL)) {
                return Some(id);
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

            // Still local, so the entry moves as it is
            if thief.push(id) {
                moved += 1;
                continue;
            }

            // The thief's ring filled up, so the rest go back to the shared
            // queue, unless a worker waiting on one already ran it
            if !executor::slot(id).is_some_and(|data| data.claim_queued(QUEUED_LOCAL)) {
                continue;
            }

            if !POOL.injector().push(id) {
                executor::fail(id);
                continue;
            }

            moved += 1;
        }

        moved
    }

    /// Everything this worker was holding when it went down
    ///
    /// ## Returns
    /// The tasks still queued, which can be run, and the ones it was
    /// running, which can't: the task its loop held, and every task it
    /// was helping with inside that one
    pub(crate) fn recover(&self) -> (Vec<usize>, Vec<usize>) {
        let mut stranded = Vec::new();

        let held = self.slot.take_held();

        if held != NO_TASK {
            stranded.push(held);
        }

        for nested in &self.nested {
            match nested.swap(0, Ordering::AcqRel) {
                0 => {}
                held => stranded.push(held - 1),
            }
        }

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
        self.streak.store(0, Ordering::Relaxed);
        self.port.store(0, Ordering::Relaxed);

        self.slot.busied();
        self.slot.empty();
    }

    /// The loop the worker follows
    ///
    /// The guard marks the slot dead if a panic unwinds through
    /// here
    fn run(&'static self) {
        let mut guard = ExitGuard::new(self);

        self.slot.started();

        // So the manager can ask whether the thread is blocked
        self.port.store(
            unsafe { libc::pthread_mach_thread_np(libc::pthread_self()) },
            Ordering::Relaxed,
        );

        // So a task this worker runs can help while it waits
        help::enter(self);

        loop {
            if self.slot.state() == WorkerState::Stopping {
                break;
            }

            faults::worker_dies();

            let Some(id) = POOL.find_work(self) else {
                // Also clears up after dead peers, so the pool recovers
                // without the manager
                POOL.sweep_one();
                self.park();
                continue;
            };

            // Recorded so a worker that dies inside the task leaves a note
            // of which one
            self.slot.hold(id);

            // Lost to a stop, so the task goes back to the shared queue
            if !self.slot.begin_task() {
                self.slot.put_down();

                // Refused only when the task's slot has already gone
                if !POOL.injector().push(id) {
                    executor::fail(id);
                }

                break;
            }

            faults::worker_dies();

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
    /// A submission queues before it checks for parked workers, so a
    /// task can't slip between the park and the last look
    fn park(&self) {
        self.slot.park(
            || POOL.parked_in(),
            || POOL.parked_out(),
            || !POOL.injector().is_empty() || self.backlog() > 0 || POOL.lifo_waiting(),
        );
    }
}

impl PoolThread for Worker {
    #[inline(always)]
    fn slot(&self) -> &ThreadSlot {
        &self.slot
    }

    /// Empties the slot and gives the worker's place back
    fn left(&'static self) {
        // Tasks spawned into the slot or the ring just before a stop, which
        // nobody else would ever come for
        let leftover = POOL
            .take_lifo(self)
            .into_iter()
            .chain(std::iter::from_fn(|| self.pop()));

        for id in leftover {
            if !POOL.injector().push(id) {
                executor::fail(id);
            }
        }

        self.release();
        POOL.left();
    }
}
