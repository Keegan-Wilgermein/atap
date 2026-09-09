//! # Worker
//! One thread that takes tasks and runs them, and the ring of
//! task ids waiting for it
//!
//! The ring lives here, in the pool's static array, rather than
//! on the worker's own stack. That is the whole reason a panic
//! costs one task instead of a queue full of them: the thread
//! goes, the ring stays exactly where it was, and whoever
//! cleans up afterwards can still read every id in it
//!
//! Its counters are plain atomics that anything may read at any
//! time, including while the worker is part way through a task.
//! Asking a worker whether it is busy and how much it has
//! waiting costs a load apiece and never gets in its way

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
    /// Moved by the owner and by thieves alike, so both of
    /// them have to win a compare exchange to take anything
    head: AtomicU32,

    /// Where the next push lands
    ///
    /// Only ever moved by the owner, which is what makes the
    /// push side a plain store
    tail: AtomicU32,

    /// The task being run right now, or `NO_TASK`
    current: AtomicUsize,

    /// Tasks finished since the worker started
    ///
    /// The manager watches this rather than the state, because
    /// a worker that is `Running` every time it is looked at
    /// might be getting through thousands of tasks or stuck in
    /// one, and only a count can tell those apart
    completed: AtomicU64,

    /// What `completed` read when the manager last looked
    ///
    /// Only the manager touches this, so a worker pays nothing
    /// for being watched. The difference between it and
    /// `completed` is whether this worker finished anything in
    /// the last tick, which is what separates one that is
    /// working from one that is stuck
    watched: AtomicU64,

    /// Manager ticks this worker has been idle for
    ///
    /// Counted in ticks rather than kept as a timestamp
    /// because the manager is the only thing that reads or
    /// writes it, and it already knows how long a tick is
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
    ///
    /// One subtraction off two atomics, readable from any
    /// thread at any time, including while the worker is
    /// running something
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

    /// Whether this worker has finished anything since the
    /// manager last looked, and records that it has looked
    ///
    /// ## Returns
    /// `true` if the worker got through at least one task in
    /// the last tick. A worker that is running and hasn't is
    /// pinned by whatever it is on, and is contributing
    /// nothing to the queue behind it
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
    ///
    /// Claiming before spawning is what stops two threads
    /// starting a worker into the same slot
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
    /// rather than it being lost for the life of the process
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

    /// Asks the worker to stop once it has put down whatever
    /// it is holding
    ///
    /// Never interrupts a task. A worker checks between tasks,
    /// so the one it is on runs to the end and comes back to
    /// its listeners normally
    pub(crate) fn stop(&self) {
        if !self.state().alive() {
            return;
        }

        self.state
            .store(WorkerState::Stopping as u32, Ordering::Release);

        self.wake();
    }

    /// Wakes the worker if it is asleep
    ///
    /// ## Behaviour
    /// The state is moved off `Parked` before the wake goes
    /// out, and that is the part that matters. A worker only
    /// sleeps while its word still reads `Parked`, so a wake
    /// arriving in the moment between the worker deciding to
    /// sleep and actually sleeping would otherwise be sent to
    /// nobody and then slept straight through. Changing the
    /// word first means that worker doesn't sleep at all
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

    /// Queues a task on this worker
    ///
    /// ## Returns
    /// Whether it fit. A full ring is not an error, it is the
    /// signal to leave the task in the injector where anybody
    /// can reach it, which is better than holding work nobody
    /// else can see
    ///
    /// ## Safety
    /// Only the worker itself may push. The tail is moved with
    /// a plain store rather than an exchange, so a second
    /// pusher would be writing the same word at the same time.
    /// A thief pushing what it stole is still the owner of the
    /// ring it is pushing into, which is the only reason that
    /// path is allowed
    pub(crate) fn push(&self, id: usize) -> bool {
        let tail = self.tail.load(Ordering::Relaxed);
        let head = self.head.load(Ordering::Acquire);

        if tail.wrapping_sub(head) >= LOCAL_QUEUE as u32 {
            return false;
        }

        // The fullness check above is what makes this safe to
        // write without a compare exchange: it is loaded from
        // `head` with `Acquire`, so this cell is one every
        // thief has already moved past
        self.ring[(tail & LOCAL_QUEUE_MASK) as usize].store(id as u32 + 1, Ordering::Release);

        // Published last, so a thief that sees this tail also
        // sees the id behind it
        self.tail.store(tail.wrapping_add(1), Ordering::Release);

        true
    }

    /// Takes the task that has been waiting longest
    ///
    /// ## Behaviour
    /// The owner wins its way out with a compare exchange the
    /// same as a thief does, because a thief is racing it for
    /// the same end of the ring. Whichever of them lands the
    /// exchange is the one that owns the id, and the loser
    /// simply looks again
    pub(crate) fn pop(&self) -> Option<usize> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);

            if head == tail {
                return None;
            }

            let raw = self.ring[(head & LOCAL_QUEUE_MASK) as usize].load(Ordering::Acquire);

            // Can't happen: the tail is published after the
            // cell it covers. Looked at anyway, because taking
            // a zero for an id would be a wild pointer later
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

    /// Moves half this worker's backlog onto another
    ///
    /// ## Returns
    /// How many moved. Zero either because there was nothing
    /// worth taking or because another thief got there first,
    /// and the caller treats both the same way
    ///
    /// ## Behaviour
    /// Taken from the oldest end, so stealing serves the tasks
    /// that have waited longest rather than the ones that
    /// happened to arrive most recently
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

        // The ids above are only worth anything if this lands.
        // Winning the exchange is what proves the victim hadn't
        // already moved past them
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

            // A ring that filled up while this was in flight
            // sends the rest back where anybody can reach them,
            // rather than this thief sitting on work it can't
            // hold
            if !thief.push(id) {
                POOL.injector().push(id);
                continue;
            }

            moved += 1;
        }

        moved
    }

    /// Everything this worker was holding when it went down
    ///
    /// ## Returns
    /// The tasks that were still queued, which anybody can
    /// run, and the one it had already claimed, which nobody
    /// can — its `Task` was swapped out of the slot before it
    /// started and went down with the thread
    pub(crate) fn recover(&self) -> (Vec<usize>, usize) {
        let stranded = self.current.swap(NO_TASK, Ordering::AcqRel);
        let mut queued = Vec::new();

        while let Some(id) = self.pop() {
            queued.push(id);
        }

        (queued, stranded)
    }

    /// Hands the slot back to be claimed again
    ///
    /// Kept apart from `recover` because a slot is only ready
    /// to be claimed again once everything that was in it has
    /// been accounted for, and `recover` is what accounts for
    /// it
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
    /// The guard is what makes a panic recoverable. It runs on
    /// the way out either way, so a clean stop empties the slot
    /// and an unwind marks it dead for somebody else to clear
    fn run(&'static self) {
        let mut guard = Exit {
            worker: self,
            clean: false,
        };

        // Exchanged rather than stored, so a stop that arrived
        // before the thread was even up isn't thrown away
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
                // A dead peer is picked up here as well as by
                // the manager, so the pool still recovers
                // itself while the manager is down
                //
                // Checked on the way to a park rather than
                // between tasks. A worker with work to do has
                // better things to be doing, and the counter
                // behind this is shared, so touching it once
                // per task would put every worker on the same
                // cache line for no gain
                POOL.sweep_one();
                self.park();
                continue;
            };

            // Stamped before the task is touched and cleared
            // after it is done with, so a worker that dies
            // inside one leaves a note saying which
            //
            // #### Note
            // Nothing between the pop above and this store can
            // unwind. There is no allocation in it and none of
            // the task's own code has run yet, which is what
            // stops an id existing only in a register at the
            // moment a panic goes past. It has to stay that way
            self.current.store(id, Ordering::Release);

            // Every move through `Running` is an exchange and
            // not a store, because a stop can land at any point
            // and a store would write it back out again. Losing
            // this one means somebody asked for a stop while
            // this was looking for work, so the task goes back
            // where anybody can reach it rather than down with
            // a worker on its way out
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
                POOL.injector().push(id);

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
    /// ## Behaviour
    /// The state is published before the queues are looked at
    /// again, and a submission queues before it wakes anybody,
    /// so a task arriving in the gap either finds this worker
    /// already marked parked and wakes it, or is found by the
    /// re-check before it sleeps. The same store then load in
    /// both directions `TaskData::enter_read` relies on
    fn park(&self) {
        // Announced first, then the state, then the re-check.
        // A submission does the opposite: it queues, then reads
        // the count. Both sides write one word and read the
        // other under sequential consistency, so it is not
        // possible for the worker to miss the task and the
        // submission to miss the worker
        POOL.parked_in();

        self.state
            .store(WorkerState::Parked as u32, Ordering::SeqCst);

        if !POOL.injector().is_empty() || self.backlog() > 0 {
            self.state.store(WorkerState::Idle as u32, Ordering::SeqCst);
            POOL.parked_out();
            return;
        }

        let _ = address_lock::wait(
            address_lock::address(&self.state),
            WorkerState::Parked as u32,
        );

        POOL.parked_out();

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
/// would through any other frame, so a worker that dies still
/// says so and still gets cleaned up
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
