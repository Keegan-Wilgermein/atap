//! # Thread Slot
//! What a worker and a sleep thread both keep about the thread
//! behind them, and the moves both make on it

use crate::{
    constants::NO_TASK,
    modules::{address_lock, faults, worker_state::WorkerState},
};
use std::{
    sync::atomic::{AtomicU32, AtomicUsize, Ordering},
    thread,
};

/// A kind of thread the pool runs behind a slot
pub(crate) trait PoolThread: Sync + 'static {
    /// The part of the slot every kind of thread keeps
    fn slot(&self) -> &ThreadSlot;

    /// Hands the slot back once the thread's loop has broken on
    /// its own
    fn left(&'static self);
}

/// Where one thread is, what it is holding, and how long it has
/// been idle
#[repr(C)]
pub(crate) struct ThreadSlot {
    /// Where the thread is, and the address it parks on
    state: AtomicU32,

    /// The task being run right now, as its id plus one, or zero for
    /// none
    ///
    /// Kept here so a thread that dies leaves a note of which task
    /// went with it
    current: AtomicUsize,

    /// Manager ticks this thread has been idle for
    idle_ticks: AtomicU32,
}

impl ThreadSlot {
    /// A slot with no thread behind it yet
    pub(crate) const fn new() -> Self {
        Self {
            state: AtomicU32::new(WorkerState::Empty as u32),
            current: AtomicUsize::new(0),
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
    pub(crate) fn alive(&self) -> bool {
        self.state().alive()
    }

    /// Whether the thread is inside a task right now
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

    /// Puts a thread behind this slot, running `run` on `owner`
    ///
    /// `stack` reserves a stack of that many bytes, or the default
    ///
    /// ## Returns
    /// Whether the thread started. If it didn't, the slot is freed
    /// again
    pub(crate) fn start<O: Sync>(
        &self,
        name: &str,
        stack: Option<usize>,
        owner: &'static O,
        run: fn(&'static O),
    ) -> bool {
        // A refusal a test asked for looks the same as the kernel's own
        if !faults::spawn_refused() {
            let mut builder = thread::Builder::new().name(String::from(name));

            if let Some(stack) = stack {
                builder = builder.stack_size(stack);
            }

            if builder.spawn(move || run(owner)).is_ok() {
                return true;
            }
        }

        self.empty();

        false
    }

    /// Moves a thread that has just come up from starting to idle
    ///
    /// Exchanged, so a stop that arrived before the thread was up
    /// isn't lost
    #[inline(always)]
    pub(crate) fn started(&self) {
        let _ = self.state.compare_exchange(
            WorkerState::Starting as u32,
            WorkerState::Idle as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Asks the thread to stop once it has put down whatever it is
    /// holding
    ///
    /// A task already running finishes normally
    pub(crate) fn stop(&self) {
        if !self.alive() {
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

    /// Records the task the thread is about to run
    ///
    /// Nothing between taking the task and this store may unwind,
    /// or the id would be lost with the thread
    #[inline(always)]
    pub(crate) fn hold(&self, id: usize) {
        self.current.store(id + 1, Ordering::Release);
    }

    /// Records that the thread is no longer holding a task
    #[inline(always)]
    pub(crate) fn put_down(&self) {
        self.current.store(0, Ordering::Release);
    }

    /// Takes the note of which task the thread was holding
    ///
    /// ## Returns
    /// The task's id, or `NO_TASK`
    #[inline(always)]
    pub(crate) fn take_held(&self) -> usize {
        match self.current.swap(0, Ordering::AcqRel) {
            0 => NO_TASK,
            held => held - 1,
        }
    }

    /// Moves from idle into running a task
    ///
    /// ## Returns
    /// Whether it did. `false` means a stop got there first
    #[inline(always)]
    pub(crate) fn begin_task(&self) -> bool {
        self.state
            .compare_exchange(
                WorkerState::Idle as u32,
                WorkerState::Running as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    /// Moves back to idle once a task is done, unless something
    /// else changed the state meanwhile
    #[inline(always)]
    pub(crate) fn end_task(&self) {
        let _ = self.state.compare_exchange(
            WorkerState::Running as u32,
            WorkerState::Idle as u32,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Leaves the slot empty, ready to be claimed again
    #[inline(always)]
    pub(crate) fn empty(&self) {
        self.state
            .store(WorkerState::Empty as u32, Ordering::Release);
    }

    /// Marks the slot dead and wakes anything waiting on it
    pub(crate) fn mark_dead(&self) {
        self.state
            .store(WorkerState::Dead as u32, Ordering::Release);

        address_lock::wake(address_lock::address(&self.state));
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

    /// Blocks until there is something to do or somebody says to
    /// stop
    ///
    /// Publishes that it is parking before its last look for work,
    /// and whatever queues work checks for parked threads after it
    /// has queued, so work can't slip between the two
    pub(crate) fn park(
        &self,
        parked_in: impl Fn(),
        parked_out: impl Fn(),
        has_work: impl FnOnce() -> bool,
    ) {
        parked_in();

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
            parked_out();
            return;
        }

        if has_work() {
            // Compared, so a stop that landed in this window survives too
            let _ = self.state.compare_exchange(
                WorkerState::Parked as u32,
                WorkerState::Idle as u32,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );

            parked_out();
            return;
        }

        let _ = address_lock::wait(
            address_lock::address(&self.state),
            WorkerState::Parked as u32,
        );

        parked_out();

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
