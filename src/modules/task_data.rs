//! # Task Data
//! The memory a single spawned task owns, from the task
//! waiting to be run through to the output waiting to be read
//!
//! Slots are carved out of the table's blocks rather than
//! given a mapping each. A block is mapped once, never moves
//! and is never given back, so a slot's address is as fixed
//! as a private mapping's would be at a fraction of the cost.
//! The header sits at the base of the slot and the output
//! lives at a fixed offset past it, which is what lets the
//! output type stay erased
//!
//! Every handle on a task reaches the same slot through the
//! same id, so duplicating a handle has never duplicated any
//! of this. The sharing is the id, not the allocator
//!
//! #### Note
//! Nothing outside the `Executor` may call any of this. The
//! `Executor` is the only thing that knows when a slot is
//! still alive, so it is the only thing allowed to touch one

use crate::{
    constants::{
        CANCELLING, INLINE_PAYLOAD, NO_SELECT, NOT_WAITING, PAYLOAD_OFFSET, PRIORITY_BAND_SHIFT,
        PRIORITY_CLASS_SHIFT, PRIORITY_SEQUENCE_MASK,
    },
    modules::{
        erased_task::ErasedTask, mapping, series::SeriesTask, task_kind::TaskKind,
        task_setup::TaskSetup, task_state::TaskState,
    },
};
use libc::c_void;
use std::{
    mem, ptr,
    sync::atomic::{AtomicBool, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, Ordering},
    thread,
    time::{Duration, Instant},
};

/// Everything one spawned task owns
///
/// Laid out `repr(C)` because the payload is found by adding
/// a fixed offset to the base of the header, and that only
/// holds if the header can't be reordered out from under it
///
/// The whole thing is sized to fit inside `PAYLOAD_OFFSET`,
/// which the assert in `init` checks. Adding a field without
/// taking one away will fail that assert at compile time
/// rather than quietly running the header into the output
///
/// #### Note
/// The counts are all `u32` rather than `usize`, which is what
/// makes room for the priority and the queue link without the
/// slot growing. None of them is anywhere near a ceiling: four
/// billion listeners on one task, or four billion live tasks,
/// are both well past what the table itself can address
#[repr(C)]
pub(crate) struct TaskData {
    /// Where the task is in its life
    ///
    /// Also the address every listener blocks on, which is
    /// why it is a `u32` and why it is first
    state: AtomicU32,

    /// Live `TaskHandle`s, plus one for the `Executor`
    /// until it has finished with the task
    listeners: AtomicU32,

    /// Listeners part way through cloning the output
    ///
    /// A clone reads the payload where it lies, so moving the
    /// output out has to wait for any clone already under way
    /// to finish rather than pulling it out from under one
    readers: AtomicU32,

    /// `size_of` the output type, checked before any read
    size: u32,

    /// The next slot on the free list, as its id plus one so
    /// that zero can mean the end of it
    ///
    /// Only meaningful while this slot is `Free`. Nothing can
    /// be reading it then, because a slot only goes on the
    /// list once its last listener has gone
    next: AtomicU32,

    /// The next task in whichever run queue holds this one,
    /// as its id plus one
    ///
    /// Separate from `next` rather than sharing it, because a
    /// queued task is very much alive and a slot on the free
    /// list is very much not. Sharing one word would work only
    /// for as long as nobody made those two states overlap
    ///
    /// A task is in at most one queue at a time, so one link
    /// covers the injector, a worker's spill and a sleep
    /// thread's hand off alike
    queue_next: AtomicU32,

    /// Whether the payload currently holds a value
    ///
    /// Tracked apart from the state so that cancelling a
    /// finished task doesn't have to drop the output out
    /// from under a listener that is reading it
    filled: AtomicBool,

    /// What happens when a run of this finishes
    ///
    /// Read once, at the end of a run. Everything up to that
    /// moment is the same whether a task runs once or forever,
    /// which is why they share a slot and a handle at all
    kind: AtomicU8,

    /// Whether this task wants a thread it can block
    ///
    /// Asked of the task itself at spawn, where the concrete
    /// type is still in hand, and kept because a re-arm has
    /// nothing left to ask. A stored answer rather than a
    /// question put to the task again, so the worker's path is
    /// still a load rather than a walk through the erasure
    blocking: AtomicBool,

    /// Whether the `Executor` still holds its reference
    ///
    /// Taken at creation and given back exactly once, by
    /// whichever part of the runtime finishes with the task —
    /// the run that ends it, the tick that finds its series
    /// cancelled, the sweep after a worker died, or the
    /// teardown that writes off what is left. Which of those
    /// gets there is not knowable in advance, and more than one
    /// of them can have a fair claim to be last
    ///
    /// So the reference is a thing to be won rather than a
    /// convention to be kept. Whoever takes this flag releases;
    /// everybody else has already been beaten to it and does
    /// nothing. Without it, correctness rests on every path
    /// releasing exactly once, and a second release takes the
    /// listener count below zero and frees a live slot
    ///
    /// Sits in the padding that already followed `blocking`, so
    /// it costs the header nothing
    held: AtomicBool,

    /// Whether a wake for this task is out on the manager's
    /// queue, waiting to put it back on the pool
    ///
    /// Set when the timer is armed and taken by whoever acts on
    /// it, so however many wakes arrive for one wait — the
    /// original, or one a restarted manager put back after
    /// losing it — exactly one of them queues the task. Queuing
    /// twice would put one task in a linked queue in two places
    /// at once, which is a far worse thing than the lost wake
    /// this exists to recover
    ///
    /// Sits in the padding in front of `interval`, so it costs
    /// the header nothing
    armed: AtomicBool,

    /// Runs still allowed, or `u32::MAX` for no limit
    ///
    /// Counted down as runs finish, so it must be atomic — it
    /// is the one bound field that changes after the slot is
    /// published
    runs_left: AtomicU32,

    /// The moment this stops repeating, if it does
    ///
    /// Plain rather than atomic, and safe to be: written in
    /// `init` before the `Release` store that publishes the
    /// slot, and never written again — which is exactly what
    /// `size` and `drop_glue` above already rely on
    ///
    /// An `Instant` rather than packed nanoseconds because
    /// `until` is handed one by the caller and an `Instant` is
    /// opaque, with no sound conversion to a raw clock value
    until: Option<Instant>,

    /// Nanoseconds to wait before the first run, or zero
    ///
    /// **Cleared the moment the first run begins**, which is
    /// what makes it answerable later. A manager coming back
    /// from a restart has to know whether the wait it is
    /// re-arming is a delay before the start or a gap between
    /// runs, and the two are different durations — this being
    /// zero is how it tells them apart
    start_delay: AtomicU64,

    /// Nanoseconds a `RepeatEvery` task waits between runs
    ///
    /// Zero for everything else, which never reads it. Kept in
    /// the slot rather than in the task, because the wait is
    /// arranged by the `Executor` after the run has finished
    /// and the task itself is not consulted about it
    interval: AtomicU64,

    /// The kqueue this task is sitting in a wait on, or
    /// `NOT_WAITING`
    ///
    /// Recorded so a cancel can reach into the wait and end it
    /// rather than leaving a thread inside the kernel until a
    /// timer nobody is interested in any more goes off
    ///
    /// Slots into the padding that already followed `filled`,
    /// so the header is the same 64 bytes it was
    waiting: AtomicI32,

    /// The kqueue a `join_first` wants poking when this settles
    ///
    /// `NO_SELECT` when nobody is selecting on it, which is
    /// every slot almost all of the time
    ///
    /// #### Note
    /// Sits beside `waiting` on purpose. The header has very
    /// little room left before `PAYLOAD_OFFSET`, and an `i32`
    /// here lands in the padding that field already leaves
    /// rather than costing four bytes of its own
    ///
    /// One at a time. A second `join_first` over a task that is
    /// already in somebody's set doesn't register, and finds
    /// its answer on the next look round instead — which is why
    /// nothing depends on the notification arriving
    select: AtomicI32,

    /// The class this task was spawned at and the order it
    /// was spawned in, packed into one word
    ///
    /// Read through the accessors below and nowhere else, so
    /// that the packing can change without anything outside
    /// this file having to know it did
    priority: AtomicU64,

    /// The erased task, null once claimed
    ///
    /// A `Box<dyn ErasedTask>` is two words wide and can't
    /// sit in an atomic, so what is stored is a thin pointer
    /// to that box
    task: AtomicPtr<c_void>,

    /// Drops a payload of the output type in place
    drop_glue: unsafe fn(*mut u8),

    /// An output too big to sit beside the header, or null
    /// when it fits inline like every realistic one does
    payload: AtomicPtr<u8>,

    /// The task a `Series` clones each of its runs from, or
    /// null for everything else
    ///
    /// Kept apart from `task` rather than sharing it, because
    /// the two are different trait objects and a slot has to
    /// know which one it is holding to drop it. A `Series`
    /// slot's `task` is null for exactly that reason, which
    /// also happens to be what stops one ever being run: a
    /// claim that comes back empty is already the path a task
    /// somebody else took goes down
    prototype: AtomicPtr<c_void>,
}

/// The header has to fit in front of the payload
///
/// Checked out here as well as inside `init` so that it is
/// checked at all times rather than only when something
/// happens to instantiate a task, since running the header
/// into the output would be silent corruption
const _: () = assert!(mem::size_of::<TaskData>() <= PAYLOAD_OFFSET);

impl TaskData {
    /// Fills in an empty slot ready for a task
    ///
    /// ## Returns
    /// Whether the slot is ready. The only way this fails is
    /// an output too big to sit inline and a kernel unwilling
    /// to map one of its own
    ///
    /// ## Safety
    /// The id must have come from `TaskTable::alloc`, so that
    /// nothing else is looking at the slot while it is
    /// written. The state is published last, which is what
    /// makes the rest of the header visible to anything that
    /// finds the task afterwards
    pub(crate) unsafe fn init<T>(
        data: *mut Self,
        task: *mut c_void,
        state: TaskState,
        setup: TaskSetup,
        sequence: u64,
    ) -> bool {
        // Both fold away at compile time, and both are silent
        // memory corruption if they ever stop holding
        const { assert!(mem::size_of::<Self>() <= PAYLOAD_OFFSET) };
        assert!(mem::align_of::<T>() <= PAYLOAD_OFFSET);

        let size = mem::size_of::<T>();
        let oversized = size > INLINE_PAYLOAD;

        let payload = match oversized {
            true => mapping::alloc(size),
            false => ptr::null_mut(),
        };

        if oversized && payload.is_null() {
            return false;
        }

        unsafe {
            data.write(Self {
                state: AtomicU32::new(TaskState::Free as u32),
                listeners: AtomicU32::new(2),
                readers: AtomicU32::new(0),
                size: size as u32,
                next: AtomicU32::new(0),
                queue_next: AtomicU32::new(0),
                filled: AtomicBool::new(false),
                kind: AtomicU8::new(setup.kind as u8),
                blocking: AtomicBool::new(setup.blocking),
                held: AtomicBool::new(true),
                armed: AtomicBool::new(false),
                runs_left: AtomicU32::new(setup.runs),
                until: setup.until(),
                start_delay: AtomicU64::new(setup.start_delay.as_nanos() as u64),
                interval: AtomicU64::new(setup.interval.as_nanos() as u64),
                waiting: AtomicI32::new(NOT_WAITING),
                select: AtomicI32::new(NO_SELECT),
                priority: AtomicU64::new(0),
                task: AtomicPtr::new(task),
                drop_glue: glue::<T>,
                payload: AtomicPtr::new(payload),
                prototype: AtomicPtr::new(ptr::null_mut()),
            })
        };

        // Through the accessor rather than packed inline, so
        // that the layout is written down in exactly one place
        unsafe { (*data).set_priority(setup.priority, sequence) };

        // Published last, so that anything finding the task
        // in a live state also sees the whole header behind it
        unsafe { (*data).state.store(state as u32, Ordering::Release) };

        true
    }

    /// Where the output lives, filled or not
    #[inline(always)]
    pub(crate) fn payload(&self) -> *mut u8 {
        let oversized = self.payload.load(Ordering::Acquire);

        if !oversized.is_null() {
            return oversized;
        }

        unsafe { (self as *const Self as *mut u8).add(PAYLOAD_OFFSET) }
    }

    /// `size_of` the output type this slot was built for
    #[inline(always)]
    pub(crate) fn size(&self) -> usize {
        self.size as usize
    }

    /// The current state
    #[inline(always)]
    pub(crate) fn state(&self) -> TaskState {
        TaskState::from_u32(self.state.load(Ordering::Acquire))
    }

    /// Moves the state on, publishing everything
    /// written before it
    #[inline(always)]
    pub(crate) fn set_state(&self, state: TaskState) {
        self.state.store(state as u32, Ordering::Release);
    }

    /// Moves the state on, but only from `from`
    ///
    /// Every transition that two threads could race for goes
    /// through here, so exactly one of them wins it
    #[inline(always)]
    pub(crate) fn try_state(&self, from: TaskState, to: TaskState) -> bool {
        self.state
            .compare_exchange(from as u32, to as u32, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// The address listeners block on
    #[inline(always)]
    pub(crate) fn wait_address(&self) -> *mut c_void {
        &self.state as *const AtomicU32 as *mut c_void
    }

    /// The encoded id of the next slot on the free list
    #[inline(always)]
    pub(crate) fn next(&self) -> usize {
        self.next.load(Ordering::Relaxed) as usize
    }

    /// Points this retired slot at the next one
    #[inline(always)]
    pub(crate) fn set_next(&self, next: usize) {
        self.next.store(next as u32, Ordering::Relaxed);
    }

    /// The encoded id of the next task in the queue holding
    /// this one
    #[inline(always)]
    pub(crate) fn queue_next(&self) -> usize {
        self.queue_next.load(Ordering::Acquire) as usize
    }

    /// Points this queued task at the one behind it
    #[inline(always)]
    pub(crate) fn set_queue_next(&self, next: usize) {
        self.queue_next.store(next as u32, Ordering::Release);
    }

    /// Stamps the class the caller asked for and the order
    /// this task was spawned in
    ///
    /// Written once, at creation. A task that moves between
    /// queues keeps the sequence it was born with, because
    /// restamping it on a spill or a steal would reset the
    /// age of the task that had waited longest, which is
    /// exactly backwards
    #[inline(always)]
    pub(crate) fn set_priority(&self, class: u8, sequence: u64) {
        self.priority
            .store(pack(class, sequence), Ordering::Release);
    }

    /// The class this task was spawned at
    #[inline(always)]
    pub(crate) fn priority_class(&self) -> u8 {
        (self.priority.load(Ordering::Acquire) >> PRIORITY_CLASS_SHIFT) as u8
    }

    /// The order this task was spawned in
    #[inline(always)]
    pub(crate) fn priority_sequence(&self) -> u64 {
        self.priority.load(Ordering::Acquire) & PRIORITY_SEQUENCE_MASK
    }

    /// The band that serves this task's class
    #[inline(always)]
    pub(crate) fn band(&self) -> usize {
        (self.priority_class() >> PRIORITY_BAND_SHIFT) as usize
    }

    /// How many tasks have been spawned since this one
    ///
    /// The age a starving task is measured by. Counted in
    /// tasks rather than in time, because being overtaken is
    /// what actually starves a task and it costs no clock
    /// call to know
    #[inline(always)]
    pub(crate) fn age(&self, now: u64) -> u64 {
        now.saturating_sub(self.priority_sequence())
    }

    /// Takes a read of the output, if there is still one
    ///
    /// ## Returns
    /// Whether the caller may go on to read the payload. A
    /// `false` means the output went somewhere between the
    /// wait and here, and the caller has taken nothing
    ///
    /// #### Note
    /// Sequentially consistent on purpose, and the one place
    /// in the crate that needs to be. This adds to one word
    /// then reads another, while `claim_result` writes that
    /// other word then reads this one. Anything weaker lets
    /// both sides see the old value and both decide they are
    /// clear to go, which is a clone reading an output that
    /// has already been moved away
    #[inline(always)]
    pub(crate) fn enter_read(&self) -> bool {
        self.readers.fetch_add(1, Ordering::SeqCst);

        if TaskState::from_u32(self.state.load(Ordering::SeqCst)) == TaskState::Ready {
            return true;
        }

        self.readers.fetch_sub(1, Ordering::SeqCst);

        false
    }

    /// Gives a read of the output back
    #[inline(always)]
    pub(crate) fn leave_read(&self) {
        self.readers.fetch_sub(1, Ordering::SeqCst);
    }

    /// Claims the output so the caller can move it out
    ///
    /// Winning the move to `Taken` stops any further read
    /// from starting, and waiting the current ones out is
    /// what makes it safe to take the value away
    ///
    /// ## Returns
    /// Whether the caller now owns the output
    pub(crate) fn claim_result(&self) -> bool {
        if !self.try_state(TaskState::Ready, TaskState::Taken) {
            return false;
        }

        // Bounded by however long a clone takes, and only
        // ever spun on when a read is genuinely in flight
        while self.readers.load(Ordering::SeqCst) > 0 {
            thread::yield_now();
        }

        true
    }

    /// Says which queue this task is now waiting on
    ///
    /// Never overwrites a cancel already in progress, since
    /// that canceller is holding the field precisely so the
    /// waiter can't move on underneath it
    #[inline(always)]
    pub(crate) fn set_waiting(&self, queue: i32) {
        let _ = self.waiting.compare_exchange(
            NOT_WAITING,
            queue,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Says this task is no longer waiting on anything
    ///
    /// ## Behaviour
    /// Spins while a cancel is in flight, which is the whole
    /// point of the field. A waiter that cleared it and carried
    /// on could finish, let its thread be reaped and its queue
    /// closed, and leave the canceller making a syscall against
    /// a descriptor that now belongs to something else
    /// Says a `join_first` wants this slot to poke `queue` when
    /// it settles
    ///
    /// ## Returns
    /// Whether the registration took. `false` means somebody
    /// else got there first, and the caller falls back to
    /// looking again rather than being told
    ///
    /// #### Note
    /// Registering is only ever an optimisation. The state word
    /// is the truth, and a caller that never hears anything
    /// still finds its answer by reading it
    pub(crate) fn set_select(&self, queue: i32) -> bool {
        self.select
            .compare_exchange(NO_SELECT, queue, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Takes a `join_first`'s registration back off
    ///
    /// Compared rather than stored, so a caller leaving only
    /// clears its own registration and never somebody else's
    pub(crate) fn clear_select(&self, queue: i32) {
        let _ = self
            .select
            .compare_exchange(queue, NO_SELECT, Ordering::AcqRel, Ordering::Acquire);
    }

    /// The kqueue to poke when this settles, if there is one
    #[inline(always)]
    pub(crate) fn select_queue(&self) -> i32 {
        self.select.load(Ordering::Acquire)
    }

    pub(crate) fn clear_waiting(&self) {
        loop {
            let current = self.waiting.load(Ordering::Acquire);

            if current == CANCELLING {
                std::hint::spin_loop();
                continue;
            }

            if self
                .waiting
                .compare_exchange(current, NOT_WAITING, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Takes hold of the queue this task is waiting on
    ///
    /// ## Returns
    /// The queue, and the right to make syscalls against it
    /// until `release_waiting` hands it back. `None` if the
    /// task isn't waiting, or if somebody else got there first
    pub(crate) fn claim_waiting(&self) -> Option<i32> {
        loop {
            let current = self.waiting.load(Ordering::Acquire);

            if current < 0 {
                return None;
            }

            if self
                .waiting
                .compare_exchange(current, CANCELLING, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(current);
            }
        }
    }

    /// Lets the waiter move on again
    #[inline(always)]
    pub(crate) fn release_waiting(&self) {
        self.waiting.store(NOT_WAITING, Ordering::Release);
    }

    /// What happens when a run of this finishes
    #[inline(always)]
    pub(crate) fn kind(&self) -> TaskKind {
        TaskKind::from_u8(self.kind.load(Ordering::Acquire))
    }

    /// Whether this task wants a thread it can block
    #[inline(always)]
    pub(crate) fn blocking(&self) -> bool {
        self.blocking.load(Ordering::Acquire)
    }

    /// Nanoseconds to wait before running this again
    #[inline(always)]
    pub(crate) fn interval(&self) -> u64 {
        self.interval.load(Ordering::Acquire)
    }

    /// Takes one off the run count and says whether that was
    /// the last one allowed
    ///
    /// ## Behaviour
    /// The question and the bookkeeping in one, because every
    /// caller that asks is a run that has just happened. Asking
    /// twice for one run would count it twice
    ///
    /// `u32::MAX` means unbounded and is left alone rather than
    /// counted down — a series that was never given a limit
    /// should not acquire one after four billion runs
    pub(crate) fn count_run(&self) -> bool {
        loop {
            let left = self.runs_left.load(Ordering::Acquire);

            if left == u32::MAX {
                return false;
            }

            let next = left.saturating_sub(1);

            if self
                .runs_left
                .compare_exchange(left, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next == 0;
            }
        }
    }

    /// Whether any runs are still allowed
    ///
    /// Asked without counting, for a schedule that has to know
    /// before it starts a run rather than after
    #[inline(always)]
    pub(crate) fn runs_remain(&self) -> bool {
        self.runs_left.load(Ordering::Acquire) != 0
    }

    /// Whether the next run would begin past the deadline
    ///
    /// ## Behaviour
    /// Compares the moment the next run would *start* rather
    /// than the moment this is asked, so a run that would begin
    /// after the deadline is never begun. A repeat on a 750ms
    /// gap bounded to a second therefore runs at 0ms and at
    /// 750ms and then stops, because a third would land near
    /// 1500ms
    ///
    /// `gap` is however long the wait before the next run is —
    /// the interval for anything that waits, and zero for a
    /// repeat that goes straight back on the queue
    pub(crate) fn past_deadline(&self, gap: Duration) -> bool {
        let Some(until) = self.until else {
            return false;
        };

        match Instant::now().checked_add(gap) {
            Some(next) => next >= until,
            None => true,
        }
    }

    /// Whether this was a bounded series that reached its end
    ///
    /// ## Behaviour
    /// Read from the bound fields rather than from a flag of
    /// its own, because they already say it. A one shot can
    /// never have been given a bound — the setters ask for a
    /// repeating kind — so it keeps `u32::MAX` runs and no
    /// deadline, and nothing else can look like both
    ///
    /// #### Note
    /// Only meaningful once the kind says the task is over.
    /// Half way through, a bounded series has a spent looking
    /// count and is very much still going, which is why the
    /// caller asks about the kind first
    #[inline(always)]
    pub(crate) fn spent(&self) -> bool {
        self.runs_left.load(Ordering::Acquire) != u32::MAX || self.until.is_some()
    }

    /// Says this task will not run again
    ///
    /// ## Behaviour
    /// Flipping the kind is the whole of how a bounded series
    /// ends, and it is deliberately all that changes. The state
    /// is left alone because a series that ran out *succeeded* —
    /// its last output is still there to be read, and turning
    /// it into a failure after the fact would throw away the
    /// one thing it produced
    ///
    /// Nothing re-arms a `Once`, so this single store closes
    /// every path that would have carried the series on: `run`
    /// won't queue it again, a cancel treats it as a finished
    /// one shot, the teardown sweeps skip it, and a late run of
    /// a schedule finds `begin` refusing and drops its output
    #[inline(always)]
    pub(crate) fn finish_series(&self) {
        self.kind.store(TaskKind::Once as u8, Ordering::Release);
    }

    /// Nanoseconds still owed before the first run, or zero
    #[inline(always)]
    pub(crate) fn start_delay(&self) -> u64 {
        self.start_delay.load(Ordering::Acquire)
    }

    /// Says the first run has begun, so no delay is owed
    ///
    /// Stored rather than exchanged because every run of a
    /// repeat comes through here and only the first can find
    /// anything to clear. Writing zero over zero costs nothing
    /// and needs no branch
    #[inline(always)]
    pub(crate) fn clear_start_delay(&self) {
        self.start_delay.store(0, Ordering::Release);
    }

    /// The task a series clones its runs from, or null
    #[inline(always)]
    pub(crate) fn prototype(&self) -> *mut c_void {
        self.prototype.load(Ordering::Acquire)
    }

    /// Gives a series the task it makes copies of
    ///
    /// ## Safety
    /// Written once, by the thread that allocated the slot,
    /// before anything else can reach it. Nothing is armed and
    /// no handle exists at that point, so this is the last of
    /// the setup rather than a change to a live slot
    ///
    /// The pointer must be a `Box<Box<dyn SeriesTask>>`, since
    /// that is what `destroy` will drop it as
    #[inline(always)]
    pub(crate) fn set_prototype(&self, prototype: *mut c_void) {
        self.prototype.store(prototype, Ordering::Release);
    }

    /// Takes the `Executor`'s reference on this task
    ///
    /// ## Returns
    /// Whether the caller is the one that should give it back.
    /// Exactly one caller ever gets `true`, however many decide
    /// they are finished with the task and in whatever order
    #[inline(always)]
    pub(crate) fn claim_release(&self) -> bool {
        self.held.swap(false, Ordering::AcqRel)
    }

    /// Says a wake for this task is on its way
    ///
    /// Set before the timer is registered rather than after. A
    /// manager that comes back to find this set re-arms a timer
    /// that may never have been registered at all, which costs
    /// one duplicate registration — and the other order costs a
    /// wake that nothing knows to put back
    #[inline(always)]
    pub(crate) fn arm(&self) {
        self.armed.store(true, Ordering::Release);
    }

    /// Says the wake never happened
    #[inline(always)]
    pub(crate) fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }

    /// Whether a wake for this task is still owed
    #[inline(always)]
    pub(crate) fn armed(&self) -> bool {
        self.armed.load(Ordering::Acquire)
    }

    /// Takes the wake, and with it the job of queuing the task
    ///
    /// ## Returns
    /// Whether the caller is the one that should put the task
    /// back. Exactly one caller ever gets `true` per wait,
    /// however many wakes turn up for it
    #[inline(always)]
    pub(crate) fn claim_armed(&self) -> bool {
        self.armed.swap(false, Ordering::AcqRel)
    }

    /// Takes the slot for a run
    ///
    /// ## Returns
    /// Whether the caller may go ahead and run the task. A
    /// `false` means somebody cancelled it, or it failed, or a
    /// second caller got here first, and in every case there is
    /// nothing to run
    ///
    /// ## Behaviour
    /// A task that runs once only ever comes here waiting to
    /// start. A repeating one comes back round holding the
    /// output of its last run, or holding nothing if a listener
    /// took it, so those are starting points too
    ///
    /// Winning the move into `Running` is what stops any
    /// further read from beginning, which is what makes it safe
    /// to throw the last output away and write another
    pub(crate) fn begin(&self) -> bool {
        if self.try_state(TaskState::Pending, TaskState::Running) {
            return true;
        }

        if !self.kind().repeats() {
            return false;
        }

        for from in [TaskState::Ready, TaskState::Taken] {
            if self.try_state(from, TaskState::Running) {
                self.recycle();
                return true;
            }
        }

        false
    }

    /// Throws away the output of the run before this one
    ///
    /// ## Safety
    /// Only the caller that won the move into `Running` may do
    /// this, which is why it is private and why `begin` is the
    /// only thing that calls it. That move is what stopped any
    /// further read from starting, and the wait below is what
    /// sees out the reads that had already started. Dropping
    /// without both is dropping a value from underneath a
    /// listener part way through cloning it
    fn recycle(&self) {
        while self.readers.load(Ordering::SeqCst) > 0 {
            thread::yield_now();
        }

        if self.filled.swap(false, Ordering::AcqRel) {
            unsafe { (self.drop_glue)(self.payload()) };
        }
    }

    /// Puts a task back for another run
    ///
    /// The same box that came out, so a repeating task costs no
    /// allocation for going round again
    #[inline(always)]
    pub(crate) fn rearm(&self, task: *mut c_void) {
        self.task.store(task, Ordering::Release);
    }

    /// Records that the payload now holds a value
    #[inline(always)]
    pub(crate) fn fill(&self) {
        self.filled.store(true, Ordering::Release);
    }

    /// Takes ownership of the payload away from the slot
    ///
    /// Only the thread that won the move to `Taken` may call
    /// this, which is what keeps it to one caller
    #[inline(always)]
    pub(crate) fn empty(&self) {
        self.filled.store(false, Ordering::Release);
    }

    /// Adds a listener
    #[inline(always)]
    pub(crate) fn add_listener(&self) {
        self.listeners.fetch_add(1, Ordering::Relaxed);
    }

    /// Drops a listener
    ///
    /// ## Returns
    /// Whether the caller was the last one out, and so the
    /// only thread that can still see the slot
    #[inline(always)]
    pub(crate) fn drop_listener(&self) -> bool {
        self.listeners.fetch_sub(1, Ordering::AcqRel) == 1
    }

    /// Takes the task out of the slot
    ///
    /// Claiming is what makes a task run at most once. However
    /// many triggers arrive for an id, only the first swap
    /// comes back with anything in it
    #[inline(always)]
    pub(crate) fn claim(&self) -> *mut c_void {
        self.task.swap(ptr::null_mut(), Ordering::AcqRel)
    }

    /// Drops everything the slot owns and empties it
    ///
    /// The slot's own memory belongs to a table block and
    /// stays where it is, ready for the next task to take the
    /// id. Only an oversized output, which had a mapping to
    /// itself, goes back to the kernel
    ///
    /// ## Safety
    /// Only the last listener may call this, and nothing may
    /// touch the slot afterwards. The payload is dropped here
    /// and nowhere else, which is what keeps it to one owner
    pub(crate) unsafe fn destroy(&self) {
        // A task nobody ever got round to running still owns
        // itself, so it goes back the way it came
        let task = self.claim();

        if !task.is_null() {
            drop(unsafe { Box::from_raw(task.cast::<Box<dyn ErasedTask>>()) });
        }

        // An output nobody took is still a live value
        if self.filled.load(Ordering::Acquire) {
            unsafe { (self.drop_glue)(self.payload()) };
        }

        // A series owns the task it was making copies of, and
        // is the only kind of slot that has one
        let prototype = self.prototype.swap(ptr::null_mut(), Ordering::AcqRel);

        if !prototype.is_null() {
            drop(unsafe { Box::from_raw(prototype.cast::<Box<dyn SeriesTask>>()) });
        }

        let oversized = self.payload.swap(ptr::null_mut(), Ordering::AcqRel);

        if !oversized.is_null() {
            mapping::free(oversized, self.size as usize);
        }

        self.waiting.store(NOT_WAITING, Ordering::Release);
        self.select.store(NO_SELECT, Ordering::Release);

        // Nothing is in a queue once it is being destroyed, so
        // the link is cleared rather than left pointing at a
        // task the next user of this id has nothing to do with
        self.queue_next.store(0, Ordering::Release);

        // Last, so the slot only reads as empty once there is
        // genuinely nothing left in it
        self.set_state(TaskState::Free);
    }
}

/// Packs a class and a sequence into the one priority word
#[inline(always)]
const fn pack(class: u8, sequence: u64) -> u64 {
    ((class as u64) << PRIORITY_CLASS_SHIFT) | (sequence & PRIORITY_SEQUENCE_MASK)
}

/// Drops a payload of type `T` in place
///
/// Handed to the slot at creation, which is the last moment
/// the output type is still known, and is the only way a
/// slot can clean up after a type it can no longer name
unsafe fn glue<T>(payload: *mut u8) {
    unsafe { ptr::drop_in_place(payload.cast::<T>()) };
}
