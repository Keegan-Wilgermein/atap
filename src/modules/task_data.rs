//! # Task Data
//! The memory one spawned task owns, from waiting to run to its
//! output waiting to be read
//!
//! Only the `Executor` may touch a slot, since only it knows
//! whether the slot is still alive

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
    sync::atomic::{
        AtomicBool, AtomicI8, AtomicI32, AtomicPtr, AtomicU32, AtomicU64, AtomicU8, Ordering,
    },
    thread,
    time::{Duration, Instant},
};

/// Everything one spawned task owns
///
/// `repr(C)` because the payload sits at a fixed offset past
/// the header, and the header must fit inside `PAYLOAD_OFFSET`
#[repr(C)]
pub(crate) struct TaskData {
    /// Where the task is in its life
    ///
    /// Also the address listeners block on, so it is a `u32`
    state: AtomicU32,

    /// Live `TaskHandle`s, plus one for the `Executor`
    /// until it has finished with the task
    listeners: AtomicU32,

    /// Listeners part way through cloning the output
    ///
    /// A take waits for these before moving the output out
    readers: AtomicU32,

    /// `size_of` the output type, checked before any read
    size: u32,

    /// The next slot on the free list, as its id plus one
    ///
    /// Only meaningful while the slot is `Free`
    next: AtomicU32,

    /// The next task in whichever queue holds this one, as its id
    /// plus one
    ///
    /// Separate from `next`, since a queued task is alive and a
    /// free slot isn't
    queue_next: AtomicU32,

    /// Whether the payload currently holds a value
    ///
    /// Separate from the state, so a cancel never drops an output
    /// a listener is reading
    filled: AtomicBool,

    /// What happens when a run of this finishes
    kind: AtomicU8,

    /// Whether this task wants a thread it can block, asked once
    /// at spawn
    blocking: AtomicBool,

    /// Whether the `Executor` still holds its reference
    ///
    /// Several paths can each be the last to finish with a task, so
    /// the reference is won with a swap. Whoever takes it releases;
    /// a second release would free a live slot
    held: AtomicBool,

    /// Whether a wake for this task is out on the manager's queue
    ///
    /// Taken by whoever acts on the wake, so however many wakes
    /// arrive for one wait, the task is queued once
    armed: AtomicBool,

    /// Whether this task is parked on a socket, holding no thread
    ///
    /// Separate from `armed`, so a wake left over from a parked task
    /// that has gone can never start a delayed task early in its
    /// place
    parked: AtomicBool,

    /// Which `EVFILT_` the park is waiting on
    ///
    /// Every filter is a small negative number, so a byte holds any
    /// of them
    park_filter: AtomicI8,

    /// Whether the park has a deadline timer beside its watch
    park_timed: AtomicBool,

    /// Runs still allowed, or `u32::MAX` for no limit
    runs_left: AtomicU32,

    /// What a parked task is watching, which is whatever its filter
    /// takes: a descriptor, or a signal number
    ///
    /// Written before `parked` is set, and only read by whoever
    /// claims the park
    park_ident: AtomicI32,

    /// The moment this stops repeating, if it does
    ///
    /// Plain rather than atomic: written in `init` before the state
    /// is published, and never again
    until: Option<Instant>,

    /// Nanoseconds to wait before the first run, or zero
    ///
    /// Cleared when the first run begins, so a restarted manager
    /// can tell a start delay from a gap between runs
    start_delay: AtomicU64,

    /// Nanoseconds a `RepeatEvery` task waits between runs, zero
    /// for everything else
    interval: AtomicU64,

    /// The kqueue this task is waiting on, or `NOT_WAITING`, so a
    /// cancel can reach into the wait
    waiting: AtomicI32,

    /// The kqueue a `join_first` wants poked when this settles, or
    /// `NO_SELECT`
    ///
    /// One at a time, and only an optimisation, since a
    /// `join_first` re-reads the state regardless
    select: AtomicI32,

    /// The class this task was spawned at and the order it was
    /// spawned in, packed into one word
    priority: AtomicU64,

    /// The erased task, null once claimed
    ///
    /// A thin pointer to the `Box<dyn ErasedTask>`, which is too
    /// wide for an atomic
    task: AtomicPtr<c_void>,

    /// Drops a payload of the output type in place
    drop_glue: unsafe fn(*mut u8),

    /// An output too big to sit beside the header, or null
    payload: AtomicPtr<u8>,

    /// The task a `Series` clones its runs from, or null
    ///
    /// A series slot's `task` is null, which is also what stops it
    /// ever being run
    prototype: AtomicPtr<c_void>,
}

/// The header has to fit in front of the payload
const _: () = assert!(mem::size_of::<TaskData>() <= PAYLOAD_OFFSET);

impl TaskData {
    /// Fills in an empty slot ready for a task
    ///
    /// ## Returns
    /// Whether the slot is ready. Fails only if an oversized output
    /// can't be mapped
    ///
    /// ## Safety
    /// The id must have come from `TaskTable::alloc`, so nothing
    /// else is looking at the slot. The state is published last
    pub(crate) unsafe fn init<T>(
        data: *mut Self,
        task: *mut c_void,
        state: TaskState,
        setup: TaskSetup,
        sequence: u64,
    ) -> bool {
        // Both are silent memory corruption if they ever stop holding
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
                parked: AtomicBool::new(false),
                park_filter: AtomicI8::new(0),
                park_timed: AtomicBool::new(false),
                runs_left: AtomicU32::new(setup.runs),
                park_ident: AtomicI32::new(-1),
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

        unsafe { (*data).set_priority(setup.priority, sequence) };

        // Published last, so anything finding the task live sees the
        // whole header
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
    /// Every transition two threads could race for goes through
    /// here
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

    /// Stamps the class and the order this task was spawned in
    ///
    /// Written once. Moving between queues keeps the original
    /// sequence, so a task's age isn't reset
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

    /// How many tasks have been spawned since this one, which is
    /// its age
    #[inline(always)]
    pub(crate) fn age(&self, now: u64) -> u64 {
        now.saturating_sub(self.priority_sequence())
    }

    /// Takes a read of the output, if there is still one
    ///
    /// ## Returns
    /// Whether the caller may read the payload
    ///
    /// #### Note
    /// `SeqCst`, against `claim_result`. Each writes one word then
    /// reads the other, and anything weaker lets both go ahead
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
    /// Winning the move to `Taken` stops new reads, and reads
    /// already under way are waited out
    ///
    /// ## Returns
    /// Whether the caller now owns the output
    pub(crate) fn claim_result(&self) -> bool {
        if !self.try_state(TaskState::Ready, TaskState::Taken) {
            return false;
        }

        // Only spins while a clone is in flight
        while self.readers.load(Ordering::SeqCst) > 0 {
            thread::yield_now();
        }

        true
    }

    /// Says which queue this task is now waiting on
    ///
    /// Never overwrites a cancel in progress
    #[inline(always)]
    pub(crate) fn set_waiting(&self, queue: i32) {
        let _ = self.waiting.compare_exchange(
            NOT_WAITING,
            queue,
            Ordering::AcqRel,
            Ordering::Relaxed,
        );
    }

    /// Registers `queue` to be poked when this settles
    ///
    /// ## Returns
    /// Whether the registration took. `false` means somebody else
    /// is registered already
    pub(crate) fn set_select(&self, queue: i32) -> bool {
        self.select
            .compare_exchange(NO_SELECT, queue, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Takes a `join_first`'s registration back off, if it is
    /// still this one
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

    /// Says this task is no longer waiting on anything
    ///
    /// Spins while a cancel is in flight, so the canceller never
    /// makes a syscall against a queue that has been closed and
    /// reused
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

    /// Takes one off the run count, saying whether that was the
    /// last one allowed
    ///
    /// `u32::MAX` is unbounded and never counted down
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

    /// Whether any runs are still allowed, without counting one
    #[inline(always)]
    pub(crate) fn runs_remain(&self) -> bool {
        self.runs_left.load(Ordering::Acquire) != 0
    }

    /// Whether the next run would start past the deadline
    ///
    /// `gap` is the wait before that run: the interval for anything
    /// that waits, zero otherwise
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
    /// Only meaningful once the kind says the task is over
    #[inline(always)]
    pub(crate) fn spent(&self) -> bool {
        self.runs_left.load(Ordering::Acquire) != u32::MAX || self.until.is_some()
    }

    /// Says this task will not run again
    ///
    /// Only the kind changes, so a series that ran out keeps its
    /// last output
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
    /// Only before anything else can reach the slot. The pointer
    /// must be a `Box<Box<dyn SeriesTask>>`
    #[inline(always)]
    pub(crate) fn set_prototype(&self, prototype: *mut c_void) {
        self.prototype.store(prototype, Ordering::Release);
    }

    /// Takes the `Executor`'s reference on this task
    ///
    /// ## Returns
    /// Whether the caller should give it back. Only one caller ever
    /// gets `true`
    #[inline(always)]
    pub(crate) fn claim_release(&self) -> bool {
        self.held.swap(false, Ordering::AcqRel)
    }

    /// Says a wake for this task is on its way
    ///
    /// Set before the timer is registered, so a restarted manager
    /// can always find it
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
    /// Whether the caller should queue it. Only one caller ever
    /// gets `true` per wait
    #[inline(always)]
    pub(crate) fn claim_armed(&self) -> bool {
        self.armed.swap(false, Ordering::AcqRel)
    }

    /// Says this task is parked on `fd`, with its task back in the
    /// slot
    ///
    /// Set before anything is registered, so every wake can find
    /// it
    #[inline(always)]
    pub(crate) fn park(&self, ident: i32, filter: i16, timed: bool) {
        self.park_ident.store(ident, Ordering::Relaxed);
        self.park_filter.store(filter as i8, Ordering::Relaxed);
        self.park_timed.store(timed, Ordering::Relaxed);

        // Publishes the three above to whoever claims it
        self.parked.store(true, Ordering::Release);
    }

    /// Whether this task is parked
    #[inline(always)]
    pub(crate) fn parked(&self) -> bool {
        self.parked.load(Ordering::Acquire)
    }

    /// Takes the park, and with it the job of doing something with
    /// the task
    ///
    /// ## Returns
    /// What it was parked on, as the ident, the filter, and whether
    /// it has a timer beside them. Only one caller ever gets `Some`
    /// per park
    #[inline(always)]
    pub(crate) fn claim_parked(&self) -> Option<(i32, i16, bool)> {
        if !self.parked.swap(false, Ordering::AcqRel) {
            return None;
        }

        Some((
            self.park_ident.load(Ordering::Relaxed),
            self.park_filter.load(Ordering::Relaxed) as i16,
            self.park_timed.load(Ordering::Relaxed),
        ))
    }

    /// Takes the slot for a run
    ///
    /// ## Returns
    /// Whether the caller may run the task. `false` means it was
    /// cancelled, failed, or already taken for a run
    ///
    /// A repeat can start from `Ready` or `Taken` as well as
    /// `Pending`. Winning the move into `Running` stops new reads
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
    /// Only after winning the move into `Running`, which stops new
    /// reads. Reads already under way are waited out
    fn recycle(&self) {
        while self.readers.load(Ordering::SeqCst) > 0 {
            thread::yield_now();
        }

        if self.filled.swap(false, Ordering::AcqRel) {
            unsafe { (self.drop_glue)(self.payload()) };
        }
    }

    /// Puts a task back for another run
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
    /// Only the thread that won the move to `Taken` may call this
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
    /// Only the first swap gets it, so a task runs at most once
    #[inline(always)]
    pub(crate) fn claim(&self) -> *mut c_void {
        self.task.swap(ptr::null_mut(), Ordering::AcqRel)
    }

    /// Drops everything the slot owns and empties it
    ///
    /// ## Safety
    /// Only the last listener may call this, and nothing may touch
    /// the slot afterwards
    pub(crate) unsafe fn destroy(&self) {
        // A task that never ran still owns itself
        let task = self.claim();

        if !task.is_null() {
            drop(unsafe { Box::from_raw(task.cast::<Box<dyn ErasedTask>>()) });
        }

        // An output nobody took is still a live value
        if self.filled.load(Ordering::Acquire) {
            unsafe { (self.drop_glue)(self.payload()) };
        }

        // A series owns its prototype
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
        self.parked.store(false, Ordering::Release);

        // Cleared, so the next task given this id doesn't inherit a
        // link
        self.queue_next.store(0, Ordering::Release);

        // Last, so the slot only reads as empty once it is
        self.set_state(TaskState::Free);
    }
}

/// Packs a class and a sequence into the one priority word
#[inline(always)]
const fn pack(class: u8, sequence: u64) -> u64 {
    ((class as u64) << PRIORITY_CLASS_SHIFT) | (sequence & PRIORITY_SEQUENCE_MASK)
}

/// Drops a payload of type `T` in place, for a slot that can no
/// longer name `T`
unsafe fn glue<T>(payload: *mut u8) {
    unsafe { ptr::drop_in_place(payload.cast::<T>()) };
}
