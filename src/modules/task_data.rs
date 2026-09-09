//! # Task Data
//! The memory a single spawned task owns, from the task
//! waiting to be run through to the output waiting to be read
//!
//! One mapping per task, taken straight from the kernel so
//! that the address holds still for the whole life of the
//! task no matter what the table around it does. The header
//! sits at the base and the output lives at a fixed offset
//! past it, which is what lets the output type stay erased
//!
//! #### Note
//! Nothing outside the `Executor` may call any of this. The
//! `Executor` is the only thing that knows when a slot is
//! still alive, so it is the only thing allowed to touch one

use crate::{
    constants::PAYLOAD_OFFSET,
    modules::{erased_task::ErasedTask, mapping, task_state::TaskState},
};
use libc::c_void;
use std::{
    mem, ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU32, AtomicUsize, Ordering},
    thread,
};

/// Everything one spawned task owns
///
/// Laid out `repr(C)` because the payload is found by adding
/// a fixed offset to the base of the header, and that only
/// holds if the header can't be reordered out from under it
#[repr(C)]
pub(crate) struct TaskData {
    /// Where the task is in its life
    ///
    /// Also the address every listener blocks on, which is
    /// why it is a `u32` and why it is first
    state: AtomicU32,

    /// Whether the payload currently holds a value
    ///
    /// Tracked apart from the state so that cancelling a
    /// finished task doesn't have to drop the output out
    /// from under a listener that is reading it
    ///
    /// Sits here rather than further down so the header packs
    /// into the space before the payload with room to spare
    filled: AtomicBool,

    /// Live `TaskHandle`s, plus one for the `Executor`
    /// until it has finished with the task
    listeners: AtomicUsize,

    /// The erased task, null once claimed
    ///
    /// A `Box<dyn ErasedTask>` is two words wide and can't
    /// sit in an atomic, so what is stored is a thin pointer
    /// to that box
    task: AtomicPtr<c_void>,

    /// Listeners part way through cloning the output
    ///
    /// A clone reads the payload where it lies, so moving the
    /// output out has to wait for any clone already under way
    /// to finish rather than pulling it out from under one
    readers: AtomicUsize,

    /// Drops a payload of the output type in place
    drop_glue: unsafe fn(*mut u8),

    /// The length of the mapping, for handing it back
    map_len: usize,

    /// `size_of` the output type, checked before any read
    size: usize,
}

impl TaskData {
    /// Maps and fills in the memory for one task
    ///
    /// `listeners` starts at 2, one for the `TaskHandle`
    /// being handed back and one for the `Executor`. The
    /// `Executor`'s reference is what stops a handle that is
    /// dropped immediately from freeing the slot out from
    /// under the thread about to run it
    ///
    /// ## Returns
    /// The base of the mapping, or null if the kernel
    /// refused it
    pub(crate) fn create<T>(task: *mut c_void, state: TaskState) -> *mut Self {
        // Both of these fold away at compile time, and both
        // are silent memory corruption if they ever fail
        const { assert!(mem::size_of::<Self>() <= PAYLOAD_OFFSET) };
        assert!(mem::align_of::<T>() <= PAYLOAD_OFFSET);

        let size = mem::size_of::<T>();
        let map_len = mapping::round_up(PAYLOAD_OFFSET + size);
        let base = mapping::alloc(map_len);

        if base.is_null() {
            return ptr::null_mut();
        }

        let data = base.cast::<Self>();

        unsafe {
            data.write(Self {
                state: AtomicU32::new(state as u32),
                filled: AtomicBool::new(false),
                listeners: AtomicUsize::new(2),
                task: AtomicPtr::new(task),
                readers: AtomicUsize::new(0),
                drop_glue: glue::<T>,
                map_len,
                size,
            })
        };

        return data;
    }

    /// Where the output lives, filled or not
    #[inline(always)]
    pub(crate) fn payload(&self) -> *mut u8 {
        return unsafe { (self as *const Self as *mut u8).add(PAYLOAD_OFFSET) };
    }

    /// `size_of` the output type this slot was built for
    #[inline(always)]
    pub(crate) fn size(&self) -> usize {
        return self.size;
    }

    /// The current state
    #[inline(always)]
    pub(crate) fn state(&self) -> TaskState {
        return TaskState::from_u32(self.state.load(Ordering::Acquire));
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
        return self
            .state
            .compare_exchange(from as u32, to as u32, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
    }

    /// The address listeners block on
    #[inline(always)]
    pub(crate) fn wait_address(&self) -> *mut c_void {
        return &self.state as *const AtomicU32 as *mut c_void;
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

        return false;
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

        return true;
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
        return self.listeners.fetch_sub(1, Ordering::AcqRel) == 1;
    }

    /// Takes the task out of the slot
    ///
    /// Claiming is what makes a task run at most once. However
    /// many triggers arrive for an id, only the first swap
    /// comes back with anything in it
    #[inline(always)]
    pub(crate) fn claim(&self) -> *mut c_void {
        return self.task.swap(ptr::null_mut(), Ordering::AcqRel);
    }

    /// Whether the task is still sitting there unclaimed
    #[inline(always)]
    pub(crate) fn unclaimed(&self) -> bool {
        return !self.task.load(Ordering::Acquire).is_null();
    }

    /// Drops everything the slot owns and gives the
    /// mapping back
    ///
    /// ## Safety
    /// Only the last listener may call this, and nothing may
    /// touch the slot afterwards. The payload is dropped here
    /// and nowhere else, which is what keeps it to one owner
    pub(crate) unsafe fn destroy(data: *mut Self) {
        let this = unsafe { &*data };

        // A task nobody ever got round to running still owns
        // itself, so it goes back the way it came
        let task = this.claim();

        if !task.is_null() {
            drop(unsafe { Box::from_raw(task.cast::<Box<dyn ErasedTask>>()) });
        }

        // An output nobody took is still a live value
        if this.filled.load(Ordering::Acquire) {
            unsafe { (this.drop_glue)(this.payload()) };
        }

        let map_len = this.map_len;
        mapping::free(data.cast::<u8>(), map_len);
    }
}

/// Drops a payload of type `T` in place
///
/// Handed to the slot at creation, which is the last moment
/// the output type is still known, and is the only way a
/// slot can clean up after a type it can no longer name
unsafe fn glue<T>(payload: *mut u8) {
    unsafe { ptr::drop_in_place(payload.cast::<T>()) };
}
