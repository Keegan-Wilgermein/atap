//! # Task State
//! Where a spawned task is in its life, from the moment
//! the `Executor` is handed it to the moment its slot
//! goes back on the free list

/// The lifecycle of one spawned task
///
/// Held in a `TaskData` as an `AtomicU32` rather than
/// anything richer because it doubles as the address
/// listeners block on, and `os_sync_wait_on_address` only
/// watches words of 4 or 8 bytes
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskState {
    /// No task in this slot
    ///
    /// Zero on purpose. Table blocks come back from the
    /// kernel zeroed, so a block that has never been touched
    /// already reads as a run of empty slots with nothing
    /// needing to be written to it first
    ///
    /// #### Note
    /// A `TaskHandle` never sees this. The `Executor` filters
    /// an empty slot out before anything can read one and
    /// answers `Failed` in its place, so this is bookkeeping
    /// that happens to be visible rather than a state a task
    /// can be found in. It is public only because matching on
    /// the rest of the enum has to account for it
    Free = 0,

    /// Waiting for the `Executor` to claim it
    Pending = 1,

    /// Claimed, and running right now
    Running = 2,

    /// The output is written and safe to read
    Ready = 3,

    /// The output was moved out by `take`,
    /// so there is nothing left to hand out
    Taken = 4,

    /// Abandoned by a listener
    ///
    /// A task already in flight still runs to the end,
    /// its result just never reaches anyone
    Cancelled = 5,

    /// Nothing is going to produce an output for this task
    ///
    /// Covers every way that can happen: the task panicked,
    /// the thread running it died holding it, there was
    /// nothing left to run it, or a repeat could not be put
    /// back on the clock. What they have in common is that
    /// waiting longer won't help
    Failed = 6,
}

impl TaskState {
    /// Rebuilds a state from the raw value in the atomic
    ///
    /// Anything unrecognised is treated as `Failed`, since a
    /// state this crate didn't write means the slot can't be
    /// trusted and a listener waiting on it should be let go
    #[inline(always)]
    pub(crate) fn from_u32(raw: u32) -> Self {
        match raw {
            0 => Self::Free,
            1 => Self::Pending,
            2 => Self::Running,
            3 => Self::Ready,
            4 => Self::Taken,
            5 => Self::Cancelled,
            _ => Self::Failed,
        }
    }

    /// Whether the state can still change
    ///
    /// Terminal states are the ones worth waking a listener
    /// for, and the point past which a read won't block
    ///
    /// #### Note
    /// Settled is not the same as having an output. A
    /// cancelled task, a failed one and one whose output has
    /// already been taken have all settled, and none of them
    /// has anything to hand out
    #[inline(always)]
    pub fn terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
}
