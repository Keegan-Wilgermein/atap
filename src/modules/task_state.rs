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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TaskState {
    /// No task in this slot
    ///
    /// Zero on purpose. Table blocks come back from the
    /// kernel zeroed, so a block that has never been touched
    /// already reads as a run of empty slots with nothing
    /// needing to be written to it first
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

    /// The `Executor` died before it could finish the task
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
        return match raw {
            0 => Self::Free,
            1 => Self::Pending,
            2 => Self::Running,
            3 => Self::Ready,
            4 => Self::Taken,
            5 => Self::Cancelled,
            _ => Self::Failed,
        };
    }

    /// Whether the state can still change
    ///
    /// Terminal states are the ones worth waking a listener
    /// for, and the point past which a read won't block
    #[inline(always)]
    pub(crate) fn terminal(self) -> bool {
        return !matches!(self, Self::Pending | Self::Running);
    }
}
