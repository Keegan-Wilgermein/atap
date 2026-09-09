//! # Task State
//! Where a spawned task is in its life, from the moment
//! the `Executor` is handed it to the moment its slot
//! goes back to the kernel

/// The lifecycle of one spawned task
///
/// Held in a `TaskData` as an `AtomicU32` rather than
/// anything richer because it doubles as the address
/// listeners block on, and `os_sync_wait_on_address` only
/// watches words of 4 or 8 bytes
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TaskState {
    /// Waiting for the `Executor` to claim it
    Pending = 0,

    /// Claimed, and running right now
    Running = 1,

    /// The output is written and safe to read
    Ready = 2,

    /// The output was moved out by `take`,
    /// so there is nothing left to hand out
    Taken = 3,

    /// Abandoned by a listener
    ///
    /// A task already in flight still runs to the end,
    /// its result just never reaches anyone
    Cancelled = 4,

    /// The `Executor` died before it could finish the task
    Failed = 5,
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
            0 => Self::Pending,
            1 => Self::Running,
            2 => Self::Ready,
            3 => Self::Taken,
            4 => Self::Cancelled,
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
