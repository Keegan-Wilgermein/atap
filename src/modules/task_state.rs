//! # Task State
//! Where a spawned task is in its life

/// The lifecycle of one spawned task
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TaskState {
    /// No task in this slot
    ///
    /// A `TaskHandle` never reports this. An empty slot reads as
    /// `Failed` through a handle
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
    /// The task panicked, the thread running it died, or nothing
    /// was left to run it
    Failed = 6,
}

impl TaskState {
    /// Rebuilds a state from the raw value in the atomic
    ///
    /// Anything unrecognised reads as `Failed`
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

    /// Whether a read on this state returns without waiting
    ///
    /// #### Note
    /// Doesn't mean there is an output. A cancelled, failed or
    /// taken task has settled with nothing to hand out
    #[inline(always)]
    pub fn terminal(self) -> bool {
        !matches!(self, Self::Pending | Self::Running)
    }
}
