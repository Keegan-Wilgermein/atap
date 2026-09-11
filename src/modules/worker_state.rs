//! # Worker State
//! Where a worker is, from its slot being claimed to the slot
//! being given back
//!
//! A `u32` because workers park on it, and the kernel's
//! address wait only watches words of 4 or 8 bytes

/// The lifecycle of one worker
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WorkerState {
    /// No worker in this slot
    Empty = 0,

    /// Claimed, but its thread isn't up yet
    ///
    /// Claiming before spawning stops two threads starting a
    /// worker into the same slot
    Starting = 1,

    /// Awake and looking for something to do
    Idle = 2,

    /// Running a task right now
    Running = 3,

    /// Asleep on its own state word, waiting to be woken
    Parked = 4,

    /// Asked to stop, and will once it has put down whatever
    /// it is holding
    Stopping = 5,

    /// The thread went down without being asked to
    ///
    /// Its queue goes back to the pool. Only the task it was
    /// running is lost
    Dead = 6,

    /// A `Dead` slot somebody is already clearing up
    ///
    /// Stops two threads recovering the same slot
    Recovering = 7,
}

impl WorkerState {
    /// Rebuilds a state from the raw value in the atomic
    ///
    /// Anything unrecognised reads as `Dead`, so it gets cleaned
    /// up
    #[inline(always)]
    pub(crate) fn from_u32(raw: u32) -> Self {
        match raw {
            0 => Self::Empty,
            1 => Self::Starting,
            2 => Self::Idle,
            3 => Self::Running,
            4 => Self::Parked,
            5 => Self::Stopping,
            7 => Self::Recovering,
            _ => Self::Dead,
        }
    }

    /// Whether a thread is meant to be behind this slot
    #[inline(always)]
    pub(crate) fn alive(self) -> bool {
        matches!(
            self,
            Self::Starting | Self::Idle | Self::Running | Self::Parked | Self::Stopping
        )
    }

    /// Whether the worker is on a task right now
    #[inline(always)]
    pub(crate) fn busy(self) -> bool {
        self == Self::Running
    }

    /// Whether a dead thread's slot needs clearing up
    #[inline(always)]
    pub(crate) fn needs_recovery(self) -> bool {
        self == Self::Dead
    }
}
