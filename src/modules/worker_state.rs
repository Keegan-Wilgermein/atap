//! # Worker State
//! Where a worker is, from the moment its slot in the pool is
//! claimed to the moment the slot is given back
//!
//! Doubles as the address the worker parks on, so it is a
//! `u32` for the same reason `TaskState` is: the kernel's
//! address wait only watches words of 4 or 8 bytes

/// The lifecycle of one worker
///
/// #### Note
/// `Empty` and `Dead` are not the same thing and the
/// difference is what makes recovery work. `Empty` is a slot
/// nobody is using, `Dead` is a slot whose thread went down
/// with a task and a queue still in it. One is free to claim,
/// the other has to be cleaned up first
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum WorkerState {
    /// No worker in this slot
    ///
    /// Zero on purpose, so the whole pool starts out empty
    /// with nothing needing to be written to it first
    Empty = 0,

    /// The slot is claimed but its thread isn't up yet
    ///
    /// Claiming before spawning is what stops two threads
    /// starting a worker into the same slot
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
    /// Its ring still holds every task it hadn't got to, and
    /// those are redelegated rather than lost. Only the one
    /// task it had already claimed is beyond saving
    Dead = 6,
}

impl WorkerState {
    /// Rebuilds a state from the raw value in the atomic
    ///
    /// Anything unrecognised is treated as `Dead`, since a
    /// state this crate didn't write means the worker can't
    /// be trusted and the safe reading is that it needs
    /// cleaning up
    #[inline(always)]
    pub(crate) fn from_u32(raw: u32) -> Self {
        match raw {
            0 => Self::Empty,
            1 => Self::Starting,
            2 => Self::Idle,
            3 => Self::Running,
            4 => Self::Parked,
            5 => Self::Stopping,
            _ => Self::Dead,
        }
    }

    /// Whether a thread is meant to be behind this slot
    ///
    /// `Dead` says one was and isn't any more, which is a
    /// different question and the one `needs_recovery` asks
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

    /// Whether this slot has a thread's mess left in it
    #[inline(always)]
    pub(crate) fn needs_recovery(self) -> bool {
        self == Self::Dead
    }
}
