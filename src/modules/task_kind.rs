//! # Task Kind
//! What a slot does when the run it is holding comes to an end

/// How a slot behaves once its run has finished
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TaskKind {
    /// Runs once and settles
    ///
    /// Zero, so a slot nothing has written to is an ordinary task
    Once = 0,

    /// Runs again the moment it finishes, until it is cancelled
    Repeating = 1,

    /// Waits out an interval between runs, until it is cancelled
    ///
    /// The wait is a timer on the manager's queue, not a thread
    RepeatEvery = 2,

    /// Starts a fresh run on the interval whatever the last one is
    /// doing, until it is cancelled
    ///
    /// Its own slot is never run. It holds the prototype each run
    /// is cloned from, and the output of the latest run to finish
    Series = 3,
}

impl TaskKind {
    /// Rebuilds a kind from the raw value in the slot
    ///
    /// Anything unrecognised reads as `Once`
    #[inline(always)]
    pub(crate) fn from_u8(raw: u8) -> Self {
        match raw {
            1 => Self::Repeating,
            2 => Self::RepeatEvery,
            3 => Self::Series,
            _ => Self::Once,
        }
    }

    /// Whether a run of this is followed by another
    #[inline(always)]
    pub(crate) fn repeats(self) -> bool {
        self != Self::Once
    }
}

impl TaskKind {
    /// Whether a run of this is followed by a wait rather than
    /// by the next run
    #[inline(always)]
    pub(crate) fn waits(self) -> bool {
        self == Self::RepeatEvery
    }

    /// Whether this is a schedule, whose slot never reaches a
    /// worker
    #[inline(always)]
    pub(crate) fn schedules(self) -> bool {
        self == Self::Series
    }
}
