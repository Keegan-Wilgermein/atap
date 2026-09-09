//! # Task Kind
//! What a slot does when the run it is holding comes to an end
//!
//! A task that runs once and a task that runs until it is
//! stopped are the same thing right up to the last moment of
//! the run, so they share a slot, a handle and every path
//! through the `Executor`. This is the one bit of the slot that
//! tells them apart

/// How a slot behaves once its run has finished
///
/// #### Note
/// An enum rather than a flag because there are more of these
/// coming. `repeat_every` waits an interval between runs and
/// `every` starts them on a schedule whether the last one
/// finished or not, and both are decided in the same place
/// this one is
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum TaskKind {
    /// Runs once and settles
    ///
    /// Zero on purpose, so a slot that nothing has said
    /// anything about is an ordinary task, the same way
    /// `TaskState::Free` being zero makes an untouched block
    /// read as empty slots
    Once = 0,

    /// Runs again the moment it finishes, until it is cancelled
    Repeating = 1,

    /// Waits out an interval between runs, until it is
    /// cancelled
    ///
    /// The wait costs no thread. The task goes back in its slot
    /// and a timer on the manager's queue puts it back on the
    /// worker queue when the interval is up
    RepeatEvery = 2,
}

impl TaskKind {
    /// Rebuilds a kind from the raw value in the slot
    ///
    /// Anything unrecognised is treated as `Once`, since a task
    /// that settles is the safe reading of a byte this crate
    /// didn't write. A wrong `Repeating` would run something
    /// forever that was never meant to run twice
    #[inline(always)]
    pub(crate) fn from_u8(raw: u8) -> Self {
        match raw {
            1 => Self::Repeating,
            2 => Self::RepeatEvery,
            _ => Self::Once,
        }
    }

    /// Whether a run of this should be followed by another
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
}
