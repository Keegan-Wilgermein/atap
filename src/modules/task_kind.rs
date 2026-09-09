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
/// An enum rather than a flag because there turned out to be
/// four of these. Three of them are one task going round in
/// its own slot, and the fourth isn't a task at all
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

    /// Starts a fresh run on the interval whatever the last
    /// one is doing, until it is cancelled
    ///
    /// The odd one out. Its slot holds no task and is never
    /// queued or run — what it holds is the prototype every
    /// run is cloned from, and a place for whichever run
    /// finished most recently to leave its output. The runs
    /// themselves are ordinary one shot tasks in slots of
    /// their own, which is what lets them overlap at all
    Series = 3,
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
            3 => Self::Series,
            _ => Self::Once,
        }
    }

    /// Whether a run of this should be followed by another
    ///
    /// Which is also what makes `Ready` and `Taken` transient
    /// rather than the end of the story, so it is asked
    /// wherever something is about to treat a settled state as
    /// a finished one
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

    /// Whether this is a schedule rather than a task
    ///
    /// A `Series` slot never reaches a worker, so everything
    /// that walks a queue or claims a task can ignore it. What
    /// it does instead is answered entirely by the manager
    #[inline(always)]
    pub(crate) fn schedules(self) -> bool {
        self == Self::Series
    }
}
