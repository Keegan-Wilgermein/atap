//! # Task Setup
//! Everything decided about a task before it runs
//!
//! Spawning has grown a lot of small decisions — what priority
//! it goes in at, whether it runs once or forever, how long it
//! waits between runs, which half of the pool it belongs to.
//! Carrying them as one thing keeps the signatures readable and
//! means a new decision is a field rather than another argument
//! threaded through three call sites

use crate::{constants::DEFAULT_PRIORITY, modules::task_kind::TaskKind};
use std::time::{Duration, Instant};

/// When a series should stop, if it should
///
/// An enum rather than two fields because the two ways of
/// saying it are mutually exclusive and only one can be stored.
/// `for_duration` gives a span and `until` gives a moment, and
/// a series handed both would have to ignore one of them
///
/// #### Note
/// The span isn't resolved into a moment until the task is
/// spawned, and deliberately so. It is measured from when the
/// *repeating* starts, which isn't known until the start delay
/// is — and the delay can only be set after the deadline, since
/// `after` closes the bound axes behind it
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Deadline {
    /// Runs until it is cancelled
    None,

    /// Runs for this long once the repeating has started
    Span(Duration),

    /// Runs until this moment
    At(Instant),
}

/// How a task should be scheduled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TaskSetup {
    /// Whether it runs once, forever, forever with a gap, or
    /// on a schedule of its own
    pub(crate) kind: TaskKind,

    /// The gap between runs, read by `RepeatEvery`, or the
    /// period between them, read by `Series`
    pub(crate) interval: Duration,

    /// How long to wait before the first run
    ///
    /// Kept apart from `interval` rather than sharing it,
    /// because a delayed repeat has both — a delay before it
    /// starts and a gap between the runs after that — and one
    /// field could only ever hold one of them
    pub(crate) start_delay: Duration,

    /// When to stop repeating, if at all
    pub(crate) deadline: Deadline,

    /// Runs still allowed, or `u32::MAX` for no limit
    ///
    /// A sentinel rather than an `Option<u32>` so the slot can
    /// hold it in a plain `AtomicU32`. A series asked for
    /// `u32::MAX` runs is indistinguishable from one asked to
    /// run forever anyway
    pub(crate) runs: u32,

    /// The class it is served at
    pub(crate) priority: u8,

    /// Whether it wants a thread it can block
    ///
    /// Filled in by the `Executor` rather than by the caller,
    /// since it is the task itself that answers this and the
    /// `Executor` is the last thing to hold it before the type
    /// is erased
    pub(crate) blocking: bool,
}

impl Default for TaskSetup {
    /// A task that runs once, now, at the default priority
    ///
    /// Every field has a default because the builder
    /// accumulates rather than replaces — entering a state
    /// changes only the fields that state is about, and leaves
    /// the rest as they were
    fn default() -> Self {
        Self {
            kind: TaskKind::Once,
            interval: Duration::ZERO,
            start_delay: Duration::ZERO,
            deadline: Deadline::None,
            runs: u32::MAX,
            priority: DEFAULT_PRIORITY,
            blocking: false,
        }
    }
}

impl TaskSetup {
    /// A task that runs once and settles
    pub(crate) fn once(priority: u8) -> Self {
        Self {
            kind: TaskKind::Once,
            interval: Duration::ZERO,
            start_delay: Duration::ZERO,
            deadline: Deadline::None,
            runs: u32::MAX,
            priority,
            blocking: false,
        }
    }

    /// Turns the deadline into the moment it actually falls on
    ///
    /// ## Behaviour
    /// A span is measured from when the repeating starts, which
    /// is the start delay from now — so a task delayed a second
    /// and bounded to five gets five seconds of repeating
    /// rather than four
    ///
    /// #### Note
    /// A span so long that the clock can't hold the answer
    /// comes back as no deadline at all. Asking to repeat for
    /// longer than a monotonic clock can express is asking to
    /// repeat forever, and that is what it gets
    pub(crate) fn until(&self) -> Option<Instant> {
        match self.deadline {
            Deadline::None => None,
            Deadline::At(when) => Some(when),
            Deadline::Span(span) => Instant::now()
                .checked_add(self.start_delay)
                .and_then(|start| start.checked_add(span)),
        }
    }

    /// Records what the task said about wanting to block
    pub(crate) fn blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }
}
