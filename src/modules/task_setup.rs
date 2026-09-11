//! # Task Setup
//! Everything decided about a task before it runs

use crate::{constants::DEFAULT_PRIORITY, modules::task_kind::TaskKind};
use std::time::{Duration, Instant};

/// When a series should stop, if it should
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
    /// Whether it runs once, forever, forever with a gap, or on a
    /// schedule
    pub(crate) kind: TaskKind,

    /// The gap between runs, read by `RepeatEvery`, or the
    /// period between them, read by `Series`
    pub(crate) interval: Duration,

    /// How long to wait before the first run
    pub(crate) start_delay: Duration,

    /// When to stop repeating, if at all
    pub(crate) deadline: Deadline,

    /// Runs still allowed, or `u32::MAX` for no limit
    pub(crate) runs: u32,

    /// The class it is served at
    pub(crate) priority: u8,

    /// Whether it wants a thread it can block
    ///
    /// Filled in at spawn from `Task::blocking`
    pub(crate) blocking: bool,
}

impl Default for TaskSetup {
    /// A task that runs once, now, at the default priority
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

    /// The moment the deadline falls on
    ///
    /// A span is measured from when the repeating starts, after any
    /// start delay. One too long for the clock means no deadline
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
