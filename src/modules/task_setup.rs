//! # Task Setup
//! Everything decided about a task before it runs
//!
//! Spawning has grown a lot of small decisions — what priority
//! it goes in at, whether it runs once or forever, how long it
//! waits between runs, which half of the pool it belongs to.
//! Carrying them as one thing keeps the signatures readable and
//! means a new decision is a field rather than another argument
//! threaded through three call sites

use crate::modules::task_kind::TaskKind;
use std::time::Duration;

/// How a task should be scheduled
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TaskSetup {
    /// Whether it runs once, forever, or forever with a gap
    pub(crate) kind: TaskKind,

    /// The gap, which only `RepeatEvery` reads
    pub(crate) interval: Duration,

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

impl TaskSetup {
    /// A task that runs once and settles
    pub(crate) fn once(priority: u8) -> Self {
        Self {
            kind: TaskKind::Once,
            interval: Duration::ZERO,
            priority,
            blocking: false,
        }
    }

    /// A task that runs again the moment it finishes
    pub(crate) fn repeating(priority: u8) -> Self {
        Self {
            kind: TaskKind::Repeating,
            interval: Duration::ZERO,
            priority,
            blocking: false,
        }
    }

    /// A task that waits out an interval between runs
    pub(crate) fn every(priority: u8, interval: Duration) -> Self {
        Self {
            kind: TaskKind::RepeatEvery,
            interval,
            priority,
            blocking: false,
        }
    }

    /// Records what the task said about wanting to block
    pub(crate) fn blocking(mut self, blocking: bool) -> Self {
        self.blocking = blocking;
        self
    }
}
