//! # Gate
//! Whether a give starts a waiting task's next run, and what
//! follows once a run, or a series of runs, is over

use crate::modules::task_setup::Deadline;
use std::{
    sync::atomic::{AtomicU8, AtomicU32, AtomicUsize, Ordering},
    time::{Duration, Instant},
};

/// Nothing is owed, and the task sits in its slot until a give
const WAITING: u8 = 0;

/// A give was taken, and the run it starts is queued or delayed
const QUEUED: u8 = 1;

/// A run, or a series of them, is under way
const RUNNING: u8 = 2;

/// A single run is under way, and a give that landed during it is
/// owed one more
const PENDING: u8 = 3;

/// No more gives are taken
const FINISHED: u8 = 4;

/// What a give did
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Trigger {
    /// It starts the next run, or the next series
    Start,

    /// It only replaced the value the next run is handed
    Replaced,

    /// The task takes no more gives
    Closed,
}

/// What follows a run, or a series, that is over
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AfterRun {
    /// Nothing is owed, so the task waits for a give
    Wait,

    /// A give landed during the run, so the next one starts now
    Again,

    /// No more gives are coming, so the task is over
    Finish,
}

/// The shared half of a task that waits for gives
pub(crate) struct Gate {
    /// Where the task is between gives, as one of the constants above
    state: AtomicU8,

    /// Gives still taken, or `u32::MAX` for no limit
    gives_left: AtomicU32,

    /// Handles that can still give to the task
    ///
    /// Once none are left and nothing is owed, nothing can ever start
    /// the task again
    givers: AtomicUsize,

    /// Runs each series is allowed, or `u32::MAX` for no limit
    runs: u32,

    /// When each series stops repeating, if it does
    deadline: Deadline,

    /// Nanoseconds each give waits before its run or series starts
    delay: u64,

    /// Whether a give starts a series rather than a single run
    ///
    /// A give during a series only replaces the value. One during a
    /// single run is owed a run of its own
    series: bool,
}

impl Gate {
    /// A gate with one handle giving to it, and nothing owed
    pub(crate) fn new(
        gives: u32,
        runs: u32,
        deadline: Deadline,
        delay: Duration,
        series: bool,
    ) -> Self {
        Self {
            state: AtomicU8::new(WAITING),
            gives_left: AtomicU32::new(gives),
            givers: AtomicUsize::new(1),
            runs,
            deadline,
            delay: delay.as_nanos().min(u64::MAX as u128) as u64,
            series,
        }
    }

    /// A gate for a handle to no task, which takes nothing
    pub(crate) fn detached() -> Self {
        Self {
            state: AtomicU8::new(FINISHED),
            gives_left: AtomicU32::new(0),
            givers: AtomicUsize::new(1),
            runs: u32::MAX,
            deadline: Deadline::None,
            delay: 0,
            series: false,
        }
    }

    /// Records another handle that can give
    #[inline(always)]
    pub(crate) fn add_giver(&self) {
        self.givers.fetch_add(1, Ordering::SeqCst);
    }

    /// Records that a handle that could give is gone
    ///
    /// ## Returns
    /// Whether it was the last one
    #[inline(always)]
    pub(crate) fn drop_giver(&self) -> bool {
        self.givers.fetch_sub(1, Ordering::SeqCst) == 1
    }

    /// Runs each series is allowed
    #[inline(always)]
    pub(crate) fn runs(&self) -> u32 {
        self.runs
    }

    /// Nanoseconds each give waits before its run or series starts
    #[inline(always)]
    pub(crate) fn delay(&self) -> u64 {
        self.delay
    }

    /// When a series starting now, once its delay is up, stops
    /// repeating
    ///
    /// A span is counted from the start of each series, so every
    /// series gets the whole of it
    pub(crate) fn until(&self) -> Option<Instant> {
        match self.deadline {
            Deadline::None => None,
            Deadline::At(when) => Some(when),
            Deadline::Span(span) => Instant::now()
                .checked_add(Duration::from_nanos(self.delay))
                .and_then(|start| start.checked_add(span)),
        }
    }

    /// Whether nothing is owed and the task waits for a give
    #[inline(always)]
    pub(crate) fn waiting(&self) -> bool {
        self.state.load(Ordering::SeqCst) == WAITING
    }

    /// Whether the task takes no more gives
    #[inline(always)]
    pub(crate) fn finished(&self) -> bool {
        self.state.load(Ordering::SeqCst) == FINISHED
    }

    /// Says the task takes no more gives, whatever it was doing
    #[inline(always)]
    pub(crate) fn finish(&self) {
        self.state.store(FINISHED, Ordering::SeqCst);
    }

    /// Takes a give, whose value has already been left for the run
    pub(crate) fn trigger(&self) -> Trigger {
        loop {
            let state = self.state.load(Ordering::SeqCst);

            let to = match state {
                WAITING => QUEUED,
                RUNNING if !self.series => PENDING,
                QUEUED | PENDING | RUNNING => return Trigger::Replaced,
                _ => return Trigger::Closed,
            };

            // Only a give that starts something counts against the count
            if self.gives_left.load(Ordering::Acquire) == 0 {
                return Trigger::Closed;
            }

            if self
                .state
                .compare_exchange(state, to, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                self.spend_give();

                return match to {
                    QUEUED => Trigger::Start,
                    _ => Trigger::Replaced,
                };
            }
        }
    }

    /// Takes one off the gives left, unless there is no limit
    ///
    /// Only whoever won the move out of `WAITING` or `RUNNING` calls
    /// this, so no two callers race for the same give
    fn spend_give(&self) {
        let _ =
            self.gives_left
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |left| match left {
                    u32::MAX | 0 => None,
                    left => Some(left - 1),
                });
    }

    /// Moves a queued run into running, at the start of each run
    ///
    /// A run of a series already under way leaves it alone
    #[inline(always)]
    pub(crate) fn begin(&self) {
        let _ = self
            .state
            .compare_exchange(QUEUED, RUNNING, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// Decides what follows a run, or a series, that is over
    ///
    /// The task must already be back in its slot, so a give that
    /// starts the next run finds it there
    pub(crate) fn after_run(&self) -> AfterRun {
        loop {
            let state = self.state.load(Ordering::SeqCst);

            match state {
                PENDING => {
                    if self
                        .state
                        .compare_exchange(PENDING, QUEUED, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        return AfterRun::Again;
                    }
                }

                QUEUED | RUNNING => {
                    let over = self.gives_left.load(Ordering::Acquire) == 0 || !self.has_givers();

                    let to = match over {
                        true => FINISHED,
                        false => WAITING,
                    };

                    if self
                        .state
                        .compare_exchange(state, to, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        continue;
                    }

                    if over {
                        return AfterRun::Finish;
                    }

                    // The last giver may have gone between the look above and
                    // the move. It saw a run under way and left the ending to
                    // this side, so it is looked at again
                    if !self.has_givers() && self.close_waiting() {
                        return AfterRun::Finish;
                    }

                    return AfterRun::Wait;
                }

                WAITING => return AfterRun::Wait,

                _ => return AfterRun::Finish,
            }
        }
    }

    /// Closes a gate with nothing owed on it, for a cancel or the
    /// last giver going
    ///
    /// ## Returns
    /// Whether this caller closed it, and so has to let the task go.
    /// `false` means a run is under way, and its end will
    pub(crate) fn close_waiting(&self) -> bool {
        self.state
            .compare_exchange(WAITING, FINISHED, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Whether any handle can still give
    #[inline(always)]
    fn has_givers(&self) -> bool {
        self.givers.load(Ordering::SeqCst) != 0
    }
}
