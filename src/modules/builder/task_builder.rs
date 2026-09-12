//! # Task Builder
//! Building a task up before it is spawned
//!
//! What may be chained is decided by the type, so a combination
//! that makes no sense doesn't compile. Entering a state never
//! clears anything already set
//!
//! ```ignore
//! Runtime::task(t).repeat().every(gap).until(when).spawn();
//! Runtime::task(t).repeat().count(10).after(delay).spawn();
//! Runtime::task(t).at_rate(period).for_duration(span).spawn();
//! ```
//!
//! And ones that don't build:
//!
//! ```ignore
//! Runtime::task(t).repeat().at_rate(period);      // a kind is already chosen
//! Runtime::task(t).at_rate(period).every(gap);    // `every` belongs to `Repeat`
//! Runtime::task(t).repeat().for_duration(s).until(t);  // one deadline
//! Runtime::task(t).repeat().after(d).count(10);   // `after` closed the bounds
//! ```

use crate::{
    executor::Executor,
    futures::task::Task,
    modules::{
        task_handle::TaskHandle,
        task_kind::TaskKind,
        task_setup::{Deadline, TaskSetup},
    },
};
use super::builder_markers::{Once, Open, Rate, Repeat, Repeatable, Set};
use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

/// A task being built up before it is spawned
///
/// Nothing runs until `spawn` is called, so a dropped builder
/// starts nothing
///
/// - `K` — the kind: `Once`, `Repeat` or `Rate`
/// - `D` — whether a deadline has been set
/// - `C` — whether a run count has been set
pub struct TaskBuilder<F, K = Once, D = Open, C = Open>
where
    F: Task,
{
    task: F,
    setup: TaskSetup,

    _state: PhantomData<(K, D, C)>,
}

impl<F> TaskBuilder<F, Once, Open, Open>
where
    F: Task,
{
    /// A task that will run once, at the default priority
    pub(crate) fn new(task: F) -> Self {
        Self {
            task,
            setup: TaskSetup::default(),
            _state: PhantomData,
        }
    }
}

impl<F, K, D, C> TaskBuilder<F, K, D, C>
where
    F: Task,
{
    /// Sets the priority the task is served at
    ///
    /// Higher is more urgent, and `DEFAULT_PRIORITY` sits halfway.
    /// Priority decides the order tasks are started in, not how
    /// much of a thread they get once running
    pub fn priority(mut self, priority: u8) -> Self {
        self.setup.priority = priority;
        self
    }

    /// Waits out a delay before the first run
    ///
    /// On a repeat it delays only the first run, not the gaps
    /// after it
    ///
    /// Closes `count`, `for_duration` and `until`, so set those
    /// first: `.repeat().count(10).after(d)`
    ///
    /// #### Note
    /// A delay still waiting when the runtime shuts down is written
    /// off, and its handle reads `TaskFailed`. One that spans a
    /// manager restart runs late, never early
    pub fn after(mut self, delay: Duration) -> TaskBuilder<F, K, Set, Set> {
        self.setup.start_delay = delay;
        self.moved()
    }

    /// Carries everything set so far into new states
    fn moved<K2, D2, C2>(self) -> TaskBuilder<F, K2, D2, C2> {
        TaskBuilder {
            task: self.task,
            setup: self.setup,
            _state: PhantomData,
        }
    }
}

impl<F, D, C> TaskBuilder<F, Once, D, C>
where
    F: Task,
{
    /// Runs again as soon as the last run finishes
    ///
    /// Runs never overlap. With no `every`, the next run is queued
    /// the moment the last one publishes, and without a `count`,
    /// `for_duration` or `until` it runs until cancelled
    pub fn repeat(mut self) -> TaskBuilder<F, Repeat, D, C> {
        self.setup.kind = TaskKind::Repeating;
        self.moved()
    }

    /// Starts a run every `period`, whether the last one has
    /// finished or not
    ///
    /// The period is start to start, so a 200ms task on a 50ms
    /// period has four runs in flight at once.
    /// `repeat().every(gap)` waits between runs instead
    ///
    /// #### Note
    /// Nothing pushes back, so a task slower than its period piles
    /// runs up behind each other. Runs finishing together publish
    /// one output between them, and periods missed while the
    /// manager restarts are skipped rather than made up
    pub fn at_rate(mut self, period: Duration) -> TaskBuilder<F, Rate, D, C> {
        self.setup.kind = TaskKind::Series;
        self.setup.interval = period;
        self.moved()
    }
}

impl<F, D, C> TaskBuilder<F, Repeat, D, C>
where
    F: Task,
{
    /// Waits out a gap between runs
    ///
    /// The gap is from the end of one run to the start of the
    /// next, so runs never overlap. The wait holds no thread
    ///
    /// #### Note
    /// A gap can come out long under load or across a manager
    /// restart, but never short. A cancel is seen by readers at
    /// once, but the slot isn't freed until the current gap is up
    pub fn every(mut self, gap: Duration) -> Self {
        self.setup.kind = TaskKind::RepeatEvery;
        self.setup.interval = gap;
        self
    }
}

impl<F, K, C> TaskBuilder<F, K, Open, C>
where
    F: Task,
    K: Repeatable,
{
    /// Stops repeating once `span` has passed
    ///
    /// Measured from when the repeating starts, so a delay set with
    /// `after` doesn't eat into it
    ///
    /// ## Behaviour
    /// A run that would start past the deadline is never started,
    /// so a 750ms gap bounded to 1s runs at 0ms and 750ms, then
    /// stops. The last output stays readable, and `is_finished`
    /// reads true
    pub fn for_duration(mut self, span: Duration) -> TaskBuilder<F, K, Set, C> {
        self.setup.deadline = Deadline::Span(span);
        self.moved()
    }

    /// Stops repeating at `when`
    ///
    /// A run that would start past it is never started, and a
    /// moment already past means one run. Can't be combined with
    /// `for_duration`
    pub fn until(mut self, when: Instant) -> TaskBuilder<F, K, Set, C> {
        self.setup.deadline = Deadline::At(when);
        self.moved()
    }
}

impl<F, K, D> TaskBuilder<F, K, D, Open>
where
    F: Task,
    K: Repeatable,
{
    /// Stops repeating after `runs` runs
    ///
    /// ## Behaviour
    /// Combines with a deadline, ending at whichever comes first.
    /// The last output stays readable, and `is_finished` reads true
    ///
    /// #### Note
    /// A count of zero still runs a `repeat` once. On `at_rate` it
    /// starts nothing, and the handle reads `TaskFailed`
    pub fn count(mut self, runs: u32) -> TaskBuilder<F, K, D, Set> {
        self.setup.runs = runs;
        self.moved()
    }
}

impl<F, D, C> TaskBuilder<F, Once, D, C>
where
    F: Task,
{
    /// Starts the task and gives back its handle
    ///
    /// Never blocks. A spawn after the runtime has shut down gives
    /// a handle that reads `TaskFailed`
    pub fn spawn(self) -> TaskHandle<F::Output> {
        Executor::new_task(self.task, self.setup)
    }
}

impl<F, D, C> TaskBuilder<F, Repeat, D, C>
where
    F: Task,
{
    /// Starts the repeat and gives back its handle
    ///
    /// One handle for the whole series. `join` gives the latest
    /// output, `take` moves one out for the next run to replace,
    /// and `cancel` ends the series
    pub fn spawn(self) -> TaskHandle<F::Output> {
        Executor::new_task(self.task, self.setup)
    }
}

impl<F, D, C> TaskBuilder<F, Rate, D, C>
where
    F: Task + Clone,
{
    /// Starts the schedule and gives back its handle
    ///
    /// The first run goes now, not a period from now. Each run is a
    /// fresh clone of the task, which is why this needs `Clone`
    pub fn spawn(self) -> TaskHandle<F::Output> {
        Executor::new_series(self.task, self.setup)
    }
}

#[cfg(test)]
mod type_checks {
    //! Chains that must build, checked by compiling them

    use super::*;
    use crate::{Sleep, SleepMode, SleepTask};

    fn task() -> SleepTask {
        Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed)
    }

    #[allow(dead_code)]
    fn one_shots() {
        let _ = TaskBuilder::new(task()).spawn();
        let _ = TaskBuilder::new(task()).priority(200).spawn();
        let _ = TaskBuilder::new(task()).after(Duration::ZERO).spawn();
        let _ = TaskBuilder::new(task())
            .after(Duration::ZERO)
            .priority(200)
            .spawn();
    }

    #[allow(dead_code)]
    fn repeats() {
        let _ = TaskBuilder::new(task()).repeat().spawn();
        let _ = TaskBuilder::new(task())
            .repeat()
            .every(Duration::ZERO)
            .spawn();
        let _ = TaskBuilder::new(task()).repeat().count(10).spawn();
        let _ = TaskBuilder::new(task())
            .repeat()
            .for_duration(Duration::ZERO)
            .spawn();
        let _ = TaskBuilder::new(task()).repeat().until(Instant::now()).spawn();
    }

    /// A count and a deadline compose, in either order
    #[allow(dead_code)]
    fn both_bounds() {
        let _ = TaskBuilder::new(task())
            .repeat()
            .count(10)
            .for_duration(Duration::ZERO)
            .spawn();

        let _ = TaskBuilder::new(task())
            .repeat()
            .until(Instant::now())
            .count(10)
            .spawn();
    }

    /// `after` closes the bounds and leaves the kind alone
    #[allow(dead_code)]
    fn delays() {
        let _ = TaskBuilder::new(task())
            .repeat()
            .count(10)
            .after(Duration::ZERO)
            .spawn();

        let _ = TaskBuilder::new(task())
            .repeat()
            .after(Duration::ZERO)
            .every(Duration::ZERO)
            .spawn();

        let _ = TaskBuilder::new(task())
            .after(Duration::ZERO)
            .repeat()
            .every(Duration::ZERO)
            .spawn();
    }

    #[allow(dead_code)]
    fn schedules() {
        let _ = TaskBuilder::new(task()).at_rate(Duration::ZERO).spawn();
        let _ = TaskBuilder::new(task())
            .at_rate(Duration::ZERO)
            .count(10)
            .spawn();
        let _ = TaskBuilder::new(task())
            .at_rate(Duration::ZERO)
            .for_duration(Duration::ZERO)
            .priority(200)
            .spawn();
    }
}
