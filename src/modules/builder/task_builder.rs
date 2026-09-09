//! # Task Builder
//! Building a task up before it is handed to the `Executor`
//!
//! Everything a task can be asked for is a chain from
//! `Runtime::task`, and what may be chained is decided by the
//! type rather than by a check at spawn. A combination that
//! makes no sense is a combination that doesn't compile
//!
//! ## The model
//! Methods that move the builder into a state are always
//! available; a state's own methods are available only while
//! you are in it. Entering a state **never clears anything** —
//! the `TaskSetup` accumulates and every field has a default,
//! so all that changes is the marker, and with it what may be
//! called next
//!
//! ```ignore
//! Runtime::task(t).repeat().every(gap).until(when).spawn();
//! Runtime::task(t).repeat().count(10).after(delay).spawn();
//! Runtime::task(t).at_rate(period).for_duration(span).spawn();
//! ```
//!
//! And the ones that don't build:
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
/// Nothing has happened yet. The task is held here, inert, and
/// only reaches the `Executor` when `spawn` is called — so a
/// builder that is dropped instead starts nothing
///
/// ## The parameters
/// - `K` — the kind: `Once`, `Repeat` or `Rate`
/// - `D` — whether a deadline has been set
/// - `C` — whether a run count has been set
///
/// Three rather than one, because the three decisions are
/// independent and a single marker would need a variant for
/// every reachable combination of them
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
    ///
    /// Reached through `Runtime::task`
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
    /// Higher is more urgent. `DEFAULT_PRIORITY` sits halfway
    /// up, so there is as much room to put a task below what a
    /// plain spawn gives it as to lift one above
    ///
    /// Available from every state and changes none of them —
    /// priority is not a scheduling decision, it is the order
    /// the pool serves what it has been given
    ///
    /// #### Note
    /// Priority decides the order tasks are *started* in, not
    /// how much of a thread they get once they are running
    pub fn priority(mut self, priority: u8) -> Self {
        self.setup.priority = priority;
        self
    }

    /// Waits out a delay before the first run
    ///
    /// Available from every state, and composes with all of
    /// them: on a one shot it is a task that runs later, and on
    /// a repeat it delays the first run without touching the
    /// gap between the ones after it
    ///
    /// ## What it closes
    /// Both bound axes, so `count`, `for_duration` and `until`
    /// are gone after it. The kind is left alone, which is why
    /// `.repeat().after(d).every(gap)` still builds
    ///
    /// Put the bounds before the delay if you want both:
    /// `.repeat().count(10).after(d)`
    /// ## If the manager goes
    /// The delay is a timer on the manager's queue, so this
    /// notices.
    ///
    /// **Away and coming back:** the run is *late*, not lost.
    /// The slot says a wake is owed until something acts on it,
    /// so a manager coming back arms a fresh timer for every
    /// delay still outstanding. A delay that spans a restart
    /// runs long, by however long the manager was away — never
    /// short
    ///
    /// **Gone for good:** the task is written off rather than
    /// left waiting on a queue that has closed, so the handle
    /// settles and every reader gets an answer. One spawned
    /// after that point settles `Failed` straight away, since
    /// the timer it needs can't be armed at all
    ///
    /// ## If the runtime is shut down
    /// A delay that hasn't fired is written off rather than
    /// waited for. A task sitting on a timer holds no thread
    /// and sits in no queue, so the drain can't see it — and a
    /// shutdown that blocked for an hour because something was
    /// scheduled an hour out would be no use to anybody
    ///
    pub fn after(mut self, delay: Duration) -> TaskBuilder<F, K, Set, Set> {
        self.setup.start_delay = delay;
        self.moved()
    }

    /// Carries everything decided so far into new states
    ///
    /// The whole of what a transition is. Nothing is reset and
    /// nothing is recomputed — only the markers change, and
    /// with them what may be called next
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
    /// A run finishes before the next one starts, always. There
    /// is no interval and no clock: the moment a run publishes
    /// its output the task goes back on the queue, behind
    /// whatever else is waiting, so it takes a share of the
    /// pool rather than a thread of it
    ///
    /// Give it a gap with `every`, and an ending with `count`,
    /// `for_duration` or `until`. Without one of those it runs
    /// until it is cancelled
    ///
    /// #### Note
    /// Reachable only from `Once`, which is what makes it
    /// exclusive with `at_rate` — a task cannot be both
    /// sequential and overlapping, and the type says so
    /// ## If the manager goes
    /// Nothing, and nothing is skipped either. There is no
    /// clock here and no timer: a run puts itself straight back
    /// on the pool as its last act, so this is the one repeat
    /// that never involved the manager and the only one that
    /// survives it giving up for good
    ///
    /// ## If the runtime is shut down
    /// The series ends. Surviving a manager that died is not
    /// the same as surviving a pool that has been stopped —
    /// putting itself back on the queue is exactly the move
    /// that fails once nothing is accepting work, so the run in
    /// flight finishes and the series settles after it
    ///
    pub fn repeat(mut self) -> TaskBuilder<F, Repeat, D, C> {
        self.setup.kind = TaskKind::Repeating;
        self.moved()
    }

    /// Starts a run on the period, whether the last one has
    /// finished or not
    ///
    /// The interval is the *period*, not the gap. A task taking
    /// 200ms on a 50ms period has four of itself in flight at
    /// once and still starts a fifth on time
    ///
    /// The clock is the kernel's. One repeating timer is armed
    /// at the start and left alone, so the cadence never drifts
    /// with how long a run took or how busy the pool was
    ///
    /// ## Against `repeat().every(gap)`
    /// `every` is the gap *between* runs, so a 200ms task on
    /// a 50ms gap runs every 250ms and never overlaps. `at_rate`
    /// is the period, so the same task on a 50ms period starts
    /// every 50ms and overlaps four deep
    ///
    /// #### Note
    /// Each run is a fresh copy of the task, which is what lets
    /// runs overlap at all — and why `spawn` asks for `Clone`
    /// here where the other kinds don't
    /// #### Note
    /// Runs pile up if the pool can't keep up. The period is
    /// kept whatever else is happening, so a task that takes
    /// longer than its period leaves runs queued behind each
    /// other and nothing pushes back. That is what a fixed rate
    /// means — check before putting a slow task on a short one
    ///
    /// #### Note
    /// Runs overlap, so "most recent" is as precise as the
    /// order they happened to finish in. Two landing together
    /// publish one output between them and the other is dropped
    ///
    /// ## If the manager goes
    /// The clock is a timer on the manager's queue, so this is
    /// the one that notices most.
    ///
    /// **Away and coming back: periods are skipped.** A
    /// repeating timer that goes off while nobody is reading
    /// the queue is folded into one wake carrying a count, and
    /// one wake starts one run. A schedule on a 20ms period
    /// through a 400ms absence therefore starts a single run
    /// when the manager returns, not the twenty it missed, and
    /// then carries on to the original cadence because the
    /// kernel kept the clock throughout
    ///
    /// That is the deliberate half of it. Firing the whole
    /// backlog at once would answer an outage with a burst,
    /// which is the opposite of what a fixed rate is for
    ///
    /// **Gone for good:** the schedule ends. A last output
    /// stays readable if it had one, and no run starts after
    /// that. Runs already in flight are not interrupted — they
    /// finish, and find nowhere to publish
    ///
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
    /// The gap is *between* runs rather than the period of
    /// them, so a task taking 200ms on a 50ms gap runs every
    /// 250ms and no two runs are ever in flight together
    ///
    /// The wait costs nothing. No worker and no sleep thread is
    /// held for it: the task goes back in its slot and a timer
    /// puts it back on the queue when the gap is up, so a
    /// thousand tasks waiting out an hour cost a thousand slots
    /// and no threads
    ///
    /// #### Note
    /// Belongs to `Repeat` and is not reachable from `Rate`,
    /// which was already given its period by `at_rate` and has
    /// no second interval to set
    /// ## Accuracy
    /// The kernel timer is asked for the gap exactly and marked
    /// critical, so it fires as tightly as one can be asked to.
    /// What the timer can't cover is the moment between firing
    /// and a worker picking the task up, which is however busy
    /// the pool is
    ///
    /// ## If the manager goes
    /// The wait is a timer on the manager's queue, so this is
    /// one of the two that notices.
    ///
    /// **Away and coming back:** runs are *late*, not lost. The
    /// queue stays open across a restart and the slot says a
    /// wake is owed until something acts on it, so a manager
    /// coming back arms a fresh timer for every wait still
    /// outstanding. A gap can therefore come out longer than it
    /// was asked for — never shorter
    ///
    /// **Gone for good:** the series ends, and is written off
    /// rather than left waiting on a queue that has closed, so
    /// every reader gets an answer instead of blocking for the
    /// life of the process
    ///
    /// #### Note
    /// A cancel lands immediately for every reader, but the
    /// slot itself isn't given back until the gap it was
    /// waiting out is up. The timer is left to fire and clear
    /// up on its way through rather than being chased down,
    /// which is worth knowing if the gap is long
    ///
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
    /// Stops repeating once a span has passed
    ///
    /// Measured from when the **repeating starts**, not from
    /// when this is called, so an `after` in the chain doesn't
    /// eat into it — `.after(1s).repeat().for_duration(5s)` is
    /// five seconds of repeating rather than four
    ///
    /// ## Behaviour
    /// A run that would *begin* past the deadline is never
    /// started. So a repeat on a 750ms gap bounded to 1s runs
    /// at 0ms and at 750ms and then stops, because the third
    /// would land near 1500ms — the series finishes and exits
    /// rather than arming a timer for a run nobody wants
    ///
    /// ## When it is over
    /// The last output stays readable — it succeeded, it didn't
    /// fail — and the handle reads `is_finished`
    pub fn for_duration(mut self, span: Duration) -> TaskBuilder<F, K, Set, C> {
        self.setup.deadline = Deadline::Span(span);
        self.moved()
    }

    /// Stops repeating at a moment
    ///
    /// The same ending `for_duration` gives, said as a point
    /// rather than a span. A moment already past means the
    /// series runs once and stops
    ///
    /// ## Behaviour
    /// A run that would *begin* past the moment is never
    /// started, exactly as in `for_duration`
    ///
    /// #### Note
    /// One deadline per series. Setting this closes
    /// `for_duration` and setting that closes this, because
    /// they are two ways of writing the same field and a series
    /// that had both would have to ignore one
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
    /// Stops repeating after a number of runs
    ///
    /// Exactly `runs` runs, then the series is over
    ///
    /// ## Behaviour
    /// Composes with a deadline rather than replacing it.
    /// A series given both ends at whichever is reached first,
    /// so `.count(10).for_duration(1s)` is "ten runs, or one
    /// second, whichever comes up first"
    ///
    /// ## When it is over
    /// The last output stays readable and the handle reads
    /// `is_finished`, which is the only way to tell a series
    /// that has run out from one that is between runs
    ///
    /// #### Note
    /// A count of zero is not a task that never runs. A
    /// sequential repeat has its first run queued before there
    /// is anything to count against, so it runs once and stops;
    /// a schedule is asked before each run is started, so it
    /// starts none. Neither is worth asking for on purpose —
    /// leave the chain off entirely for a task that shouldn't
    /// run
    pub fn count(mut self, runs: u32) -> TaskBuilder<F, K, D, Set> {
        self.setup.runs = runs;
        self.moved()
    }
}

impl<F, D, C> TaskBuilder<F, Once, D, C>
where
    F: Task,
{
    /// ## Behaviour
    /// Never blocks the calling thread. A task arriving faster
    /// than the pool can get through goes on a queue with no
    /// ceiling rather than pushing back on whoever spawned it
    ///
    /// ## If the manager goes
    /// Nothing. A spawned task reaches a worker without passing
    /// through the manager at all, and the pool finds its own
    /// work, reverses its own queue and clears up after its own
    /// dead whether anything is supervising it or not
    ///
    /// What stops while the manager is away is the pool
    /// *adapting* — no growing, no reaping, no rebalancing and
    /// no lifting an overtaken task out of the way. Under load
    /// that shows up as tasks taking longer, never as tasks not
    /// running
    ///
    /// ## If the runtime is shut down
    /// A manager that dies leaves a pool that still runs
    /// everything; a shutdown stops the pool as well. A task
    /// spawned after one settles `Failed` straight away with
    /// its slot given back, so the handle reads an error rather
    /// than blocking on a result that was never coming
    ///
    /// Starts the task and gives back its handle
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
    /// One handle for the whole series rather than one per run.
    /// `join` gives the most recent output, `take` moves one
    /// out and the run after publishes another, and `cancel`
    /// ends the series rather than one run of it
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
    /// The first run goes now rather than a period from now,
    /// the same way every other spawn starts as soon as it can
    ///
    /// #### Note
    /// The `Clone` sits here rather than on the builder, so a
    /// task that can't be cloned is turned away by the call
    /// that actually needed to clone it. Every other kind runs
    /// one copy in one slot and asks for nothing
    pub fn spawn(self) -> TaskHandle<F::Output> {
        Executor::new_series(self.task, self.setup)
    }
}

#[cfg(test)]
mod type_checks {
    //! Chains that must build, checked by building them
    //!
    //! Nothing in here is ever called, and nothing in here
    //! needs to be. The assertion is that the chain *type
    //! checks*, and that has already happened by the time
    //! anything could run
    //!
    //! #### Note
    //! The other half of this can't live here. A chain that
    //! must **not** build can't be written in a file that has
    //! to compile, so the rejections are checked by eye until
    //! there is a `trybuild` suite for them:
    //!
    //! ```ignore
    //! .repeat().at_rate(p)                // a kind is already chosen
    //! .at_rate(p).repeat()                // likewise
    //! .at_rate(p).every(gap)              // `every` belongs to `Repeat`
    //! .repeat().for_duration(s).until(t)  // one deadline per series
    //! .repeat().until(t).for_duration(s)  // and the other way round
    //! .repeat().count(3).count(10)        // one count per series
    //! .repeat().after(d).count(10)        // `after` closed the bounds
    //! .repeat().after(d).for_duration(s)  // likewise
    //! .count(10)                          // a bound on a one shot
    //! .for_duration(s)                    // likewise
    //! ```

    use super::*;
    use crate::{Sleep, SleepTask};

    fn task() -> SleepTask {
        Sleep::sleep(Duration::from_millis(1), false)
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
