//! # Task Builder
//! Building a task up before it is spawned
//!
//! What may be chained is decided by the type, so a combination
//! that makes no sense doesn't compile. Entering a state opens its
//! methods and moving on closes them, and entering a state never
//! clears anything already set
//!
//! ```no_run
//! # use atap::{Runtime, compute::Compute, sleep::Sleep};
//! # use std::time::{Duration, Instant};
//! # let t = || Sleep::sleep(Duration::from_millis(1));
//! # let (gap, delay, period, span) = (Duration::from_millis(5), Duration::from_millis(5), Duration::from_millis(5), Duration::from_secs(1));
//! # let when = Instant::now() + span;
//! # let [a, b, c] = [(); 3].map(|()| Runtime::task(Compute::compute(|()| 1)).spawn());
//! Runtime::task(t()).repeat().every(gap).until(when).spawn();
//! Runtime::task(t()).repeat().count(10).after(delay).spawn();
//! Runtime::task(t()).at_rate(period).for_duration(span).spawn();
//! Runtime::task(t()).wait_for::<i32>().count(3).repeat().count(5).spawn();
//! Runtime::task(t()).receive((a, b, c)).count(1).spawn();
//! ```
//!
//! And ones that don't build:
//!
//! ```text
//! Runtime::task(t).repeat().at_rate(period);      // a kind is already chosen
//! Runtime::task(t).at_rate(period).every(gap);    // `every` belongs to `Repeat`
//! Runtime::task(t).repeat().for_duration(s).until(t);  // one deadline
//! Runtime::task(t).repeat().after(d).count(10);   // `after` closed the bounds
//! Runtime::task(t).repeat().wait_for::<i32>();    // a wait comes before the kind
//! Runtime::task(t).wait_for::<i32>().receive(h);  // one thing starts each run
//! ```

use super::builder_markers::{
    NoWait, Once, Open, Rate, ReceiveAll, ReceiveAny, Repeat, Repeatable, Set, Unset, WaitFor,
    Waits, Wiring,
};
use crate::{
    executor::Executor,
    futures::task::Task,
    modules::{
        forward::{Forwarder, cloned_into},
        handle_kind::Waiting,
        handle_set::HandleSet,
        input::{self, Receives, Standalone},
        merge_set::MergeSet,
        task_handle::TaskHandle,
        task_kind::TaskKind,
        task_setup::{Deadline, TaskSetup},
    },
};
use std::{
    marker::PhantomData,
    time::{Duration, Instant},
};

/// Links made once the task has an id, one per `give_to`
type Forwards = Vec<Box<dyn FnOnce(usize)>>;

/// A task being built up before it is spawned
///
/// Nothing runs until `spawn` is called, so a dropped builder
/// starts nothing
///
/// - `K`: the kind, `Once`, `Repeat` or `Rate`
/// - `D`: whether a deadline has been set
/// - `C`: whether the open state's count has been set
/// - `W`: what starts each run, `NoWait`, `WaitFor<T>`,
///   `ReceiveAll<H>` or `ReceiveAny<H>`
#[must_use = "a builder does nothing until `spawn` is called"]
pub struct TaskBuilder<F, K = Once, D = Open, C = Open, W = NoWait>
where
    F: Task,
    W: Wiring,
{
    task: F,
    setup: TaskSetup,

    /// The set a receive links to at spawn, or nothing
    link: W::Link,

    /// Where every output of this task goes, linked at spawn
    forwards: Forwards,

    _state: PhantomData<(K, D, C, W)>,
}

impl<F> TaskBuilder<F, Once, Open, Open, NoWait>
where
    F: Task,
{
    /// A task that will run once, at the default priority
    pub(crate) fn new(task: F) -> Self {
        Self {
            task,
            setup: TaskSetup::default(),
            link: (),
            forwards: Vec::new(),
            _state: PhantomData,
        }
    }
}

impl<F, K, D, C, W> TaskBuilder<F, K, D, C, W>
where
    F: Task,
    W: Wiring,
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
    /// after it. On a task that waits for gives, or receives, it is
    /// waited out after everything that starts a run or series
    ///
    /// Closes `count`, `for_duration` and `until`, so set those
    /// first: `.repeat().count(10).after(d)`
    ///
    /// #### Note
    /// A delay still waiting when the runtime shuts down is written
    /// off, and its handle reads `TaskFailed`. One that spans a
    /// manager restart runs late, never early
    pub fn after(mut self, delay: Duration) -> TaskBuilder<F, K, Set, Set, W> {
        self.setup.start_delay = delay;
        self.moved()
    }

    /// Gives every output of this task to a task waiting for it
    ///
    /// ## Behaviour
    /// Linked at spawn, before the task can publish. Each output is
    /// given to `target` as it lands, the same as a `give` from its
    /// handle, and a repeat gives every run's output. Chaining this
    /// more than once gives to each target
    ///
    /// The link counts as a way to give to `target` until this task
    /// publishes nothing more
    ///
    /// ```no_run
    /// # use atap::{Runtime, compute::Compute};
    /// let printer = Runtime::task(Compute::compute(|n: u64| println!("{n}")))
    ///     .wait_for::<u64>()
    ///     .spawn();
    ///
    /// Runtime::task(Compute::compute(|()| 7u64)).give_to(&printer).spawn();
    /// ```
    ///
    /// Only to a task waiting for this task's output:
    ///
    /// ```compile_fail,E0308
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let words = Runtime::task(Compute::compute(|word: String| word))
    ///     .wait_for::<String>()
    ///     .spawn();
    ///
    /// let _ = Runtime::task(Compute::compute(|()| 7u64)).give_to(&words).spawn();
    /// ```
    pub fn give_to<O>(mut self, target: &TaskHandle<O, Waiting<F::Output>>) -> Self
    where
        F::Output: Clone,
    {
        let target = target.retyped::<()>();

        self.forwards.push(Box::new(move |id| {
            let forward = Forwarder::new(target, cloned_into::<F::Output, F::Output>);

            Executor::forward(id, Box::new(forward));
        }));

        self
    }

    /// Carries everything set so far into new states
    fn moved<K2, D2, C2>(self) -> TaskBuilder<F, K2, D2, C2, W> {
        TaskBuilder {
            task: self.task,
            setup: self.setup,
            link: self.link,
            forwards: self.forwards,
            _state: PhantomData,
        }
    }

    /// Carries everything set so far into a new wiring, with what it
    /// links to
    fn rewire<W2>(self, link: W2::Link) -> TaskBuilder<F, K, D, C, W2>
    where
        W2: Wiring,
    {
        TaskBuilder {
            task: self.task,
            setup: self.setup,
            link,
            forwards: self.forwards,
            _state: PhantomData,
        }
    }
}

impl<F, D, C, W> TaskBuilder<F, Once, D, C, W>
where
    F: Task,
    W: Wiring,
{
    /// Runs again as soon as the last run finishes
    ///
    /// Runs never overlap. With no `every`, the next run is queued
    /// the moment the last one publishes, and without a `count`,
    /// `for_duration` or `until` it runs until cancelled
    ///
    /// On a task that waits for gives or receives, each arrival
    /// starts a series like this. An arrival during a series only
    /// replaces the value the rest of it is handed. The wait's count
    /// is closed, and the repeat's own is opened
    pub fn repeat(mut self) -> TaskBuilder<F, Repeat, D, W::AfterKind<C>, W> {
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
    /// On a task that waits for gives or receives, each arrival
    /// starts a schedule like this. The wait's count is closed, and
    /// the schedule's own is opened
    ///
    /// #### Note
    /// Nothing pushes back, so a task slower than its period piles
    /// runs up behind each other. Runs finishing together publish
    /// one output between them, and periods missed while the
    /// manager restarts are skipped rather than made up
    pub fn at_rate(mut self, period: Duration) -> TaskBuilder<F, Rate, D, W::AfterKind<C>, W> {
        self.setup.kind = TaskKind::Series;
        self.setup.interval = period;
        self.moved()
    }
}

impl<F, D, C> TaskBuilder<F, Once, D, C, NoWait>
where
    F: Task,
{
    /// Waits for a give of `T` before each run
    ///
    /// ## Behaviour
    /// Nothing runs at spawn. Each give through the handle `spawn`
    /// returns starts a run with the value, and afterwards the task
    /// waits for the next give, until it is cancelled, its `count`
    /// of gives is used up, or no handle is left that could give.
    /// Chaining a kind after this makes each give start that kind of
    /// series instead, with its own count and deadline
    ///
    /// `T` has to be what the task takes. A task that takes
    /// [`Nothing`](crate::Nothing) can wait for any `T`, and drops
    /// the value
    ///
    /// ```no_run
    /// # use atap::{Runtime, compute::Compute};
    /// # fn main() -> Result<(), atap::RuntimeError> {
    /// let doubler = Runtime::task(Compute::compute(|v: i32| v * 2)).wait_for::<i32>().spawn();
    /// doubler.give(7)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// #### Note
    /// Gives don't queue. A give while a run is waiting to start or
    /// under way leaves only the newest value
    ///
    /// A task can only wait for what it takes:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let _ = Runtime::task(Compute::compute(|value: i32| value))
    ///     .wait_for::<String>()
    ///     .spawn();
    /// ```
    ///
    /// And a wait comes before the kind, never after:
    ///
    /// ```compile_fail,E0599
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let _ = Runtime::task(Compute::compute(|value: i32| value))
    ///     .wait_for::<i32>()
    ///     .repeat()
    ///     .wait_for::<i32>();
    /// ```
    pub fn wait_for<T>(mut self) -> TaskBuilder<F, Once, D, C, WaitFor<T>> {
        self.setup.waits = true;
        self.rewire(())
    }

    /// Runs with an output from every task in `from`, once each has
    /// published
    ///
    /// ## Behaviour
    /// `from` is a set of handles of any types: a handle, a tuple of
    /// sets, an array of sets or a `Vec` of sets, nested however
    /// deep. Once every task in it has published, the task runs with
    /// their outputs in the same shape, and the next run waits for
    /// every task to publish again. An output that lands while its
    /// place in the next run is already filled replaces it
    ///
    /// An output already there when this spawns counts. Once a task
    /// in the set publishes nothing more without filling its place,
    /// no later run can happen, and the task finishes
    ///
    /// The handles in `from` are kept until the task finishes. The
    /// handle `spawn` returns is plain, since only the set feeds it
    ///
    /// A task that takes [`Nothing`](crate::Nothing) only waits for
    /// the set
    ///
    /// ```no_run
    /// # use atap::{Runtime, compute::Compute};
    /// # let counter = Runtime::task(Compute::compute(|()| 2u64)).spawn();
    /// # let word = |w: &'static str| Runtime::task(Compute::compute(move |()| w.to_string())).spawn();
    /// # let (first, second) = (word("hello"), word("there"));
    /// let total = Runtime::task(Compute::compute(|(count, words): (u64, Vec<String>)| {
    ///     format!("{count}: {}", words.join(" "))
    /// }))
    /// .receive((counter, vec![first, second]))
    /// .count(1)
    /// .spawn();
    /// ```
    ///
    /// What the set hands over has to be what the task takes:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let words = Runtime::task(Compute::compute(|()| String::from("x"))).spawn();
    /// let _ = Runtime::task(Compute::compute(|value: i32| value)).receive(words).spawn();
    /// ```
    ///
    /// A tuple holds at most twelve sets, so a bigger one nests:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let h = || Runtime::task(Compute::compute(|()| 1u8)).spawn();
    ///
    /// let _ = Runtime::task(Compute::compute(|()| 0u8))
    ///     .receive((h(), h(), h(), h(), h(), h(), h(), h(), h(), h(), h(), h(), h()));
    /// ```
    ///
    /// And nothing else can give to a task that receives:
    ///
    /// ```compile_fail,E0599
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let source = Runtime::task(Compute::compute(|()| 1)).spawn();
    /// let receiver = Runtime::task(Compute::compute(|value: i32| value)).receive(source).spawn();
    /// let _ = receiver.give(2);
    /// ```
    pub fn receive<H>(mut self, from: H) -> TaskBuilder<F, Once, D, C, ReceiveAll<H>>
    where
        H: HandleSet,
    {
        self.setup.waits = true;
        self.rewire(from)
    }

    /// Runs with each output of any task in `from`, turned into what
    /// the task takes
    ///
    /// ## Behaviour
    /// `from` is a set of handles whose outputs each turn into the
    /// task's input with `Into`: a handle, a tuple of sets, an array
    /// of sets or a `Vec` of sets. Every output of any of them starts
    /// a run, and outputs don't queue: one that lands while a run is
    /// waiting to start or under way leaves only the newest value
    ///
    /// Once every task in the set publishes nothing more, the task
    /// finishes. The handle `spawn` returns is plain
    ///
    /// A task that takes [`Nothing`](crate::Nothing) is only started
    ///
    /// ```no_run
    /// # use atap::{Runtime, compute::Compute};
    /// # let line = |w: &'static str| Runtime::task(Compute::compute(move |()| w.to_string())).spawn();
    /// # let (errors, warnings) = (line("error"), line("warning"));
    /// let log = Runtime::task(Compute::compute(|line: String| println!("{line}")))
    ///     .receive_any((errors, warnings))
    ///     .spawn();
    /// ```
    ///
    /// Every output has to turn into what the task takes:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let text = Runtime::task(Compute::compute(|()| String::from("x"))).spawn();
    /// let _ = Runtime::task(Compute::compute(|value: u64| value)).receive_any((text,)).spawn();
    /// ```
    ///
    /// One thing starts each run, so a wait and a receive don't mix:
    ///
    /// ```compile_fail,E0599
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let source = Runtime::task(Compute::compute(|()| 1)).spawn();
    ///
    /// let _ = Runtime::task(Compute::compute(|value: i32| value))
    ///     .wait_for::<i32>()
    ///     .receive_any((source,));
    /// ```
    pub fn receive_any<H>(mut self, from: H) -> TaskBuilder<F, Once, D, C, ReceiveAny<H>>
    where
        H: Send + 'static,
    {
        self.setup.waits = true;
        self.rewire(from)
    }
}

impl<F, D, C, W> TaskBuilder<F, Once, D, C, W>
where
    F: Task,
    W: Wiring,
{
    /// Stops taking what starts a run after `arrivals` of them
    ///
    /// ## Behaviour
    /// Counts gives, or deliveries from what the task receives, but
    /// only those that start something. Once the last one's run, or
    /// series, is over the task is finished, and a later give comes
    /// back `Finished`. Without this the task keeps taking them until
    /// it is cancelled
    ///
    /// Set once:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let _ = Runtime::task(Compute::compute(|value: i32| value))
    ///     .wait_for::<i32>()
    ///     .count(3)
    ///     .count(4);
    /// ```
    ///
    /// And only on a task that waits:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let _ = Runtime::task(Compute::compute(|()| 1)).count(3);
    /// ```
    pub fn count(mut self, arrivals: u32) -> TaskBuilder<F, Once, D, Set, W>
    where
        C: Unset,
        W: Waits,
    {
        self.setup.gives = arrivals;
        self.moved()
    }
}

impl<F, D, C, W> TaskBuilder<F, Repeat, D, C, W>
where
    F: Task,
    W: Wiring,
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

impl<F, K, C, W> TaskBuilder<F, K, Open, C, W>
where
    F: Task,
    K: Repeatable,
    W: Wiring,
{
    /// Stops repeating once `span` has passed
    ///
    /// Measured from when the repeating starts, so a delay set with
    /// `after` doesn't eat into it. On a task that waits or
    /// receives, each series gets the whole span
    ///
    /// ## Behaviour
    /// A run that would start past the deadline is never started,
    /// so a 750ms gap bounded to 1s runs at 0ms and 750ms, then
    /// stops. The last output stays readable, and `is_finished`
    /// reads true
    pub fn for_duration(mut self, span: Duration) -> TaskBuilder<F, K, Set, C, W> {
        self.setup.deadline = Deadline::Span(span);
        self.moved()
    }

    /// Stops repeating at `when`
    ///
    /// A run that would start past it is never started, and a
    /// moment already past means one run. Can't be combined with
    /// `for_duration`
    pub fn until(mut self, when: Instant) -> TaskBuilder<F, K, Set, C, W> {
        self.setup.deadline = Deadline::At(when);
        self.moved()
    }
}

impl<F, K, D, C, W> TaskBuilder<F, K, D, C, W>
where
    F: Task,
    K: Repeatable,
    W: Wiring,
{
    /// Stops repeating after `runs` runs
    ///
    /// ## Behaviour
    /// Combines with a deadline, ending at whichever comes first.
    /// The last output stays readable, and `is_finished` reads true.
    /// On a task that waits or receives, each series gets the whole
    /// count
    ///
    /// #### Note
    /// A count of zero still runs a `repeat` once. On `at_rate` it
    /// starts nothing, and the handle reads `TaskFailed`
    ///
    /// Set once, and before `after`:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    /// use std::time::Duration;
    ///
    /// let _ = Runtime::task(Compute::compute(|()| 1))
    ///     .repeat()
    ///     .after(Duration::from_millis(1))
    ///     .count(3);
    /// ```
    pub fn count(mut self, runs: u32) -> TaskBuilder<F, K, D, Set, W>
    where
        C: Unset,
    {
        self.setup.runs = runs;
        self.moved()
    }
}

/// Links every `give_to`, once the task has an id
fn link_forwards(forwards: Forwards, id: usize) {
    for forward in forwards {
        forward(id);
    }
}

impl<F, D, C> TaskBuilder<F, Once, D, C, NoWait>
where
    F: Task,
    F::Input: Standalone,
{
    /// Starts the task and gives back its handle
    ///
    /// Never blocks. A spawn after the runtime has shut down gives
    /// a handle that reads `TaskFailed`
    pub fn spawn(self) -> TaskHandle<F::Output> {
        let TaskBuilder {
            mut task,
            setup,
            forwards,
            ..
        } = self;

        task.give(input::token(), input::standalone());

        let handle = Executor::new_task(task, setup);
        link_forwards(forwards, handle.id());

        handle
    }
}

impl<F, D, C> TaskBuilder<F, Repeat, D, C, NoWait>
where
    F: Task,
    F::Input: Standalone,
{
    /// Starts the repeat and gives back its handle
    ///
    /// One handle for the whole series. `join` gives the latest
    /// output, `take` moves one out for the next run to replace,
    /// and `cancel` ends the series
    pub fn spawn(self) -> TaskHandle<F::Output> {
        let TaskBuilder {
            mut task,
            setup,
            forwards,
            ..
        } = self;

        task.give(input::token(), input::standalone());

        let handle = Executor::new_task(task, setup);
        link_forwards(forwards, handle.id());

        handle
    }
}

impl<F, D, C> TaskBuilder<F, Rate, D, C, NoWait>
where
    F: Task + Clone,
    F::Input: Standalone,
{
    /// Starts the schedule and gives back its handle
    ///
    /// The first run goes now, not a period from now. Each run is a
    /// fresh clone of the task
    pub fn spawn(self) -> TaskHandle<F::Output> {
        let TaskBuilder {
            mut task,
            setup,
            forwards,
            ..
        } = self;

        task.give(input::token(), input::standalone());

        let handle = Executor::new_series(task, setup);
        link_forwards(forwards, handle.id());

        handle
    }
}

impl<F, D, C, T> TaskBuilder<F, Once, D, C, WaitFor<T>>
where
    F: Task,
    T: Send + 'static,
{
    /// Spawns the task waiting for its first give, and gives back
    /// the handle that gives to it
    ///
    /// Nothing runs until something is given. Dropping every handle
    /// that could give lets the task go
    pub fn spawn<M>(self) -> TaskHandle<F::Output, Waiting<T>>
    where
        F::Input: Receives<T, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            forwards,
            ..
        } = self;

        let handle = Executor::new_waiting::<F, T, M>(task, setup);
        link_forwards(forwards, handle.id());

        handle
    }
}

impl<F, D, C, T> TaskBuilder<F, Repeat, D, C, WaitFor<T>>
where
    F: Task,
    T: Send + 'static,
{
    /// Spawns the repeat waiting for its first give, and gives back
    /// the handle that gives to it
    pub fn spawn<M>(self) -> TaskHandle<F::Output, Waiting<T>>
    where
        F::Input: Receives<T, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            forwards,
            ..
        } = self;

        let handle = Executor::new_waiting::<F, T, M>(task, setup);
        link_forwards(forwards, handle.id());

        handle
    }
}

impl<F, D, C, T> TaskBuilder<F, Rate, D, C, WaitFor<T>>
where
    F: Task + Clone,
    T: Send + 'static,
{
    /// Spawns the schedule waiting for its first give, and gives
    /// back the handle that gives to it
    pub fn spawn<M>(self) -> TaskHandle<F::Output, Waiting<T>>
    where
        F::Input: Receives<T, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            forwards,
            ..
        } = self;

        let handle = Executor::new_waiting_series::<F, T, M>(task, setup);
        link_forwards(forwards, handle.id());

        handle
    }
}

impl<F, D, C, H> TaskBuilder<F, Once, D, C, ReceiveAll<H>>
where
    F: Task,
    H: HandleSet,
{
    /// Spawns the task, run by the set it receives from
    ///
    /// The handle is plain, since only the set feeds the task
    pub fn spawn<M>(self) -> TaskHandle<F::Output>
    where
        F::Input: Receives<H::Output, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            link,
            forwards,
            ..
        } = self;

        let waiting = Executor::new_waiting::<F, H::Output, M>(task, setup);
        link_forwards(forwards, waiting.id());

        Executor::receive_all(waiting, link)
    }
}

impl<F, D, C, H> TaskBuilder<F, Repeat, D, C, ReceiveAll<H>>
where
    F: Task,
    H: HandleSet,
{
    /// Spawns the repeat, a series of which each full set starts
    pub fn spawn<M>(self) -> TaskHandle<F::Output>
    where
        F::Input: Receives<H::Output, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            link,
            forwards,
            ..
        } = self;

        let waiting = Executor::new_waiting::<F, H::Output, M>(task, setup);
        link_forwards(forwards, waiting.id());

        Executor::receive_all(waiting, link)
    }
}

impl<F, D, C, H> TaskBuilder<F, Rate, D, C, ReceiveAll<H>>
where
    F: Task + Clone,
    H: HandleSet,
{
    /// Spawns the schedule, a series of which each full set starts
    pub fn spawn<M>(self) -> TaskHandle<F::Output>
    where
        F::Input: Receives<H::Output, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            link,
            forwards,
            ..
        } = self;

        let waiting = Executor::new_waiting_series::<F, H::Output, M>(task, setup);
        link_forwards(forwards, waiting.id());

        Executor::receive_all(waiting, link)
    }
}

impl<F, D, C, H> TaskBuilder<F, Once, D, C, ReceiveAny<H>>
where
    F: Task,
    H: Send + 'static,
{
    /// Spawns the task, run by each output of any task in its set
    ///
    /// The handle is plain, since only the set feeds the task
    pub fn spawn<M>(self) -> TaskHandle<F::Output>
    where
        H: MergeSet<F::Input, M>,
        F::Input: Receives<H::Given, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            link,
            forwards,
            ..
        } = self;

        let waiting = Executor::new_waiting::<F, H::Given, M>(task, setup);
        link_forwards(forwards, waiting.id());

        Executor::receive_any::<_, F::Input, M, H>(waiting, link)
    }
}

impl<F, D, C, H> TaskBuilder<F, Repeat, D, C, ReceiveAny<H>>
where
    F: Task,
    H: Send + 'static,
{
    /// Spawns the repeat, a series of which each output starts
    pub fn spawn<M>(self) -> TaskHandle<F::Output>
    where
        H: MergeSet<F::Input, M>,
        F::Input: Receives<H::Given, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            link,
            forwards,
            ..
        } = self;

        let waiting = Executor::new_waiting::<F, H::Given, M>(task, setup);
        link_forwards(forwards, waiting.id());

        Executor::receive_any::<_, F::Input, M, H>(waiting, link)
    }
}

impl<F, D, C, H> TaskBuilder<F, Rate, D, C, ReceiveAny<H>>
where
    F: Task + Clone,
    H: Send + 'static,
{
    /// Spawns the schedule, a series of which each output starts
    pub fn spawn<M>(self) -> TaskHandle<F::Output>
    where
        H: MergeSet<F::Input, M>,
        F::Input: Receives<H::Given, M>,
        M: 'static,
    {
        let TaskBuilder {
            task,
            setup,
            link,
            forwards,
            ..
        } = self;

        let waiting = Executor::new_waiting_series::<F, H::Given, M>(task, setup);
        link_forwards(forwards, waiting.id());

        Executor::receive_any::<_, F::Input, M, H>(waiting, link)
    }
}

#[cfg(test)]
mod type_checks {
    //! Chains that must build, checked by compiling them

    use super::*;
    use crate::{
        Runtime,
        compute::Compute,
        sleep::{Sleep, SleepMode, SleepTask},
    };

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
        let _ = TaskBuilder::new(task())
            .repeat()
            .until(Instant::now())
            .spawn();
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

    /// A compute chains every way a task does
    #[allow(dead_code)]
    fn computes() {
        let _ = TaskBuilder::new(Compute::compute(|()| 1)).spawn();
        let _ = TaskBuilder::new(Compute::compute(|()| 1).blocking())
            .priority(200)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|()| 1))
            .repeat()
            .every(Duration::ZERO)
            .count(3)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|()| 1))
            .at_rate(Duration::ZERO)
            .count(3)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|()| 1))
            .after(Duration::ZERO)
            .spawn();
        let _: i32 = Runtime::block(Compute::compute(|()| 1));
    }

    /// A wait comes before the kind, and each state has a count of
    /// its own
    #[allow(dead_code)]
    fn waits() {
        let _ = TaskBuilder::new(Compute::compute(|value: i32| value))
            .wait_for::<i32>()
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|value: i32| value))
            .wait_for::<i32>()
            .count(3)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|value: i32| value))
            .wait_for::<i32>()
            .count(3)
            .repeat()
            .count(5)
            .every(Duration::ZERO)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|value: i32| value))
            .wait_for::<i32>()
            .at_rate(Duration::ZERO)
            .for_duration(Duration::ZERO)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|()| 1))
            .wait_for::<()>()
            .priority(200)
            .after(Duration::ZERO)
            .spawn();

        // A task that takes nothing waits for anything
        let _ = TaskBuilder::new(task())
            .wait_for::<String>()
            .repeat()
            .spawn();

        let handle = TaskBuilder::new(Compute::compute(|value: i32| value))
            .wait_for::<i32>()
            .spawn();

        let _ = handle.give(1);
    }

    /// Receives take sets of any shape, and forwarding chains
    /// anywhere
    #[allow(dead_code)]
    fn receives() {
        let a = TaskBuilder::new(Compute::compute(|()| 1u8)).spawn();
        let b = TaskBuilder::new(Compute::compute(|()| 2u16)).spawn();
        let c = TaskBuilder::new(Compute::compute(|()| String::new())).spawn();

        let _ = TaskBuilder::new(Compute::compute(|(a, (b, c)): (u8, (u16, String))| {
            (a, b, c)
        }))
        .receive((a.clone(), (b.clone(), c.clone())))
        .count(1)
        .spawn();
        let _ = TaskBuilder::new(Compute::compute(|all: Vec<u8>| all))
            .receive(vec![a.clone(), a.clone()])
            .repeat()
            .count(2)
            .spawn();
        let _ = TaskBuilder::new(Compute::compute(|pair: [u8; 2]| pair))
            .receive([a.clone(), a.clone()])
            .at_rate(Duration::ZERO)
            .spawn();
        let _ = TaskBuilder::new(task())
            .receive((a.clone(), c.clone()))
            .spawn();

        let _ = TaskBuilder::new(Compute::compute(|value: u64| value))
            .receive_any((a.clone(), b.clone()))
            .spawn();
        let _ = TaskBuilder::new(task())
            .receive_any(vec![c.clone()])
            .spawn();

        let waiting = TaskBuilder::new(Compute::compute(|value: u8| value))
            .wait_for::<u8>()
            .spawn();

        let _ = TaskBuilder::new(Compute::compute(|()| 3u8))
            .give_to(&waiting)
            .give_to(&waiting)
            .repeat()
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
