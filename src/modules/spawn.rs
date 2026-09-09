//! # Spawn
//! Building a task up before it is handed to the `Executor`
//!
//! `Runtime` has a method for each of the common ways to start
//! a task, and between them they cover most of what anybody
//! wants. What they can't cover is the combinations: there is a
//! `spawn_with_priority` but no `repeating_with_priority`, and
//! adding one per pair would be a method for every point in a
//! grid that only grows
//!
//! This is that grid written once. `TaskSetup` already exists
//! to keep those decisions together on the inside, and this is
//! the same idea turned outward, so a caller sets the ones they
//! care about and leaves the rest alone

use crate::{
    constants::DEFAULT_PRIORITY,
    executor::Executor,
    futures::task::Task,
    modules::{task_handle::TaskHandle, task_setup::TaskSetup},
};
use std::time::Duration;

/// A task being built up before it is spawned
///
/// Nothing has happened yet. The task is held here, inert, and
/// only reaches the `Executor` when `spawn` is called — so a
/// builder that is dropped instead starts nothing
pub struct Spawn<F>
where
    F: Task,
{
    task: F,
    setup: TaskSetup,

    /// Whether the first run waits out `setup.interval`
    ///
    /// Not part of `TaskSetup`, because it isn't something the
    /// slot has to remember. It decides which door the task
    /// goes through once, at spawn, and after that a delayed
    /// one shot is a one shot like any other
    delayed: bool,
}

impl<F> Spawn<F>
where
    F: Task,
{
    /// A task that will run once, at the default priority
    ///
    /// Reached through `Runtime::task`
    pub(crate) fn new(task: F) -> Self {
        Self {
            task,
            setup: TaskSetup::once(DEFAULT_PRIORITY),
            delayed: false,
        }
    }

    /// Sets the priority the task is served at
    ///
    /// Higher is more urgent. `DEFAULT_PRIORITY` sits halfway
    /// up, so there is as much room to put a task below what a
    /// plain spawn gives it as to lift one above
    ///
    /// #### Note
    /// Priority decides the order tasks are *started* in, not
    /// how much of a thread they get once they are running
    pub fn priority(mut self, priority: u8) -> Self {
        self.setup.priority = priority;
        self
    }

    /// Runs again the moment it finishes, until it is cancelled
    ///
    /// See `Runtime::repeating` for what that means and what
    /// the handle does
    ///
    /// #### Note
    /// The kind setters are last one wins. Each writes the kind
    /// and the interval together, so there is no way to end up
    /// with one set and not the other
    pub fn repeating(mut self) -> Self {
        self.setup = TaskSetup::repeating(self.setup.priority);
        self.delayed = false;
        self
    }

    /// Waits out an interval between runs, until it is
    /// cancelled
    ///
    /// The interval is the gap *between* runs rather than the
    /// period of them. See `Runtime::repeat_every`
    pub fn repeat_every(mut self, interval: Duration) -> Self {
        self.setup = TaskSetup::every(self.setup.priority, interval);
        self.delayed = false;
        self
    }

    /// Runs once, when a delay is up
    ///
    /// See `Runtime::after`. Still a one shot — the delay
    /// changes when the first run happens, not what happens
    /// after it
    ///
    /// #### Note
    /// Last one wins here too, so `.after(d).repeating()` is a
    /// repeating task with no delay on it, and
    /// `.repeating().after(d)` is a delayed one shot. There is
    /// no combination of the two
    pub fn after(mut self, delay: Duration) -> Self {
        self.setup = TaskSetup::after(self.setup.priority, delay);
        self.delayed = true;
        self
    }

    /// Starts a fresh run on the interval, whether the last one
    /// has finished or not
    ///
    /// The interval is the *period* rather than the gap. See
    /// `Runtime::every`
    ///
    /// ## Behaviour
    /// Changes what is being built rather than setting a field,
    /// which is why this is the one setter that gives back a
    /// different type. A schedule clones its task for every run
    /// and goes to the `Executor` by a different door, so it
    /// can't share a `spawn` with the other three
    ///
    /// The `Clone` bound lands on `Schedule::spawn` rather than
    /// here, so a task that can't be cloned is turned away by
    /// the call that actually needed to clone it
    pub fn every(self, interval: Duration) -> Schedule<F> {
        // The delay goes with the kind it belonged to. A
        // schedule starts its first run now, the same way every
        // other spawn starts as soon as it can
        Schedule {
            setup: TaskSetup::series(self.setup.priority, interval),
            task: self.task,
        }
    }

    /// Starts the task and gives back its handle
    ///
    /// A `Spawn` that has had nothing set on it is exactly
    /// `Runtime::spawn`
    pub fn spawn(self) -> TaskHandle<F::Output> {
        match self.delayed {
            true => Executor::new_delayed(self.task, self.setup),
            false => Executor::new_task(self.task, self.setup),
        }
    }
}

/// A schedule being built up before it is started
///
/// Reached only through `Spawn::every`, and has no kind setters
/// of its own — a schedule that stopped being a schedule would
/// have to go back to being a `Spawn`, and nothing wants that
pub struct Schedule<F>
where
    F: Task,
{
    task: F,
    setup: TaskSetup,
}

impl<F> Schedule<F>
where
    F: Task + Clone,
{
    /// Sets the priority every run of the schedule is served at
    ///
    /// The same class each run goes in at, rather than
    /// something the schedule itself is served at — the
    /// schedule never reaches a worker at all
    pub fn priority(mut self, priority: u8) -> Self {
        self.setup.priority = priority;
        self
    }

    /// Starts the schedule and gives back its handle
    ///
    /// One handle for the whole schedule rather than one per
    /// run. The first run goes now rather than an interval from
    /// now, the same way every other spawn starts as soon as it
    /// can
    pub fn spawn(self) -> TaskHandle<F::Output> {
        Executor::new_series(self.task, self.setup)
    }
}
