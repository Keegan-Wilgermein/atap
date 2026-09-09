//! # Series
//! The two halves of a task that runs on a schedule of its
//! own: the prototype every run is cloned from, and the
//! wrapper each run is actually spawned as
//!
//! `repeating` and `repeat_every` both go round in one slot,
//! because no two runs of them are ever in flight together. A
//! series has no such promise — the interval comes round on
//! time whether the last run has finished or not — so two runs
//! can't share a payload, a state word or a box. Each one gets
//! a slot of its own and publishes back into the series slot on
//! its way out
//!
//! That is the whole reason a series needs cloning where the
//! other two need none. `Task::execute` takes `&self` and
//! would happily be called from two threads at once, but
//! `prepare` takes `&mut self`, and a run that can't prepare
//! itself is a run that starts with the last one's state

use crate::{
    executor::{self, Executor},
    futures::task::{Task, sealed},
};

/// A task a schedule can make more of
///
/// Erased the same way `ErasedTask` erases an output, and for
/// the same reason: the slot holding the prototype has no idea
/// what type is in it. What it keeps instead is the one thing
/// the manager ever asks a prototype to do
///
/// #### Note
/// `Send` rather than `Sync`, which is a promise about how this
/// is used rather than a gap in it. The prototype is handed to
/// the manager once, at spawn, after the spawning thread has
/// finished with it, and only the manager touches it from then
/// on — a single-threaded queue loop, so no two calls to
/// `launch` for the same series can overlap
pub(crate) trait SeriesTask: Send {
    /// Spawns one run of the series
    ///
    /// ## Returns
    /// Whether a run is actually on its way. `false` means the
    /// table had no slot to give or nothing is left to run it,
    /// and a schedule that can't produce runs is a slot doing
    /// nothing forever
    fn launch(&self, series: usize, priority: u8) -> bool;
}

impl<F> SeriesTask for F
where
    F: Task + Clone,
{
    #[inline(always)]
    fn launch(&self, series: usize, priority: u8) -> bool {
        // Taken before the run exists and given back when it
        // is dropped, so the series slot cannot be freed and
        // its id handed to somebody else while a run that
        // still intends to publish into it is out there
        Executor::add_listener(series);

        executor::spawn_run(
            SeriesRun {
                series,
                inner: self.clone(),
            },
            priority,
        )
    }
}

/// One run of a series, wrapped so its output finds its way
/// home
///
/// An ordinary one shot task in every respect. It is queued,
/// stolen, offloaded and cancelled like anything else, holds
/// its own slot, and gives that slot up when it finishes. What
/// makes it a run rather than a task is the last thing it does
///
/// #### Note
/// Its own output is `()`, so the slot it holds costs the
/// header and nothing else. The real output goes to the series,
/// which is the only slot a caller ever has a handle on
pub(crate) struct SeriesRun<F>
where
    F: Task,
{
    /// The slot every run of this series publishes into
    ///
    /// An id and a listener's claim on it, the same pair a
    /// `TaskHandle` is. A run can easily outlive the schedule
    /// that started it — a cancel takes effect at the next
    /// tick, while a run of it carries on to the end — and
    /// publishing into an id that has since been given to
    /// somebody else would write this run's output over a task
    /// that has nothing to do with it
    series: usize,

    /// This run's copy of the task
    inner: F,
}

impl<F> Drop for SeriesRun<F>
where
    F: Task,
{
    /// Gives the run's claim on the series back
    ///
    /// Reached however the run ends: queued and finished,
    /// never queued at all, or unwound part way through. In
    /// every one of those the box holding this is dropped,
    /// which is the whole reason the claim lives here rather
    /// than being given back by hand at the end of `execute`
    fn drop(&mut self) {
        Executor::drop_listener(self.series);
    }
}

impl<F> sealed::Sealed for SeriesRun<F> where F: Task {}

impl<F> Task for SeriesRun<F>
where
    F: Task,
{
    type Output = ();

    /// Runs this copy and leaves the result with the series
    ///
    /// Published from in here rather than after the run has
    /// settled, because the `Executor` knows nothing about
    /// where a run's output is supposed to end up — as far as
    /// it is concerned this task returns nothing at all
    #[inline(always)]
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        executor::publish(self.series, self.inner.execute(reactor_id, task_id));
    }

    #[inline(always)]
    fn prepare(&mut self) {
        self.inner.prepare();
    }

    /// Whatever the task inside says
    ///
    /// Asked of the copy rather than of the wrapper, so a
    /// series of blocking tasks reaches the sleep threads and
    /// doesn't sit on workers for its whole interval
    #[inline(always)]
    fn blocking(&self) -> bool {
        self.inner.blocking()
    }
}
