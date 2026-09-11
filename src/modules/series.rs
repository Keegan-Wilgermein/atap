//! # Series
//! The prototype a schedule clones its runs from, and the
//! wrapper each run is spawned as
//!
//! Runs of a schedule can overlap, so each gets its own slot
//! and publishes back into the series slot

use crate::{
    executor::{self, Executor},
    futures::task::{Task, sealed},
};

/// A task a schedule can make more of
///
/// `Send` but not `Sync`, because only the manager thread ever
/// calls `launch`
pub(crate) trait SeriesTask: Send {
    /// Spawns one run of the series
    ///
    /// ## Returns
    /// Whether a run is on its way
    fn launch(&self, series: usize, priority: u8) -> bool;
}

impl<F> SeriesTask for F
where
    F: Task + Clone,
{
    #[inline(always)]
    fn launch(&self, series: usize, priority: u8) -> bool {
        // Held until the run is dropped, so the series slot can't be
        // freed and reused while a run could still publish into it
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

/// One run of a series, which publishes its output into the
/// series slot
pub(crate) struct SeriesRun<F>
where
    F: Task,
{
    /// The slot this run publishes into, which it holds a listener
    /// on
    series: usize,

    /// This run's copy of the task
    inner: F,
}

impl<F> Drop for SeriesRun<F>
where
    F: Task,
{
    /// Gives the run's claim on the series back, however the run
    /// ended
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

    /// Runs this copy and publishes the result into the series
    #[inline(always)]
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        executor::publish(self.series, self.inner.execute(reactor_id, task_id));
    }

    #[inline(always)]
    fn prepare(&mut self) {
        self.inner.prepare();
    }

    /// Whatever the task inside says
    #[inline(always)]
    fn blocking(&self) -> bool {
        self.inner.blocking()
    }
}
