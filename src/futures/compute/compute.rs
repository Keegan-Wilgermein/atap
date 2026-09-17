//! # Compute
//! Turning a closure into a task

use crate::futures::compute::compute_task::ComputeTask;

/// Runs work of the program's own as a task
///
/// Everything a task can be chained with works the same:
/// priorities, delays, repeats and schedules
pub struct Compute;

impl Compute {
    /// A task that runs `work`
    ///
    /// ## Behaviour
    /// `work` is handed the task's input, which is `()` for a task
    /// nothing gives input to, and what it returns is the output.
    /// It runs on a worker, so it should be doing work rather than
    /// waiting. Work that waits on something outside the runtime,
    /// like a lock or a file through `std`, says so with
    /// [`ComputeTask::blocking`]
    ///
    /// `Fn` rather than `FnOnce`, so the same task can repeat. Each
    /// run is handed its own copy of the input
    ///
    /// ## Returns
    /// The task, ready for `Runtime::task` or `Runtime::block`
    ///
    /// ```no_run
    /// # use atap::{Runtime, compute::Compute};
    /// # fn main() -> Result<(), atap::RuntimeError> {
    /// let answer = Runtime::task(Compute::compute(|()| 6 * 7)).spawn();
    /// assert_eq!(answer.join()?, 42);
    ///
    /// let doubled = Runtime::block(Compute::compute(|()| 21 * 2));
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A task that takes input can't be spawned with nothing to
    /// give it:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let _ = Runtime::task(Compute::compute(|value: i32| value * 2)).spawn();
    /// ```
    ///
    /// Or blocked on:
    ///
    /// ```compile_fail,E0277
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let _ = Runtime::block(Compute::compute(|value: i32| value * 2));
    /// ```
    pub fn compute<F, V, T>(work: F) -> ComputeTask<F, V, T>
    where
        F: Fn(V) -> T + Send + 'static,
        V: Clone + Send + 'static,
        T: Send + 'static,
    {
        ComputeTask::new(work)
    }
}
