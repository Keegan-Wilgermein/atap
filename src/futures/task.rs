//! # Task
//! The trait every task implements

/// Stops `Task` being implemented outside the crate
///
/// `Task` has to be public for the signatures that name it, so
/// sealing is what keeps it closed
pub(crate) mod sealed {
    /// Implemented for every type this crate allows as a task
    pub trait Sealed {}
}

/// Implemented by everything the runtime can run
///
/// ## Behaviour
/// `execute` is called once, on one thread, and runs to
/// completion. What it returns is the output
#[allow(private_bounds)]
pub trait Task: sealed::Sealed + Send + 'static {
    /// The final output type
    type Output: Send + 'static;

    /// Runs the task and returns its output
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output;

    /// Resets any state before a run
    ///
    /// Called before every `execute`, including every run of a
    /// repeat, which reuses the same task
    fn prepare(&mut self) {}

    /// Whether this task holds its thread long enough to be run on
    /// a sleep thread instead of a worker
    ///
    /// Asked once, at spawn. `Runtime::block` ignores it
    #[inline(always)]
    fn blocking(&self) -> bool {
        false
    }
}
