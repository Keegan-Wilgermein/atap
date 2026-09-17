//! # Compute task
//! The task `Compute::compute` returns, and what it does once run

use crate::futures::task::{Task, sealed};
use crate::modules::input::Token;
use std::{fmt, marker::PhantomData};

/// Work of the program's own, waiting to be run
///
/// ## Returns
/// Whatever the closure returns
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ComputeTask<F, V, T> {
    /// What each run calls
    work: F,

    /// What each run is handed, once something has given it
    ///
    /// Kept for the life of the task, so a repeat hands every run the
    /// same value
    input: Option<V>,

    /// Whether the work waits on something outside the runtime
    blocking: bool,

    /// The output, which the task itself never holds
    _output: PhantomData<fn() -> T>,
}

impl<F, V, T> ComputeTask<F, V, T> {
    /// Wraps `work`, with no input given yet
    pub(crate) fn new(work: F) -> Self {
        Self {
            work,
            input: None,
            blocking: false,
            _output: PhantomData,
        }
    }

    /// Says the work waits on something outside the runtime
    ///
    /// ## Behaviour
    /// The task runs on a sleep thread, the way a file task does,
    /// so no worker is held while it waits. Work that keeps a core
    /// busy doesn't need this
    ///
    /// ## Returns
    /// The task. Calling it twice keeps it blocking
    pub fn blocking(mut self) -> Self {
        self.blocking = true;
        self
    }
}

impl<F, V, T> Clone for ComputeTask<F, V, T>
where
    F: Clone,
    V: Clone,
{
    /// A copy with the same work and the same input
    fn clone(&self) -> Self {
        Self {
            work: self.work.clone(),
            input: self.input.clone(),
            blocking: self.blocking,
            _output: PhantomData,
        }
    }
}

impl<F, V, T> fmt::Debug for ComputeTask<F, V, T> {
    /// Whether it has its input and whether it blocks
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ComputeTask")
            .field("given", &self.input.is_some())
            .field("blocking", &self.blocking)
            .finish_non_exhaustive()
    }
}

impl<F, V, T> sealed::Sealed for ComputeTask<F, V, T> {}

impl<F, V, T> Task for ComputeTask<F, V, T>
where
    F: Fn(V) -> T + Send + 'static,
    V: Clone + Send + 'static,
    T: Send + 'static,
{
    type Output = T;
    type Input = V;

    /// Calls the work with a copy of the input
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        let input = self
            .input
            .clone()
            .expect("a compute only runs once it has been given its input");

        (self.work)(input)
    }

    /// Whatever `blocking` said
    fn blocking(&self, _token: Token) -> bool {
        self.blocking
    }

    /// Keeps the input every run is handed a copy of
    fn give(&mut self, _token: Token, input: Self::Input) {
        self.input = Some(input);
    }
}
