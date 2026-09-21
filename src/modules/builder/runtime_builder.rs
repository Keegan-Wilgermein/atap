//! # Runtime Builder
//! The chain a runtime is started through when the defaults don't
//! suit

use crate::{
    modules::{errors::RuntimeError, tuning::Tuning},
    runtime::Runtime,
};

/// The sizes a runtime is about to start with
///
/// ## Behaviour
/// Nothing happens until `init` is called. Every setting left
/// alone keeps the value a plain `Runtime::init` uses
///
/// ```no_run
/// use atap::{Runtime, RuntimeError};
///
/// # fn main() -> Result<(), RuntimeError> {
/// Runtime::builder()
///     .workers_per_core(2)
///     .sleep_threads_per_core(4)
///     .worker_stack(2 * 1024 * 1024)
///     .init()?;
/// # Ok(())
/// # }
/// ```
pub struct RuntimeBuilder {
    /// What the pool is given once `init` is called
    tuning: Tuning,
}

impl RuntimeBuilder {
    /// A builder holding the defaults
    pub(crate) const fn new() -> Self {
        Self {
            tuning: Tuning::new(),
        }
    }

    /// Sets the workers the pool settles around, per core
    ///
    /// A target rather than a limit: the pool passes it on overload
    /// or real need, and never goes below one worker per core
    pub fn workers_per_core(mut self, count: usize) -> Self {
        self.tuning.set_workers_per_core(count);

        self
    }

    /// Sets the sleep threads the pool settles around, per core
    ///
    /// Sleep threads run the tasks that block, so this is the depth
    /// of blocking work that runs at once
    pub fn sleep_threads_per_core(mut self, count: usize) -> Self {
        self.tuning.set_sleep_threads_per_core(count);

        self
    }

    /// Sets the stack reserved for each worker thread
    pub fn worker_stack(mut self, bytes: usize) -> Self {
        self.tuning.set_worker_stack(bytes);

        self
    }

    /// Starts the runtime with these sizes
    ///
    /// ## Returns
    /// `BadArgument` for a count of zero or a stack too small for a
    /// thread, and `AlreadyInit` when a runtime is already running,
    /// which leaves the sizes as they are
    ///
    /// After a `shutdown` these sizes replace the ones before them
    pub fn init(self) -> Result<(), RuntimeError> {
        self.tuning.check()?;

        Runtime::init_with(self.tuning)
    }
}

impl Default for RuntimeBuilder {
    fn default() -> Self {
        Self::new()
    }
}
