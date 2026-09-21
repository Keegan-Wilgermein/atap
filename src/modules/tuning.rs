//! # Tuning
//! The sizes a runtime is started with, and where the pool reads
//! them from

use crate::{
    constants::{MIN_WORKER_STACK, SLEEP_MULTIPLIER, WORKER_MULTIPLIER, WORKER_STACK},
    modules::errors::RuntimeError,
};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Workers the pool settles around, per core
static WORKERS_PER_CORE: AtomicUsize = AtomicUsize::new(WORKER_MULTIPLIER);

/// Sleep threads the pool settles around, per core
static SLEEP_THREADS_PER_CORE: AtomicUsize = AtomicUsize::new(SLEEP_MULTIPLIER);

/// Stack reserved for each worker thread
static STACK: AtomicUsize = AtomicUsize::new(WORKER_STACK);

/// The sizes one runtime runs with
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Tuning {
    /// Workers the pool settles around, per core
    workers_per_core: usize,

    /// Sleep threads the pool settles around, per core
    sleep_threads_per_core: usize,

    /// Stack reserved for each worker thread
    worker_stack: usize,
}

impl Tuning {
    /// The sizes a plain `Runtime::init` runs with
    pub(crate) const fn new() -> Self {
        Self {
            workers_per_core: WORKER_MULTIPLIER,
            sleep_threads_per_core: SLEEP_MULTIPLIER,
            worker_stack: WORKER_STACK,
        }
    }

    /// Sets the workers the pool settles around, per core
    pub(crate) fn set_workers_per_core(&mut self, count: usize) {
        self.workers_per_core = count;
    }

    /// Sets the sleep threads the pool settles around, per core
    pub(crate) fn set_sleep_threads_per_core(&mut self, count: usize) {
        self.sleep_threads_per_core = count;
    }

    /// Sets the stack reserved for each worker thread
    pub(crate) fn set_worker_stack(&mut self, bytes: usize) {
        self.worker_stack = bytes;
    }

    /// Checks the sizes are ones a pool can run with
    ///
    /// ## Returns
    /// `BadArgument` for a count of zero, or a stack too small for
    /// a thread
    pub(crate) fn check(&self) -> Result<(), RuntimeError> {
        match self.workers_per_core == 0
            || self.sleep_threads_per_core == 0
            || self.worker_stack < MIN_WORKER_STACK
        {
            true => Err(RuntimeError::BadArgument),
            false => Ok(()),
        }
    }
}

/// Puts these sizes in place for the runtime that is starting
pub(crate) fn apply(tuning: Tuning) {
    WORKERS_PER_CORE.store(tuning.workers_per_core, Ordering::Release);
    SLEEP_THREADS_PER_CORE.store(tuning.sleep_threads_per_core, Ordering::Release);
    STACK.store(tuning.worker_stack, Ordering::Release);
}

/// Workers the pool settles around, per core
#[inline(always)]
pub(crate) fn workers_per_core() -> usize {
    WORKERS_PER_CORE.load(Ordering::Acquire)
}

/// Sleep threads the pool settles around, per core
#[inline(always)]
pub(crate) fn sleep_threads_per_core() -> usize {
    SLEEP_THREADS_PER_CORE.load(Ordering::Acquire)
}

/// Stack reserved for each worker thread
#[inline(always)]
pub(crate) fn worker_stack() -> usize {
    STACK.load(Ordering::Acquire)
}
