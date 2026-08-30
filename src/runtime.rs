//! # Runtime
//! 
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use crate::{RuntimeError, futures::task::Task, modules::{counter::Counter, int_check::IntCheck, reactor::Reactor}};

/// Whether the runtime has been initialised yet
/// 
/// Use `SeqCst` operations only as
/// it is important that a `Runtime` only gets
/// initialised once
static INIT: AtomicBool = AtomicBool::new(false);

/// The kqueue id associated with the process
/// 
/// Only use `Relaxed` reads for speed
/// and a single `SeqCst` write at initialisation
/// to ensure everything reads it correctly
static KQUEUE_ID: AtomicI32 = AtomicI32::new(0);

/// How many tasks are running at the moment
static RUNNING_TASKS: Counter = Counter::new();

pub struct Runtime;

impl Runtime {
    /// Inits a new runtime
    /// 
    /// If a runtime is already initialised, this is a no-op
    /// 
    /// Runtimes aren't returned as objects to call methods on
    /// and instead handle all their operations and state internally
    /// 
    /// For this reason, Runtimes are threadsafe
    pub fn init() {
        // No-op if already initialised
        if INIT.load(Ordering::SeqCst) {
            return;
        }

        // Store the initilised value first so another thread can't
        // start at the same time
        INIT.store(true, Ordering::SeqCst);
        let id = unsafe { libc::kqueue() }.check();
        KQUEUE_ID.store(id, Ordering::SeqCst);
        init_runtime(id);
    }

    /// Blocking call
    /// 
    /// Used when you need the data
    /// the instant it arrives and
    /// don't mind waiting for it
    pub fn block_on<F>(task: F) -> F::Output
    where
        F: Task,
    {
        RUNNING_TASKS.try_increment(1);
        let id = KQUEUE_ID.load(Ordering::Relaxed);
        let out = task.execute(id);
        RUNNING_TASKS.try_decrement(1);

        out
    }

    /// Gets the count of currently active tasks
    /// across all threads
    /// 
    /// This method does not guarantee an identical reading
    /// across threads or that it's reading is accurate
    /// 
    /// This method also only reports tasks that are
    /// owned by the current process
    pub fn get_task_count() -> Result<u32, RuntimeError> {
        return RUNNING_TASKS.query();
    }
}

/// The real non user facing init function
/// 
/// Called by the `Runtime::init()` method only
fn init_runtime(id: i32) {
    Reactor::init(id);
}
