//! # Runtime
//! 
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use std::sync::atomic::{AtomicBool, Ordering};
use crate::{RuntimeError, futures::task::Task, modules::{counter::Counter}};

/// Whether the runtime has been initialised yet
/// 
/// Use `SeqCst` operations only as
/// it is important that a `Runtime` only gets
/// initialised once
static INIT: AtomicBool = AtomicBool::new(false);

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
        init_runtime();
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
        let out = task.execute();
        RUNNING_TASKS.try_decrement(1);

        out
    }

    // /// Defers the execution of the passed function to the executor,
    // /// returning a `Future` that can be manually checked
    // /// whenever you feel like to see if the task has finished
    // /// 
    // /// Will never block the current thread
    // pub fn whenever<T, F>(
    //     function: F,
    // ) -> Pending<T>
    // where
    //     F: Fn() -> T,
    // {
    //     Pending::new()
    // }

    /// Gets the count of currently active tasks
    /// across all threads
    /// 
    /// This method does not guarantee an identical reading
    /// across threads or that it's reading is definite
    /// 
    /// This method also only reports tasks that are
    /// owned by the current process
    pub fn get_task_count() -> Result<u32, RuntimeError> {
        return RUNNING_TASKS.query();
    }
}

/// The real non user facing init function
/// 
/// Called by the `Runtime` method only
fn init_runtime() {
    println!("Runtime initialised");
}
