//! # Runtime
//!
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use crate::{
    RuntimeError,
    constants::{DEAD_KQUEUE_ID, DEFAULT_PRIORITY, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW},
    executor::Executor,
    futures::task::Task,
    modules::{
        int_check::IntCheck, pool_stats::PoolStats, task_handle::TaskHandle, worker_pool::POOL,
    },
    reactor::Reactor,
};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicI32, Ordering},
        mpsc,
    },
    thread,
    time::Instant,
};

/// Whether the runtime has been initialised yet
///
/// Use `SeqCst` operations only as
/// it is important that a `Runtime` only gets
/// initialised once
static INIT: AtomicBool = AtomicBool::new(false);

/// Whether initialisation has finished, successfully or not
///
/// `INIT` says somebody has started, this says they are done.
/// Without the pair, a thread that lost the race to `init`
/// could spawn a task before there was anything to run it
static READY: AtomicBool = AtomicBool::new(false);

/// The kqueue id that the `Reactor` watches
///
/// Only use `Relaxed` reads for speed
/// and a single `SeqCst` write at initialisation
/// to ensure everything reads it correctly
static REACTOR_KQUEUE_ID: AtomicI32 = AtomicI32::new(0);

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
    pub fn init() -> Option<RuntimeError> {
        // Claimed and checked in one operation so that two
        // threads arriving together can't both get past it
        if INIT.swap(true, Ordering::SeqCst) {
            // Somebody else got here first, and might still be
            // part way through. A task spawned before the
            // `Executor` exists has nowhere to be delivered, so
            // this waits the initialisation out rather than
            // racing it
            while !READY.load(Ordering::Acquire) {
                thread::yield_now();
            }

            return Some(RuntimeError::AlreadyInit);
        }

        let error = init_runtime();

        // Set whether or not it worked. A failed initialisation
        // is still a finished one, and everything downstream
        // already copes with a runtime that isn't there
        READY.store(true, Ordering::Release);

        error
    }

    /// Blocking call
    ///
    /// Used when you need the data
    /// the instant it arrives and
    /// don't mind waiting for it
    ///
    /// Blocking calls can't be cancelled
    /// by other threads
    #[inline(always)]
    pub fn block<F>(mut task: F) -> F::Output
    where
        F: Task,
    {
        task.prepare();
        let reactor_id = REACTOR_KQUEUE_ID.load(Ordering::Relaxed);

        // Always use task ID of 0 in blocking calls
        // because IDs are per thread so this
        // can't overlap

        task.execute(reactor_id, 0)
    }

    #[inline(always)]
    /// Spawns a task to run asynchronously
    ///
    /// This method doesn't promise consistent
    /// timimg for running tasks, so `SleepTask`s
    /// can exit late but never early
    ///
    /// Use `block()` if this is a problem
    ///
    /// ## Behaviour
    /// Never blocks the calling thread. A task arriving faster
    /// than the pool can get through goes on a queue with no
    /// ceiling rather than pushing back on whoever spawned it
    pub fn spawn<F>(task: F) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        Executor::new_task(task, DEFAULT_PRIORITY)
    }

    /// Spawns a task at a priority of your choosing
    ///
    /// Higher is more urgent. `DEFAULT_PRIORITY` sits halfway
    /// up, so there is as much room to put a task below what
    /// `spawn` gives it as to lift one above
    ///
    /// ## Behaviour
    /// A task is served ahead of everything at a lower
    /// priority and behind everything at a higher one, with
    /// one exception: a task that has been overtaken far
    /// enough is served ahead of its priority anyway. Nothing
    /// queued is ever starved by a stream of more urgent work
    /// arriving behind it
    /// 
    /// A priority of `u8::MAX` does not gaurantee immediate
    /// execution, it just means it'll run sooner than it
    /// would have otherwise
    ///
    /// #### Note
    /// Priority decides the order tasks are *started* in, not
    /// how much of a thread they get once they are running. A
    /// task at the top priority behind one long task still
    /// waits for a worker to come free
    #[inline(always)]
    pub fn spawn_with_priority<F>(task: F, priority: u8) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        Executor::new_task(task, priority)
    }

    /// What the worker pool looks like right now
    ///
    /// #### Note
    /// A snapshot rather than a lock. Every number in it was
    /// true when it was read, and the pool carries on growing,
    /// shrinking and moving work about while it is being
    /// looked at
    pub fn workers() -> PoolStats {
        POOL.stats()
    }

    /// The kqueue the `Reactor` is watching
    ///
    /// The `Executor` hands this to every task it runs, the
    /// same way `block` hands it to every task it runs
    #[inline(always)]
    pub(crate) fn reactor_id() -> i32 {
        REACTOR_KQUEUE_ID.load(Ordering::Relaxed)
    }
}

/// The real non user facing init function
///
/// Called by the `Runtime::init()` method only
fn init_runtime() -> Option<RuntimeError> {
    let reactor_id = unsafe { libc::kqueue() }.check().ok()?;
    REACTOR_KQUEUE_ID.store(reactor_id, Ordering::SeqCst);

    thread::spawn(move || {
        let (tx, rx) = mpsc::channel();
        Reactor::init(reactor_id, tx.clone());

        let mut failures = 0;
        let mut started = Instant::now();

        for dead_id in rx {
            if started.elapsed() >= RESTART_WINDOW {
                failures = 0;
            }

            failures += 1;

            if failures > RESTART_LIMIT {
                REACTOR_KQUEUE_ID.store(DEAD_KQUEUE_ID, Ordering::SeqCst);
                let _ = unsafe { libc::close(dead_id) };
                break;
            }

            thread::sleep(RESTART_BACKOFF * failures);

            let new_id = match unsafe { libc::kqueue() }.check() {
                Ok(new_id) => new_id,
                Err(_) => {
                    REACTOR_KQUEUE_ID.store(DEAD_KQUEUE_ID, Ordering::SeqCst);
                    let _ = unsafe { libc::close(dead_id) };
                    break;
                }
            };

            REACTOR_KQUEUE_ID.store(new_id, Ordering::SeqCst);
            Reactor::init(new_id, tx.clone());

            let _ = unsafe { libc::close(dead_id) };
            started = Instant::now();
        }
    });

    // Not spawned onto a thread of its own, because the
    // `Executor` puts its supervisor on one and the kqueue
    // has to exist before this function returns. A task
    // spawned the instant `init` comes back would otherwise
    // have nowhere to be delivered
    if let Some(error) = Executor::init() {
        return Some(error);
    }

    None
}
