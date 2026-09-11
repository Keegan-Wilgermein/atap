//! # Runtime
//!
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use crate::{
    RuntimeError, Sleep,
    constants::{DEAD_KQUEUE_ID, MAX_TASK_ID, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW},
    executor::{self, Executor},
    futures::task::Task,
    modules::{
        builder::TaskBuilder, int_check::IntCheck, join_policy::JoinPolicy,
        pool_stats::PoolStats, runtime_status::RuntimeStatus, task_handle::TaskHandle,
        worker_pool::POOL,
    },
    reactor::Reactor,
};
use std::{
    time::Duration,
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
static READY: AtomicBool = AtomicBool::new(false);

/// The kqueue id that the `Reactor` watches
///
/// Only use `Relaxed` reads for speed
/// and a single `SeqCst` write at initialisation
/// to ensure everything reads it correctly
static REACTOR_KQUEUE_ID: AtomicI32 = AtomicI32::new(0);

/// Global caller into the runtime
pub struct Runtime;

impl Runtime {
    /// Inits a new runtime
    ///
    /// If a runtime is already initialised, this is a no-op
    ///
    /// After a `shutdown`, this starts it again. A shutdown still
    /// in progress is waited out first
    ///
    /// Runtimes aren't returned as objects to call methods on
    /// and instead handle all their operations and state internally
    ///
    /// For this reason, Runtimes are threadsafe
    pub fn init() -> Option<RuntimeError> {
        if INIT.swap(true, Ordering::SeqCst) {
            // Somebody else is part way through, so wait it out
            while !READY.load(Ordering::Acquire) {
                thread::yield_now();
            }

            // Starts it again after a shutdown, and is `AlreadyInit`
            // otherwise
            return Executor::init();
        }

        let error = init_runtime();

        // Set whether or not it worked
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
    /// by any means
    ///
    /// Runs on the calling thread, so it still works if the
    /// manager goes or the runtime is shut down
    #[inline(always)]
    pub fn block<F>(mut task: F) -> F::Output
    where
        F: Task,
    {
        task.prepare();
        let reactor_id = REACTOR_KQUEUE_ID.load(Ordering::Relaxed);

        // IDs are per thread, so 0 can't overlap
        task.execute(reactor_id, 0)
    }

    /// Builds a task up before spawning it
    ///
    /// ## Behaviour
    /// Every task that runs on the pool starts here. What it
    /// does is decided by what is chained on before `spawn`:
    /// once now, once later, repeating back to back, repeating
    /// with a gap, on a fixed rate, and bounded by a count or
    /// a deadline or both
    ///
    /// A combination with no meaning doesn't compile, such as
    /// a gap with no repeat or the same bound set twice
    ///
    /// Nothing happens until `spawn` is called
    ///
    /// ```ignore
    /// Runtime::task(work).priority(200).repeat().every(gap).spawn();
    /// ```
    ///
    /// #### Note
    /// A bare `Runtime::task(t).spawn()` is a task that runs
    /// once, now, at the default priority
    #[inline(always)]
    pub fn task<F>(task: F) -> TaskBuilder<F>
    where
        F: Task,
    {
        TaskBuilder::new(task)
    }

    /// Sleeps the calling thread, accurately
    ///
    /// Shorthand for `Runtime::block(Sleep::sleep(time, true))`
    ///
    /// ## Returns
    /// The total time it actually took
    ///
    /// #### Note
    /// Precision mode, so the last stretch is spun rather than
    /// slept. Use `block` with `Sleep::sleep(time, false)` for
    /// a sleep that never burns a core
    #[inline(always)]
    pub fn sleep(time: Duration) -> Duration {
        Self::block(Sleep::sleep(time, true))
    }

    /// Waits for every one of a set of tasks
    ///
    /// ## Returns
    /// One result per task, in the order they were given, each
    /// exactly what `join` would have given for that task, so
    /// one task failing doesn't hide the others
    pub fn join_all<T, I>(handles: I) -> Vec<Result<T, RuntimeError>>
    where
        I: IntoIterator<Item = TaskHandle<T>>,
        T: Clone,
    {
        handles.into_iter().map(|handle| handle.join()).collect()
    }

    /// Waits for the first of several tasks to settle
    ///
    /// Settled means ready, taken, cancelled or failed. A handle
    /// with no task behind it counts as settled straight away
    ///
    /// ## Returns
    /// The winning handle, and what `policy` said to do about
    /// the rest. Only [`JoinPolicy::PassBack`] gives a `Some`,
    /// and it keeps the order they were given in
    ///
    /// The winner is a handle, not an output, so reading it with
    /// `join` or `take` is left to the caller
    ///
    /// An empty set has no winner, so what comes back is a
    /// handle to no task, and every read on it answers
    /// `NoSuchTask`
    ///
    /// ```ignore
    /// let (first, rest) = Runtime::join_first(handles, JoinPolicy::Cancel);
    /// let answer = first.take()?;
    /// ```
    ///
    /// #### Note
    /// A task that isn't the winner is untouched by having been
    /// in the set
    ///
    /// #### Note
    /// Every handle in the set has the same output type. Use
    /// `join_with_timeout` to put a deadline on a single task
    pub fn join_first<T, I>(
        handles: I,
        policy: JoinPolicy,
    ) -> (TaskHandle<T>, Option<Vec<TaskHandle<T>>>)
    where
        I: IntoIterator<Item = TaskHandle<T>>,
    {
        let mut handles: Vec<TaskHandle<T>> = handles.into_iter().collect();

        let ids: Vec<usize> = handles.iter().map(|handle| handle.id()).collect();

        // `None` only for an empty set
        let winner = match executor::join_first(&ids) {
            Some(winner) => winner,
            None => {
                return (
                    TaskHandle::new(MAX_TASK_ID),
                    match policy {
                        JoinPolicy::PassBack => Some(Vec::new()),
                        _ => None,
                    },
                );
            }
        };

        // Removed rather than swapped, so the losers keep their order
        let at = ids
            .iter()
            .position(|id| *id == winner)
            .unwrap_or_default();

        let first = handles.remove(at);

        match policy {
            JoinPolicy::PassBack => (first, Some(handles)),

            JoinPolicy::Cancel => {
                for handle in handles {
                    handle.cancel();
                }

                (first, None)
            }

            JoinPolicy::Drop => {
                drop(handles);

                (first, None)
            }
        }
    }

    /// Whether the runtime has finished initialising
    ///
    /// #### Note
    /// Says initialisation is over, not that it worked. Use
    /// `status` for whether anything came of it
    pub fn initialised() -> bool {
        READY.load(Ordering::Acquire)
    }

    /// Whether everything is up and nothing has given up
    ///
    /// Shorthand for `Runtime::status().healthy()`
    pub fn healthy() -> bool {
        Self::status().healthy()
    }

    /// What the runtime looks like right now
    ///
    /// #### Note
    /// A snapshot rather than a lock. The `Reactor` and the
    /// manager carry on while it is being looked at
    pub fn status() -> RuntimeStatus {
        let initialised = Self::initialised();

        // None of these mean anything before an initialisation
        RuntimeStatus::new(
            initialised,
            initialised && executor::shutting_down(),
            initialised && REACTOR_KQUEUE_ID.load(Ordering::Relaxed) != DEAD_KQUEUE_ID,
            initialised && executor::manager_alive(),
        )
    }

    /// Stops the runtime until the next `init`
    ///
    /// ## Behaviour
    /// Drains rather than aborts. Nothing new gets in, and a
    /// spawn after this settles `Failed` straight away.
    /// Everything already queued still runs, and a task in
    /// flight runs to the end
    ///
    /// Blocks until the pool has nothing left to do and every
    /// thread it started has gone. Anything the drain can't
    /// reach, like a repeat between runs or a socket task waiting
    /// on the network, is failed so its listeners get an answer
    ///
    /// `block` still works during and after a shutdown
    ///
    /// `init` after this starts the runtime again. Handles from
    /// before it keep reading what their tasks ended with
    ///
    /// #### Note
    /// Calling it twice is safe. The second call waits for the
    /// first to finish
    ///
    /// #### Note
    /// Never comes back while a task that never finishes is
    /// still running, so don't call it from inside a spawned task
    pub fn shutdown() {
        executor::shutdown_now();
    }

    /// Gives back the memory behind the unused part of the
    /// task table
    ///
    /// ## Returns
    /// Bytes handed back to the kernel, or `StillInUse` when
    /// the table is too close to the number of tasks alive in
    /// it for any of it to be worth taking
    ///
    /// ## Behaviour
    /// Gives back at most a fifth of the table at a time, keeps
    /// headroom above what is live, and never goes below a
    /// hundred slots. Calling it repeatedly is how it converges
    ///
    /// #### Note
    /// The runtime already does this by itself every few
    /// seconds. This forces a pass, such as straight after a
    /// burst you know isn't coming back
    pub fn trim() -> Result<usize, RuntimeError> {
        executor::trim()
    }

    /// Makes the manager come apart the next `count` times it
    /// goes round its loop
    ///
    /// ## Behaviour
    /// Fewer than the restart limit and the supervisor brings it
    /// back every time. More and it gives up, and everything
    /// waiting on its queue is failed
    ///
    /// #### Note
    /// Only here for the crate's own tests, since nothing else
    /// can reach the restart path
    #[doc(hidden)]
    pub fn inject_manager_faults(count: u32) {
        executor::inject_manager_faults(count);
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
    #[inline(always)]
    pub(crate) fn reactor_id() -> i32 {
        REACTOR_KQUEUE_ID.load(Ordering::Relaxed)
    }
}

/// The real non user facing init function
///
/// Called by the `Runtime::init()` method only
fn init_runtime() -> Option<RuntimeError> {
    let reactor_id = match unsafe { libc::kqueue() }.check() {
        Ok(reactor_id) => reactor_id,
        Err(error) => return Some(error),
    };

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

    // Not spawned, since the kqueue has to exist before `init` returns
    if let Some(error) = Executor::init() {
        return Some(error);
    }

    None
}
