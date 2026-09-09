//! # Runtime
//!
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use crate::{
    RuntimeError,
    constants::{DEAD_KQUEUE_ID, DEFAULT_PRIORITY, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW},
    executor::{self, Executor},
    futures::task::Task,
    modules::{
        int_check::IntCheck, pool_stats::PoolStats, task_handle::TaskHandle,
        task_setup::TaskSetup, worker_pool::POOL,
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
    /// by any means
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
        Executor::new_task(task, TaskSetup::once(DEFAULT_PRIORITY))
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
        Executor::new_task(task, TaskSetup::once(priority))
    }

    /// Spawns a task that keeps running until it is cancelled
    ///
    /// ## Behaviour
    /// A run finishes before the next one starts, always. There
    /// is no interval and no clock: the moment a run publishes
    /// its output the task goes back on the queue, behind
    /// whatever else is waiting, so it takes a share of the
    /// pool rather than a thread of it
    ///
    /// ## The handle
    /// The same handle as any other task, meaning the same
    /// things. `ready` is true when a result is waiting,
    /// `join` gives the most recent one, and `take` moves one
    /// out — after which the next run publishes another, so a
    /// later read succeeds where on a one shot it would stay
    /// `AlreadyTaken`
    ///
    /// `cancel` ends the series rather than one run of it. The
    /// run in flight finishes and its output is dropped, and
    /// there is no run after it
    ///
    /// #### Note
    /// It never settles on its own, so `join` waits for the
    /// next output rather than for the task to be finished.
    /// Waiting for that would be waiting forever
    ///
    /// A task that panics ends the series. It came apart part
    /// way through, and running it again isn't a way of finding
    /// out whether it would do the same twice
    #[inline(always)]
    pub fn repeating<F>(task: F) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        Executor::new_task(task, TaskSetup::repeating(DEFAULT_PRIORITY))
    }

    /// Spawns a task that runs, waits, and runs again until it
    /// is cancelled
    ///
    /// ## Behaviour
    /// The interval is the gap *between* runs, not the period
    /// of them. A run finishes, the interval is waited out, and
    /// the next run starts — so a task taking 200ms on a 50ms
    /// interval runs every 250ms rather than every 50ms, and no
    /// two runs are ever in flight together
    ///
    /// The wait costs nothing. No worker and no sleep thread is
    /// held for it: the task goes back in its slot and a timer
    /// on the manager's queue puts it back on the worker queue
    /// when the interval is up, so a thousand tasks waiting out
    /// an hour cost a thousand slots and no threads
    ///
    /// ## Accuracy
    /// The kernel timer is asked for the interval exactly and
    /// marked critical, so it fires as tightly as one can be
    /// asked to. What the timer can't cover is the moment
    /// between firing and a worker picking the task up, which
    /// is however busy the pool is
    ///
    /// ## The handle
    /// Everything `repeating` says about its handle holds here.
    /// `join` gives the most recent output, `take` moves one
    /// out and the next run publishes another, and `cancel`
    /// ends the series
    ///
    /// #### Note
    /// A cancel lands immediately for every reader, but the
    /// slot itself isn't given back until the interval it was
    /// waiting out is up. The timer is left to fire and clear
    /// up on its way through rather than being chased down,
    /// which is worth knowing if the interval is long
    #[inline(always)]
    pub fn repeat_every<F>(interval: Duration, task: F) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        Executor::new_task(task, TaskSetup::every(DEFAULT_PRIORITY, interval))
    }

    /// Spawns a task that starts again on the interval, whether
    /// the last one has finished or not
    ///
    /// ## Behaviour
    /// The interval is the *period*, not the gap. A run starts
    /// every interval on the clock, so a task taking 200ms on a
    /// 50ms period has four of itself in flight at once and
    /// still starts a fifth on time
    ///
    /// The clock is the kernel's. One repeating timer is armed
    /// at the start and left alone, so the cadence never drifts
    /// with how long a run took or how busy the pool was when
    /// the last one landed
    ///
    /// Each run is a fresh copy of the task in a slot of its
    /// own, which is what lets runs overlap at all. One slot
    /// holds one output, and two runs finishing together need
    /// somewhere separate to be until they do. That is also why
    /// this asks for `Clone` where `repeating` doesn't
    ///
    /// ## The handle
    /// One handle for the whole schedule, meaning what it
    /// always means. `join` gives the output of whichever run
    /// finished most recently, `take` moves one out and the run
    /// after it publishes another, and `cancel` ends the
    /// schedule rather than one run of it
    ///
    /// #### Note
    /// Runs overlap, so "most recent" is as precise as the
    /// order they happened to finish in. Two runs landing
    /// together publish one output between them and the other
    /// is dropped, nothing queues up behind a reader that
    /// isn't looking
    ///
    /// #### Note
    /// Runs pile up if the pool can't keep up. The interval is
    /// kept whatever else is happening, so a task that takes
    /// longer than its period, or a period that comes round
    /// while the pool is busy with something else — leaves runs
    /// queued behind each other, and nothing pushes back. That
    /// is what a fixed rate means, so check
    /// before putting a slow task on a short period
    ///
    /// #### Note
    /// A cancel lands immediately for every reader, and stops
    /// runs starting from the next tick of the interval. Runs
    /// already in flight are not interrupted, the same as
    /// anywhere else, their output simply goes nowhere
    ///
    /// The slot itself comes back once the last of them has
    /// finished, since a run that still intends to publish is a
    /// run the schedule has to outlive
    ///
    /// A run that panics costs that run. The schedule carries
    /// on, because the copy that came apart was not the task
    /// itself and the next copy is made from a prototype that
    /// never ran
    #[inline(always)]
    pub fn every<F>(interval: Duration, task: F) -> TaskHandle<F::Output>
    where
        F: Task + Clone,
    {
        Executor::new_series(task, TaskSetup::series(DEFAULT_PRIORITY, interval))
    }

    /// Gives back the memory behind the unused part of the
    /// task table
    ///
    /// ## Returns
    /// Bytes handed back to the kernel, or `StillInUse` when
    /// the table is too close to the number of tasks alive in
    /// it for any of it to be worth or safe taking
    ///
    /// ## Behaviour
    /// The table never shrinks on its own as tasks come and go,
    /// because a slot that has been used once is the cheapest
    /// slot there is to use again. A burst of a million tasks
    /// therefore leaves a million slots' worth of pages behind
    /// it, and this is how they go back
    ///
    /// Gives back at most a fifth of the table at a time, keeps
    /// headroom above what is live, and never goes below a
    /// hundred slots. Calling it repeatedly is how it converges
    ///
    /// #### Note
    /// The runtime already does this by itself, every few
    /// seconds, whenever the table is worth trimming. This is
    /// for forcing a pass at a moment of your choosing, such as
    /// straight after a burst you know isn't coming back
    pub fn trim() -> Result<usize, RuntimeError> {
        executor::trim()
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
