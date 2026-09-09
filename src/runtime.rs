//! # Runtime
//!
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use crate::{
    RuntimeError, Sleep,
    constants::{DEAD_KQUEUE_ID, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW},
    executor::{self, Executor},
    futures::task::Task,
    modules::{
        builder::TaskBuilder, int_check::IntCheck, pool_stats::PoolStats,
        runtime_status::RuntimeStatus, task_handle::TaskHandle, worker_pool::POOL,
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

/// Global caller into the runtime
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
        // A shutdown gave every slot in the table back, so
        // starting again would hand those ids to new tasks
        // while old handles are still holding them. Refused
        // rather than quietly doing nothing, because a caller
        // that gets `None` here is entitled to spawn
        if executor::shutting_down() {
            return Some(RuntimeError::ShutDown);
        }

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
    ///
    /// ## If the manager goes
    /// Nothing, and less than nothing. This runs on the calling
    /// thread and never goes near the `Executor`, so there is
    /// no part of it the manager could have been involved in
    ///
    /// ## If the runtime is shut down
    /// Nothing here either, and deliberately so. `shutdown`
    /// leaves the `Reactor` up precisely because a blocking
    /// call on a thread that couldn't get a queue of its own
    /// waits on it, and closing it would break the promise
    /// above
    ///
    /// ## Why there is no builder form
    /// Every other way into the runtime is a chain from
    /// `Runtime::task`, and this deliberately isn't one. None
    /// of what the builder offers can mean anything here: this
    /// runs on the calling thread and never reaches the
    /// `Executor`, so there is no queue to be given a priority
    /// in, and repeating it is a loop the caller writes
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

    /// Builds a task up before spawning it
    ///
    /// ## Behaviour
    /// The only way anything reaches the `Executor`. Every
    /// task that runs on the pool starts here, and what it
    /// does is decided by what is chained on before `spawn` —
    /// once now, once later, repeating back to back, repeating
    /// with a gap, on a fixed rate, and bounded by a count or
    /// a deadline or both. `block` is the only other way in,
    /// and it never goes near the pool at all
    ///
    /// There is a method per thing rather than a method per
    /// combination, which is the whole reason this is a chain.
    /// A repeating task at a priority of your choosing would
    /// otherwise need an entry point of its own, and so would
    /// every other pair
    ///
    /// The states are tracked in the type, so a combination
    /// with no meaning doesn't compile rather than being
    /// quietly ignored — asking for a gap where no repeat was
    /// asked for, or setting the same bound twice, is an error
    /// at the call site
    ///
    /// Nothing happens until `spawn` is called, so a builder
    /// that is dropped instead starts nothing
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
    /// exactly what `join` would have given for that task —
    /// including its error, so one task failing doesn't hide
    /// the others
    ///
    /// ## Behaviour
    /// Waits for them one after another, which costs nothing
    /// against waiting for them all at once: they are already
    /// running in parallel, and the last one to finish is the
    /// last one to finish whichever order they are read in
    pub fn join_all<T, I>(handles: I) -> Vec<Result<T, RuntimeError>>
    where
        I: IntoIterator<Item = TaskHandle<T>>,
        T: Clone,
    {
        handles.into_iter().map(|handle| handle.join()).collect()
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
    /// ## Behaviour
    /// The `Reactor` and the manager are supervised separately
    /// and fail separately, so they are reported separately.
    /// Every method on here that talks about what happens "if
    /// the manager goes" is describing a state this is how you
    /// detect
    ///
    /// #### Note
    /// A snapshot rather than a lock, like `workers`. Both
    /// supervisors carry on doing whatever they were doing
    /// while it is being looked at
    pub fn status() -> RuntimeStatus {
        let initialised = Self::initialised();

        RuntimeStatus {
            initialised,
            shut_down: executor::shutting_down(),

            // Both only mean anything once there has been an
            // initialisation to have survived. Before that the
            // ids hold whatever they were born with, which is
            // not the same as a queue that is up
            reactor_alive: initialised
                && REACTOR_KQUEUE_ID.load(Ordering::Relaxed) != DEAD_KQUEUE_ID,
            manager_alive: initialised && executor::manager_alive(),
        }
    }

    /// Stops the runtime for good
    ///
    /// ## Behaviour
    /// Drains rather than aborts. Nothing new gets in from the
    /// moment this is called — a spawn after it settles
    /// `Failed` straight away rather than blocking — and
    /// everything already queued still runs. Workers stop
    /// between tasks, never inside one, so a task in flight
    /// runs to the end and comes back to its listeners
    /// normally, and a task waiting its turn still gets one
    ///
    /// Blocks until the pool has nothing left to do, so a
    /// caller that comes back from this knows the work is
    /// finished rather than merely asked to finish
    ///
    /// Everything the drain can't reach is written off on the
    /// way out: a schedule waiting on a queue that has closed,
    /// a repeat between runs, a task in the ring of a worker
    /// that went down. Their listeners get an answer instead of
    /// blocking for the life of the process
    ///
    /// ## The `Reactor`
    /// Deliberately left up. `block` runs on the calling thread
    /// and is documented as uncancellable by any means, and a
    /// blocking call on a thread that couldn't get a queue of
    /// its own waits on the `Reactor` — closing it would break
    /// exactly the promise `block` makes. A blocking call
    /// during or after a shutdown still works
    ///
    /// ## It is one way
    /// The table's slots are handed back here, so starting
    /// again would give those ids to new tasks while old
    /// handles still hold them. `init` after this returns
    /// `ShutDown` rather than appearing to succeed
    ///
    /// #### Note
    /// Calling it twice is safe and does nothing the second
    /// time. The second caller comes straight back rather than
    /// tearing down a runtime somebody else is already tearing
    /// down — though it does *not* wait for the first one to
    /// finish draining
    ///
    /// #### Note
    /// The one way this doesn't come back is a task that never
    /// finishes. Draining means waiting for the work, and a
    /// task that runs forever is work that never ends
    ///
    /// For the same reason, don't call this from inside a
    /// spawned task. The drain waits for the pool to empty and
    /// the caller is itself the thing keeping it full, so it
    /// would be waiting on itself. Shut down from a thread the
    /// runtime isn't running
    pub fn shutdown() {
        executor::shutdown_now();
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

    /// Makes the manager come apart the next `count` times it
    /// goes round its loop
    ///
    /// ## Behaviour
    /// Fewer than the restart limit and the supervisor brings it
    /// back every time, timers and all. More and it gives up,
    /// closes its queue, and everything that was waiting on that
    /// queue is written off rather than left waiting for good
    ///
    /// #### Note
    /// Hidden, and here for the crate's own tests. The restart
    /// path has no other way to be reached — a manager only dies
    /// of a kernel refusing it a syscall or of a bug in here,
    /// and a test can ask for neither — so without this the one
    /// piece of machinery built entirely around surviving a
    /// failure is the one piece nothing ever exercises
    ///
    /// It is not a way to stop the runtime. Use it on a process
    /// you were finished with
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
