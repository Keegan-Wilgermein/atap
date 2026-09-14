//! # Help
//! What a worker does while a task it is running waits on another
//! task: runs queued work itself, rather than holding its thread
//!
//! A task that spawns and joins tasks inside itself would otherwise
//! hold one worker per level, and a chain deep enough would hold
//! every worker there is. Helping keeps the chain on the threads it
//! already has. Past a depth limit the worker waits instead, and a
//! new worker is started for the queued work it can't take

use crate::{
    constants::{HELP_DEPTH, HELP_POLL, STRANDED_POLL},
    executor,
    modules::{
        faults,
        task_data::QUEUED_LOCAL,
        worker::Worker,
        worker_pool::{POOL, Start},
    },
};
use std::{
    cell::Cell,
    ptr,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

thread_local! {
    /// The worker this thread runs, or null on any other thread
    static WORKER: Cell<*const Worker> = const { Cell::new(ptr::null()) };

    /// How many helped runs deep this thread is right now
    static DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// How deep a worker may help, which a test can lower
static LIMIT: AtomicUsize = AtomicUsize::new(HELP_DEPTH);

/// The first slice a waiting worker sleeps for, which doubles each
/// time nothing turns up
const FIRST_SLICE: Duration = Duration::from_micros(50);

/// Says this thread is `worker`, for the rest of its life
pub(crate) fn enter(worker: &'static Worker) {
    WORKER.with(|current| current.set(worker));
}

/// Lowers how deep a worker may help, zero for not at all
///
/// Never above `HELP_DEPTH`, which is all the room a worker has to
/// record what it is helping with
pub(crate) fn limit_depth(depth: usize) {
    LIMIT.store(depth.min(HELP_DEPTH), Ordering::SeqCst);
}

/// How deep a worker may take on work unrelated to what it waits on
#[inline(always)]
fn unrelated_limit(limit: usize) -> usize {
    limit / 2
}

/// The worker this thread runs, if it is one
#[inline(always)]
pub(crate) fn worker() -> Option<&'static Worker> {
    // Only ever set from a `&'static Worker`
    unsafe { WORKER.with(Cell::get).as_ref() }
}

/// Runs one queued task on this thread, if it is a worker with depth
/// left to help at
///
/// ## Returns
/// Whether a task was run
pub(crate) fn help_once() -> bool {
    let Some(worker) = worker() else {
        return false;
    };

    let depth = DEPTH.with(Cell::get);
    let limit = LIMIT.load(Ordering::Relaxed);

    if depth >= limit {
        return false;
    }

    // Past half way, only the task in the LIFO slot, which is most likely
    // the one being waited on. Anything else could be a whole tree of its
    // own, piled on top of this one
    let Some(id) = POOL.find_help(worker, depth < unrelated_limit(limit)) else {
        return false;
    };

    run_nested(worker, depth, id);

    true
}

/// Runs the task being waited on right here, if this thread is a worker
/// with depth left and the task is still sitting in a worker's ring or
/// LIFO slot
///
/// A task that spawns and then waits on what it spawned runs it itself,
/// so recursion goes down one branch at a time rather than holding a
/// worker at every level
///
/// ## Returns
/// Whether it ran
pub(crate) fn run_awaited(id: usize) -> bool {
    let Some(worker) = worker() else {
        return false;
    };

    let depth = DEPTH.with(Cell::get);

    if depth >= LIMIT.load(Ordering::Relaxed) {
        return false;
    }

    // Only from a local queue, which holds ids, so the entry left behind is
    // stepped over. A task in the shared queue stays there
    if !executor::slot(id).is_some_and(|data| data.claim_queued(QUEUED_LOCAL)) {
        return false;
    }

    run_nested(worker, depth, id);

    true
}

/// Runs a claimed task inside whatever this worker is already running,
/// `depth` runs down
fn run_nested(worker: &'static Worker, depth: usize, id: usize) {
    // Noted before it runs, so a worker that dies part way through leaves
    // a note of every task it went down holding
    worker.hold_nested(depth, id);

    faults::worker_dies();

    DEPTH.with(|current| current.set(depth + 1));

    executor::run(id);

    DEPTH.with(|current| current.set(depth));

    worker.put_down_nested(depth);
}

/// How a wait on another task sleeps, for a thread that may be able
/// to do something better
pub(crate) struct Patience {
    /// How long the next sleep on a worker is
    slice: Duration,

    /// Whether this wait is counted as a blocked worker
    blocked: bool,

    /// The worker this wait was counted against, once it has slept
    worker: Option<&'static Worker>,
}

impl Patience {
    /// A wait that hasn't slept yet
    pub(crate) const fn new() -> Self {
        Self {
            slice: FIRST_SLICE,
            blocked: false,
            worker: None,
        }
    }

    /// Says a task was helped with, so the next sleep starts short
    /// again
    #[inline(always)]
    pub(crate) fn helped(&mut self) {
        self.slice = FIRST_SLICE;
    }

    /// How long the next sleep may last, or `None` for as long as it
    /// takes
    ///
    /// ## Behaviour
    /// A worker sleeps in short slices that grow to `HELP_POLL`, so it
    /// keeps looking for work to help with. One too deep to take queued
    /// work counts as blocked, and once every worker is blocked
    /// with work still queued, a worker is started to take that work.
    /// A worker that is only waiting leaves the work to its peers, so
    /// the pool grows only as far as nothing else can move
    ///
    /// With no manager, nothing sleeps longer than `STRANDED_POLL`, and
    /// each wake makes sure no dead thread has stranded what is being
    /// waited on
    pub(crate) fn slice(&mut self) -> Option<Duration> {
        let mut slice = None;

        if let Some(worker) = worker() {
            // So the manager doesn't take a worker between looks for work
            // for one stuck in a call
            if self.worker.is_none() {
                worker.wait_began();
                self.worker = Some(worker);
            }

            // Too deep to take queued work, so nothing queued can move on
            // this thread until the wait ends
            let deep = DEPTH.with(Cell::get) >= unrelated_limit(LIMIT.load(Ordering::Relaxed));

            if deep {
                if !self.blocked {
                    self.blocked = true;

                    POOL.blocked_in();
                }

                // The cheap check first, since the queue check walks the rings
                if POOL.all_blocked() && POOL.has_queued_work() {
                    let _ = POOL.start_one(Start::Replace);
                }
            }

            slice = Some(self.slice);

            self.slice = (self.slice * 2).min(HELP_POLL);
        }

        if !executor::manager_alive() {
            POOL.heal_if_needed();

            slice = Some(slice.map_or(STRANDED_POLL, |slice| slice.min(STRANDED_POLL)));
        }

        slice
    }
}

impl Drop for Patience {
    /// Stops counting the wait as blocked, however it ends, including a
    /// thread going down part way through it
    fn drop(&mut self) {
        if self.blocked {
            POOL.blocked_out();
        }

        if let Some(worker) = self.worker {
            worker.wait_ended();
        }
    }
}
