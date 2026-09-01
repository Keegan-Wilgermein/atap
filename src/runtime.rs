//! # Runtime
//! 
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use std::{sync::{atomic::{AtomicBool, AtomicI32, Ordering}, mpsc}, thread, time::Instant};
use crate::{RuntimeError, constants::{DEAD_KQUEUE_ID, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW}, futures::task::Task, modules::{int_check::IntCheck, pending::Pending, reactor::Reactor}};

/// Whether the runtime has been initialised yet
/// 
/// Use `SeqCst` operations only as
/// it is important that a `Runtime` only gets
/// initialised once
static INIT: AtomicBool = AtomicBool::new(false);

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
        // No-op if already initialised
        if INIT.load(Ordering::SeqCst) {
            return None;
        }

        // Store the initilised value first so another thread can't
        // start another initialisation at the same time
        INIT.store(true, Ordering::SeqCst);

        init_runtime()?;
        None
    }

    /// Blocking call
    /// 
    /// Used when you need the data
    /// the instant it arrives and
    /// don't mind waiting for it
    #[inline(always)]
    pub fn block_on<F>(task: F) -> F::Output
    where
        F: Task,
    {
        // Only used for sleep functions, but must be called
        // anyway because an if statement will add latency
        // that can't be tracked with this call
        let called_at = Instant::now();

        let reactor_id = REACTOR_KQUEUE_ID.load(Ordering::Relaxed);
        let out = task.execute(reactor_id, called_at);

        out
    }

    #[inline(always)]
    pub fn spawn<F>(task: F) -> Pending<F::Output>
    where
        F: Task,
        F::Output: Clone,
    {
        Pending::new()
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
                },
            };

            REACTOR_KQUEUE_ID.store(new_id, Ordering::SeqCst);
            Reactor::init(new_id, tx.clone());

            let _ = unsafe { libc::close(dead_id) };
            started = Instant::now();
        }
    });

    None
}
