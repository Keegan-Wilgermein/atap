//! # Runtime
//! 
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use std::{sync::atomic::{AtomicBool, AtomicI32, Ordering}, time::Instant};
use crate::{futures::task::Task, modules::{event_type::EventType, int_check::IntCheck, reactor::Reactor}};

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
    /// 
    /// ## Panics
    /// Runtime initialisation can panic if registering an event to `libc::kqueue`
    /// returns a negative value
    pub fn init() {
        // No-op if already initialised
        if INIT.load(Ordering::SeqCst) {
            return;
        }

        // Store the initilised value first so another thread can't
        // start another initialisation at the same time
        INIT.store(true, Ordering::SeqCst);

        let reactor_id = unsafe { libc::kqueue() }.check();
        REACTOR_KQUEUE_ID.store(reactor_id, Ordering::SeqCst);

        init_runtime(reactor_id);
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
        let called_at = Instant::now();
        let reactor_id = if task.as_event() == EventType::Sleep {
            0
        } else {
            REACTOR_KQUEUE_ID.load(Ordering::Relaxed)
        };

        let out = task.execute(reactor_id, called_at);

        out
    }
}

/// The real non user facing init function
/// 
/// Called by the `Runtime::init()` method only
fn init_runtime(id: i32) {
    Reactor::init(id);
}
