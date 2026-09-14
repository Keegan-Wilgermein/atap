//! # Faults
//! Failures the crate's own tests can ask for, so the paths that
//! recover from them run on demand
//!
//! Each check costs a thread one load while nothing is owed

use std::{
    any::Any,
    panic,
    sync::atomic::{AtomicU32, Ordering},
};

/// What an injected thread death unwinds with
///
/// Told apart from a task's own panic, so no run it passes through
/// takes it for the task failing and stops it there
pub(crate) struct ThreadDeath;

/// Whether a caught panic is a thread going down rather than a task
#[inline(always)]
pub(crate) fn is_thread_death(payload: &(dyn Any + Send)) -> bool {
    payload.is::<ThreadDeath>()
}

/// Worker deaths still owed
static WORKER_DEATHS: AtomicU32 = AtomicU32::new(0);

/// Sleep thread deaths still owed
static SLEEP_DEATHS: AtomicU32 = AtomicU32::new(0);

/// Refused thread starts still owed
static SPAWN_REFUSALS: AtomicU32 = AtomicU32::new(0);

/// Owes `workers` worker deaths and `sleeps` sleep thread deaths
pub(crate) fn owe_thread_deaths(workers: u32, sleeps: u32) {
    WORKER_DEATHS.store(workers, Ordering::SeqCst);
    SLEEP_DEATHS.store(sleeps, Ordering::SeqCst);
}

/// Owes `count` refused thread starts
pub(crate) fn owe_spawn_refusals(count: u32) {
    SPAWN_REFUSALS.store(count, Ordering::SeqCst);
}

/// Takes one of the owed faults, if any are owed
#[inline(always)]
fn take(owed: &AtomicU32) -> bool {
    if owed.load(Ordering::Relaxed) == 0 {
        return false;
    }

    owed.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| match left {
        0 => None,
        left => Some(left - 1),
    })
    .is_ok()
}

/// Brings this worker down if a death is owed
///
/// Checked inside a task's run while the worker helps, so it unwinds
/// with a payload every run lets through
#[inline(always)]
pub(crate) fn worker_dies() {
    if take(&WORKER_DEATHS) {
        panic::panic_any(ThreadDeath);
    }
}

/// Brings this sleep thread down if a death is owed
#[inline(always)]
pub(crate) fn sleep_thread_dies() {
    if take(&SLEEP_DEATHS) {
        panic::panic_any(ThreadDeath);
    }
}

/// Whether this attempt to start a thread should be refused
#[inline(always)]
pub(crate) fn spawn_refused() -> bool {
    take(&SPAWN_REFUSALS)
}
