//! Thread policy
//! Moves the calling thread into the kernel's realtime band
//! so the spin can't be preempted or migrated onto an
//! efficiency core partway through
//!
//! Scheduling policy is a property of a thread, applied by that
//! thread to itself, so there is no central registry to keep.
//! Every thread that sleeps in p_mode promotes itself once, on
//! its first call, and the flag lives in thread local storage

use crate::constants::SLEEP_TOLERANCE;
use std::cell::Cell;

thread_local! {
    /// Whether this thread has already been promoted
    ///
    /// Thread local so each thread tracks its own state
    /// without any shared synchronisation
    static PROMOTED: Cell<bool> = const { Cell::new(false) };
}

/// Promotes the calling thread into the realtime band
///
/// A no-op after the first call on any given thread
///
/// ## Behaviour
/// Best effort. If the kernel refuses the policy the thread
/// keeps its default scheduling and sleeps still work, just
/// with more jitter, so there is nothing useful to propagate
///
/// #### Note
/// The promotion lasts for the life of the thread. Passing
/// `false` to a later sleep on the same thread doesn't undo it
#[inline(always)]
pub(crate) fn promote() {
    PROMOTED.with(|promoted| {
        if promoted.get() {
            return;
        }

        // Set before applying so a failed promotion
        // isn't retried on every single call
        promoted.set(true);
        apply();
    });
}

/// Applies `THREAD_TIME_CONSTRAINT_POLICY` to the calling thread
fn apply() {
    let ticks = ticks_per_nano();

    if ticks <= 0.0 {
        return;
    }

    // The spin window is the only part that has to run
    // uninterrupted, so it is the whole computation budget
    let computation = (SLEEP_TOLERANCE.as_nanos() as f64 * ticks) as u32;

    let mut policy = libc::thread_time_constraint_policy {
        period: 0,               // Non periodic, sleeps arrive whenever they arrive
        computation,             // CPU time needed once scheduled
        constraint: computation, // Deadline to finish it by, tightest possible
        preemptible: 0,          // The spin is worthless if it can be preempted
    };

    let _ = unsafe {
        libc::thread_policy_set(
            libc::pthread_mach_thread_np(libc::pthread_self()),
            libc::THREAD_TIME_CONSTRAINT_POLICY as libc::thread_policy_flavor_t,
            &mut policy as *mut _ as libc::thread_policy_t,
            libc::THREAD_TIME_CONSTRAINT_POLICY_COUNT,
        )
    };
}

/// How many `mach_absolute_time` ticks make up a nanosecond
///
/// Returns `0.0` if the kernel doesn't answer, which `apply()`
/// treats as a reason to leave the thread alone
#[allow(deprecated)]
fn ticks_per_nano() -> f64 {
    let mut info = libc::mach_timebase_info { numer: 0, denom: 0 };
    unsafe { libc::mach_timebase_info(&mut info) };

    if info.numer == 0 {
        return 0.0;
    }

    // ticks * numer / denom == nanos, so invert it
    return info.denom as f64 / info.numer as f64;
}
