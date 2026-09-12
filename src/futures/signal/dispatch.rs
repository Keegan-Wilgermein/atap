//! # Dispatch
//! The counting handler behind every signal task, and which
//! signals the runtime has taken over
//!
//! A signal says nothing a task could look at, unlike a socket,
//! so the runtime counts deliveries itself. A task then compares
//! the count with what it last saw, which makes every wake
//! something it can check rather than something it has to trust

use crate::{RuntimeError, futures::signal::signal::SigReleasePolicy, modules::int_check::IntCheck};
use std::{
    fmt, mem, ptr,
    sync::atomic::{AtomicBool, AtomicU32, Ordering},
};

/// One past the highest signal the runtime will watch
const MAX_SIGNAL: usize = 32;

/// How many times each signal has arrived since the program
/// started
static COUNTS: [AtomicU32; MAX_SIGNAL] = [const { AtomicU32::new(0) }; MAX_SIGNAL];

/// Whether the runtime's handler is installed for each signal
static CAUGHT: [AtomicBool; MAX_SIGNAL] = [const { AtomicBool::new(false) }; MAX_SIGNAL];

/// Whether anything has asked to keep each signal for good
static HELD: [AtomicBool; MAX_SIGNAL] = [const { AtomicBool::new(false) }; MAX_SIGNAL];

/// Live watchers that want each signal handed back once they are
/// gone
static WATCHERS: [AtomicU32; MAX_SIGNAL] = [const { AtomicU32::new(0) }; MAX_SIGNAL];

/// Adds one to a signal's count
///
/// ## Behaviour
/// Everything a signal handler is allowed to do and no more: one
/// atomic add. Nothing here allocates, takes a lock, or calls
/// back into the runtime
extern "C" fn count_one(signo: libc::c_int) {
    if let Some(count) = COUNTS.get(signo as usize) {
        count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Takes a signal over, so it stops doing what it normally does
/// and starts being counted
///
/// ## Returns
/// The claim to hold for an `OnDrop` watcher, and nothing for one
/// that keeps the signal. `BadSignal` for a number the kernel
/// will never hand over
pub(crate) fn catch(
    signo: libc::c_int,
    policy: SigReleasePolicy,
) -> Result<Option<Watcher>, RuntimeError> {
    let slot = catchable(signo)?;

    // Before the handler, so nothing can hand the signal back
    // between installing it and this being known
    if policy == SigReleasePolicy::Hold {
        HELD[slot].store(true, Ordering::Release);
    }

    let watch = match policy {
        SigReleasePolicy::Hold => None,
        SigReleasePolicy::OnDrop => Some(Watcher::new(signo, slot)),
    };

    install(signo, slot)?;

    Ok(watch)
}

/// How many times a signal has arrived so far
#[inline(always)]
pub(crate) fn count(signo: libc::c_int) -> u32 {
    COUNTS
        .get(signo as usize)
        .map_or(0, |count| count.load(Ordering::Relaxed))
}

/// Hands a signal back to whatever it normally does
///
/// ## Behaviour
/// Takes away any claim to keep it, so a later `OnDrop` watcher
/// can hand it back again
pub(crate) fn release(signo: libc::c_int) -> Result<(), RuntimeError> {
    let slot = catchable(signo)?;

    HELD[slot].store(false, Ordering::Release);

    restore(signo, slot)
}

/// Whether a signal is one the runtime can take over
///
/// ## Returns
/// Its slot, or `BadSignal` for a number that isn't a signal or
/// one the kernel keeps for itself
fn catchable(signo: libc::c_int) -> Result<usize, RuntimeError> {
    let slot = sendable(signo)?;

    if signo == libc::SIGKILL || signo == libc::SIGSTOP {
        return Err(RuntimeError::BadSignal);
    }

    Ok(slot)
}

/// Whether a signal is one the kernel will take at all
///
/// `SIGKILL` and `SIGSTOP` can be sent, just never caught
pub(crate) fn sendable(signo: libc::c_int) -> Result<usize, RuntimeError> {
    if signo <= 0 || signo as usize >= MAX_SIGNAL {
        return Err(RuntimeError::BadSignal);
    }

    Ok(signo as usize)
}

/// Puts the counting handler on a signal, once per signal
fn install(signo: libc::c_int, slot: usize) -> Result<(), RuntimeError> {
    if CAUGHT[slot].swap(true, Ordering::AcqRel) {
        return Ok(());
    }

    let mut action: libc::sigaction = unsafe { mem::zeroed() };

    action.sa_sigaction = count_one as extern "C" fn(libc::c_int) as libc::sighandler_t;

    // Restarted rather than interrupted, so a signal arriving can't
    // break a syscall somewhere else in the runtime
    action.sa_flags = libc::SA_RESTART;

    unsafe { libc::sigemptyset(&mut action.sa_mask) };

    let set = unsafe { libc::sigaction(signo, &action, ptr::null_mut()) }.check();

    // Not caught after all, so the next task tries again
    if set.is_err() {
        CAUGHT[slot].store(false, Ordering::Release);
    }

    set?;

    Ok(())
}

/// Puts a signal's own behaviour back, if the runtime had taken
/// it
fn restore(signo: libc::c_int, slot: usize) -> Result<(), RuntimeError> {
    if !CAUGHT[slot].swap(false, Ordering::AcqRel) {
        return Ok(());
    }

    let mut action: libc::sigaction = unsafe { mem::zeroed() };

    action.sa_sigaction = libc::SIG_DFL;

    unsafe { libc::sigemptyset(&mut action.sa_mask) };
    unsafe { libc::sigaction(signo, &action, ptr::null_mut()) }.check()?;

    Ok(())
}

/// A task's claim on a signal it wants handed back once nothing
/// is watching it
///
/// Every copy of the task holds one, and a parked task holds its
/// own, so nothing is handed back underneath a task still waiting
pub(crate) struct Watcher {
    /// The signal claimed
    signo: libc::c_int,

    /// Where its numbers live
    slot: usize,
}

impl Watcher {
    /// Takes a claim on a signal
    fn new(signo: libc::c_int, slot: usize) -> Self {
        WATCHERS[slot].fetch_add(1, Ordering::AcqRel);

        Self { signo, slot }
    }
}

/// A copy of the task is another watcher
impl Clone for Watcher {
    fn clone(&self) -> Self {
        Self::new(self.signo, self.slot)
    }
}

/// The last one out hands the signal back, unless something asked
/// to keep it
impl Drop for Watcher {
    fn drop(&mut self) {
        if WATCHERS[self.slot].fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }

        if HELD[self.slot].load(Ordering::Acquire) {
            return;
        }

        let _ = restore(self.signo, self.slot);
    }
}

impl fmt::Debug for Watcher {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Watcher").field("signal", &self.signo).finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Only a real signal the kernel will hand over can be caught
    #[test]
    fn uncatchable_signals_are_refused() {
        assert_eq!(catchable(libc::SIGKILL), Err(RuntimeError::BadSignal));
        assert_eq!(catchable(libc::SIGSTOP), Err(RuntimeError::BadSignal));
        assert_eq!(catchable(0), Err(RuntimeError::BadSignal));
        assert_eq!(catchable(-1), Err(RuntimeError::BadSignal));
        assert_eq!(catchable(MAX_SIGNAL as libc::c_int), Err(RuntimeError::BadSignal));
        assert!(catchable(libc::SIGUSR1).is_ok());
    }

    /// The two that can't be caught can still be sent
    #[test]
    fn the_uncatchable_can_still_be_sent() {
        assert!(sendable(libc::SIGKILL).is_ok());
        assert!(sendable(libc::SIGSTOP).is_ok());
        assert_eq!(sendable(0), Err(RuntimeError::BadSignal));
    }
}
