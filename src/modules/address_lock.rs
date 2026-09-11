//! # Address Lock
//! Blocking a thread on the value of a word, and waking
//! everything blocked on one
//!
//! #### Note
//! A wait only sleeps while the word still reads the value it
//! was given, so a wake landing between the caller's read and
//! its wait is never lost

use crate::RuntimeError;
use libc::c_void;
use std::{io::Error, mem, sync::atomic::AtomicU32, time::Duration};

/// Blocks while a word still reads `value`
///
/// ## Returns
/// `Ok` when the word may have changed and the caller should
/// look again, and an error only when the kernel refused in a
/// way that going round again won't fix
pub(crate) fn wait(address: *mut c_void, value: u32) -> Result<(), RuntimeError> {
    let status = unsafe {
        libc::os_sync_wait_on_address(
            address,
            value as u64,                       // Sleep only while it still reads this
            mem::size_of::<u32>(),              // A single word, whatever it holds
            libc::OS_SYNC_WAIT_ON_ADDRESS_NONE, // Single process waiting
        )
    };

    if status >= 0 {
        return Ok(());
    }

    let error = Error::last_os_error().raw_os_error();

    // A signal, or a value that moved, both mean look again
    if error == Some(libc::EINTR) || error == Some(libc::EAGAIN) {
        return Ok(());
    }

    Err(RuntimeError::AddressLock)
}

/// Blocks while a word still reads `value`, for at most
/// `timeout`
///
/// ## Returns
/// `Ok(true)` when the word may have changed, `Ok(false)` when
/// the timeout ran out, and an error only when the kernel
/// refused in a way that going round again won't fix
///
/// A zero timeout returns `Ok(false)` without waiting
pub(crate) fn wait_until(
    address: *mut c_void,
    value: u32,
    timeout: Duration,
) -> Result<bool, RuntimeError> {
    let nanos = timeout.as_nanos();

    if nanos == 0 {
        return Ok(false);
    }

    let status = unsafe {
        libc::os_sync_wait_on_address_with_timeout(
            address,
            value as u64,                       // Sleep only while it still reads this
            mem::size_of::<u32>(),              // A single word, whatever it holds
            libc::OS_SYNC_WAIT_ON_ADDRESS_NONE, // Single process waiting
            libc::OS_CLOCK_MACH_ABSOLUTE_TIME,  // Monotonic, so setting the clock can't move it
            // Saturated, so a huge timeout can't wrap to a tiny one
            nanos.min(u64::MAX as u128) as u64,
        )
    };

    if status >= 0 {
        return Ok(true);
    }

    let error = Error::last_os_error().raw_os_error();

    // The time ran out
    if error == Some(libc::ETIMEDOUT) {
        return Ok(false);
    }

    // A signal, or a value that moved, both mean look again
    if error == Some(libc::EINTR) || error == Some(libc::EAGAIN) {
        return Ok(true);
    }

    Err(RuntimeError::AddressLock)
}

/// Wakes everything blocked on a word
pub(crate) fn wake(address: *mut c_void) {
    let _ = unsafe {
        libc::os_sync_wake_by_address_all(
            address,
            mem::size_of::<u32>(), // A single word, whatever it holds
            libc::OS_SYNC_WAKE_BY_ADDRESS_NONE, // Single process waiting
        )
    };
}

/// The address of a word, in the shape the kernel wants it
#[inline(always)]
pub(crate) fn address(word: &AtomicU32) -> *mut c_void {
    word as *const AtomicU32 as *mut c_void
}
