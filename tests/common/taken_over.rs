//! # Taken over

use atap::signal::SignalKind;
use std::{mem, ptr};

/// Whether anything but the signal's own behaviour is installed
pub fn taken_over(kind: SignalKind) -> bool {
    let mut action: libc::sigaction = unsafe { mem::zeroed() };

    unsafe { libc::sigaction(kind.number(), ptr::null(), &mut action) };

    action.sa_sigaction != libc::SIG_DFL
}
