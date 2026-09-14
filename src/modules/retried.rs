//! # Retried
//! Running a syscall again when a signal interrupts it

use crate::{RuntimeError, modules::int_check::IntCheck};

/// Runs a syscall until it says something other than `EINTR`
pub(crate) fn retried(mut call: impl FnMut() -> libc::c_int) -> Result<libc::c_int, RuntimeError> {
    loop {
        match call().check() {
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            other => return other,
        }
    }
}
