//! # Max RSS

use std::mem;

/// The high water mark of the process's resident memory
pub fn max_rss() -> usize {
    let mut usage: libc::rusage = unsafe { mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };

    usage.ru_maxrss as usize
}
