//! # CPU time

use std::time::Duration;

/// Cpu time this process has been charged, user and system
pub fn cpu_time() -> Duration {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };

    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Duration::ZERO;
    }

    let seconds = |time: libc::timeval| {
        Duration::from_secs(time.tv_sec as u64) + Duration::from_micros(time.tv_usec as u64)
    };

    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}
