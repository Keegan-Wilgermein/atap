//! # Resources

use std::{fmt, mem, os::raw::c_int, time::Duration};

/// `RUSAGE_INFO_V4` from `<libproc.h>`
const RUSAGE_INFO_V4: c_int = 4;

unsafe extern "C" {
    fn proc_pid_rusage(pid: c_int, flavor: c_int, buffer: *mut RusageInfoV4) -> c_int;
}

/// The start of `rusage_info_v4`, far enough to reach the footprint,
/// padded out to the size the kernel writes
#[repr(C)]
struct RusageInfoV4 {
    uuid: [u8; 16],
    user_time: u64,
    system_time: u64,
    pkg_idle_wkups: u64,
    interrupt_wkups: u64,
    pageins: u64,
    wired_size: u64,
    resident_size: u64,
    phys_footprint: u64,
    rest: [u64; 32],
}

/// The cpu and memory the process is using at one moment
#[derive(Clone, Copy)]
pub struct Resources {
    /// Cpu time charged so far, user and system
    pub cpu: Duration,

    /// Memory the process is charged for right now, the number
    /// Activity Monitor shows
    pub footprint: u64,

    /// Resident memory right now
    pub resident: u64,

    /// The most resident memory it has ever had
    pub peak_resident: u64,

    /// Threads the process has right now
    pub threads: u64,
}

impl Resources {
    /// What the process is using now
    pub fn now() -> Self {
        let mut info: RusageInfoV4 = unsafe { mem::zeroed() };

        let read = unsafe { proc_pid_rusage(libc::getpid(), RUSAGE_INFO_V4, &mut info) } == 0;

        let (footprint, resident) = match read {
            true => (info.phys_footprint, info.resident_size),
            false => (0, 0),
        };

        Self {
            cpu: super::cpu_time(),
            footprint,
            resident,
            peak_resident: super::max_rss() as u64,
            threads: threads(),
        }
    }

    /// Cpu used between `earlier` and this, as a share of one core over
    /// `wall`
    pub fn cpu_share_since(&self, earlier: &Self, wall: Duration) -> f64 {
        let spent = self.cpu.saturating_sub(earlier.cpu);

        spent.as_secs_f64() / wall.as_secs_f64().max(f64::EPSILON) * 100.0
    }
}

impl fmt::Display for Resources {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cpu {:.3}s, footprint {:.1} MiB, resident {:.1} MiB (peak {:.1} MiB), {} threads",
            self.cpu.as_secs_f64(),
            mebibytes(self.footprint),
            mebibytes(self.resident),
            mebibytes(self.peak_resident),
            self.threads,
        )
    }
}

/// Bytes as mebibytes
pub fn mebibytes(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Threads in this process right now
fn threads() -> u64 {
    let mut info: libc::proc_taskinfo = unsafe { mem::zeroed() };
    let size = mem::size_of::<libc::proc_taskinfo>() as c_int;

    let written = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDTASKINFO,
            0,
            (&mut info as *mut libc::proc_taskinfo).cast(),
            size,
        )
    };

    match written == size {
        true => info.pti_threadnum as u64,
        false => 0,
    }
}

/// The memory the process is charged for right now, in bytes
///
/// One syscall, for a caller reading it over and over
pub fn footprint() -> u64 {
    let mut info: RusageInfoV4 = unsafe { mem::zeroed() };

    match unsafe { proc_pid_rusage(libc::getpid(), RUSAGE_INFO_V4, &mut info) } == 0 {
        true => info.phys_footprint,
        false => 0,
    }
}
