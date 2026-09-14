//! # Descriptor limit

/// Lifts the soft limit on open descriptors as far as the hard
/// limit allows, so running out isn't counted as a fault
///
/// ## Returns
/// The limit it ended up with, for the log
pub fn raise_descriptor_limit() -> u64 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };

    // `OPEN_MAX`, which is as high as macOS lets a process ask for
    let wanted = limit.rlim_max.min(10_240);

    if wanted > limit.rlim_cur {
        let raised = libc::rlimit {
            rlim_cur: wanted,
            rlim_max: limit.rlim_max,
        };

        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) };
    }

    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };

    limit.rlim_cur
}
