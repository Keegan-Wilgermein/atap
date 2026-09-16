//! # Deep recursion
//! A chain of computes each waiting on the next, far deeper than a
//! worker helps, comes back without holding every worker there is
//!
//! Lowers how deep workers help and reads the pool's size, both of
//! which are process wide, so it has a binary to itself

mod common;

use atap::{Compute, Runtime};
use common::{cores, report, settles};
use std::time::{Duration, Instant};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(60);

/// A chain of `depth` computes, each spawning and joining the next
fn chain(depth: usize) -> usize {
    if depth == 0 {
        return 0;
    }

    Runtime::task(Compute::compute(move |()| chain(depth - 1)))
        .spawn()
        .join()
        .expect("a link in the chain failed")
        + 1
}

/// Workers past their help depth are replaced, the chain finishes, and
/// the extra workers are reaped afterwards
#[test]
fn deep_recursion_replaces_blocked_workers() {
    let _ = Runtime::init();

    let before = Runtime::workers();

    report("before");

    // Chains at every help depth, down to none at all, where every link
    // holds a worker and only replacement moves the chain on
    for depth_limit in [16, 4, 1, 0] {
        Runtime::inject_help_depth(depth_limit);

        let depth = 64;
        let chains = cores().max(2);
        let started = Instant::now();

        let roots: Vec<_> = (0..chains)
            .map(|_| Runtime::task(Compute::compute(move |()| chain(depth))).spawn())
            .collect();

        for root in roots {
            assert_eq!(
                root.join_with_timeout(PATIENCE),
                Ok(depth),
                "a chain {} deep with a help depth of {} came back wrong",
                depth,
                depth_limit,
            );
        }

        let stats = Runtime::workers();

        println!(
            "{} chains {} deep, help depth {}: {:?}, {} workers now, peak {}, target {}, ceiling {}",
            chains,
            depth,
            depth_limit,
            started.elapsed(),
            stats.len(),
            stats.peak_workers(),
            stats.target(),
            stats.ceiling(),
        );
    }

    let after = Runtime::workers();

    report("every chain back");

    assert!(
        after.peak_workers() > before.target(),
        "chains holding a worker per link never grew the pool past its target of {}",
        before.target(),
    );

    assert!(
        after.peak_workers() <= after.ceiling(),
        "the pool grew to {} workers, past its ceiling of {}",
        after.peak_workers(),
        after.ceiling(),
    );

    Runtime::inject_help_depth(16);

    // Every worker started to stand in for a blocked one is reaped
    // once nothing is blocked
    assert!(
        settles_long(|| Runtime::workers().len() <= Runtime::workers().target()),
        "{} workers still running, past the target of {}, long after the chains finished",
        Runtime::workers().len(),
        Runtime::workers().target(),
    );

    report("reaped");

    assert!(
        settles(|| Runtime::healthy()),
        "the runtime wasn't healthy after deep recursion: {:?}",
        Runtime::status(),
    );
}

/// `settles`, with room for the reaper's pace
fn settles_long(mut condition: impl FnMut() -> bool) -> bool {
    let waited = Instant::now();

    while waited.elapsed() < Duration::from_secs(30) {
        if condition() {
            return true;
        }

        std::thread::sleep(Duration::from_millis(50));
    }

    condition()
}
