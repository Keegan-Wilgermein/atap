//! Runtime builder tests
//!
//! The first one inits the runtime with sizes of its own, so it
//! needs a binary to itself

mod common;

use atap::{Runtime, RuntimeError, compute::Compute};
use common::cores;

/// Workers per core, sleep threads per core and the worker stack
/// are the sizes the pool starts with
#[test]
fn tuned_sizes_reach_the_pool() {
    Runtime::builder()
        .workers_per_core(2)
        .sleep_threads_per_core(3)
        .worker_stack(1024 * 1024)
        .init()
        .expect("the runtime starts");

    let stats = Runtime::pool();

    println!(
        "{} cores gave a worker target of {} and a sleep target of {}, ceiling {}",
        cores(),
        stats.target(),
        stats.sleep_target(),
        stats.ceiling(),
    );

    assert_eq!(
        stats.target(),
        (cores() * 2).min(stats.ceiling()),
        "two workers per core should be the target",
    );

    assert_eq!(
        stats.sleep_target(),
        (cores() * 3).min(stats.ceiling()),
        "three sleep threads per core should be the target",
    );

    let handles: Vec<_> = (0..64)
        .map(|value| Runtime::task(Compute::compute(move |()| value * 2)).spawn())
        .collect();

    for (value, handle) in handles.into_iter().enumerate() {
        assert_eq!(
            handle.join().expect("every task finishes"),
            value * 2,
            "a tuned pool still runs its tasks",
        );
    }

    let again = Runtime::builder()
        .workers_per_core(1)
        .sleep_threads_per_core(1)
        .init();

    assert!(
        matches!(again, Err(RuntimeError::AlreadyInit)),
        "a second init reads AlreadyInit, got {again:?}",
    );

    assert_eq!(
        Runtime::pool().target(),
        (cores() * 2).min(stats.ceiling()),
        "a refused init should leave the sizes alone",
    );
}

/// Sizes no pool could run with are refused, and nothing starts
#[test]
fn sizes_a_pool_cannot_run_with_are_refused() {
    for builder in [
        Runtime::builder().workers_per_core(0),
        Runtime::builder().sleep_threads_per_core(0),
        Runtime::builder().worker_stack(512),
    ] {
        let started = builder.init();

        assert!(
            matches!(started, Err(RuntimeError::BadArgument)),
            "a size a pool can't run with reads BadArgument, got {started:?}",
        );
    }
}
