//! # Blocking computes
//! A compute marked `.blocking()` waits on a sleep thread, leaving the
//! workers free for the computes that need a core
//!
//! Reads the pool's busy counts, which any other test would move, so
//! it has a binary to itself

mod common;

use atap::{Runtime, compute::Compute};
use common::{cores, report, settles};
use std::{
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(30);

/// Blocking computes pile onto sleep threads while plain computes keep
/// running on the workers alongside them
#[test]
fn blocking_computes_wait_on_sleep_threads() {
    let _ = Runtime::init();

    let waits = cores() * 4;
    let wait = Duration::from_millis(200);

    assert!(
        settles(|| Runtime::pool().busy() == 0 && Runtime::pool().sleep_busy() == 0),
        "the pool was already busy before the test began"
    );

    report("before");

    let started = Instant::now();

    let waiting: Vec<_> = (0..waits)
        .map(|index| {
            Runtime::task(
                Compute::compute(move |()| {
                    thread::sleep(wait);
                    index
                })
                .blocking(),
            )
            .spawn()
        })
        .collect();

    // Given long enough for every blocking compute to have found a thread
    thread::sleep(wait / 4);

    let during = Runtime::pool();

    report("blocking computes waiting");

    // Plain computes still get cores while every one of those waits
    let answered_at = Instant::now();

    let quick: Vec<_> = (0..256u64)
        .map(|value| Runtime::task(Compute::compute(move |()| value * 2)).spawn())
        .collect();

    for (value, handle) in quick.into_iter().enumerate() {
        assert_eq!(handle.join_with_timeout(PATIENCE), Ok(value as u64 * 2));
    }

    let answered = answered_at.elapsed();

    for (index, handle) in waiting.into_iter().enumerate() {
        assert_eq!(handle.join_with_timeout(PATIENCE), Ok(index));
    }

    let took = started.elapsed();

    report("after");

    println!(
        "{} blocking computes of {:?} took {:?} together, {} sleep threads busy at once, \
         {} workers busy, and 256 plain computes answered in {:?} alongside them",
        waits,
        wait,
        took,
        during.sleep_busy(),
        during.busy(),
        answered,
    );

    assert!(
        during.sleep_busy() >= cores(),
        "only {} sleep threads were busy with {} blocking computes waiting",
        during.sleep_busy(),
        waits,
    );

    assert!(
        during.busy() < cores().max(2),
        "{} workers were held by computes marked blocking",
        during.busy(),
    );

    assert!(
        took < wait * (waits as u32) / 2,
        "{} blocking computes took {:?}, so they waited one after another",
        waits,
        took,
    );

    assert!(
        answered < wait,
        "plain computes took {:?} to answer, so the blocking ones held the workers",
        answered,
    );
}
