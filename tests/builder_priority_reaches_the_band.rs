use atap::{Runtime, Sleep};
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// Priority set through the builder gets a task served ahead
/// of a queued batch
#[test]
fn builder_priority_reaches_the_band() {
    Runtime::init();

    let tasks = 10_000;
    let started = Instant::now();

    let queued: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50), true)).spawn())
        .collect();

    // Last in, and served first anyway
    let queued_at = Instant::now();
    let urgent = Runtime::task(Sleep::sleep(Duration::from_micros(50), true))
        .priority(255)
        .spawn();

    while !urgent.settled() {
        thread::yield_now();
    }

    let waited = queued_at.elapsed();

    for handle in queued {
        handle.join().expect("every task finishes");
    }

    let total = started.elapsed();

    println!(
        "urgent task waited {:?}, the {} before it took {:?}",
        waited, tasks, total,
    );

    assert!(
        waited * 4 < total,
        "the top priority task waited {waited:?} of the batch's {total:?}",
    );
}
