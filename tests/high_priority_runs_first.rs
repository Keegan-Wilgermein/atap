use atap::{Runtime, Sleep};
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// A high priority task is served ahead of a queued batch
#[test]
fn high_priority_runs_first() {
    Runtime::init();

    let tasks = 50_000;
    let started = Instant::now();

    let queued: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50))).spawn())
        .collect();

    // Last in, and served first anyway
    let queued_at = Instant::now();
    let urgent = Runtime::task(Sleep::sleep(Duration::from_micros(50))).priority(255).spawn();

    while !urgent.settled() {
        thread::yield_now();
    }

    let waited = queued_at.elapsed();

    for handle in queued {
        handle.join().expect("every task finishes");
    }

    let total = started.elapsed();

    let workers = Runtime::workers().len();

    println!(
        "urgent task waited {:?}, the {} before it took {:?}",
        waited, tasks, total,
    );

    println!("There were {} workers running at the time", workers);

    // Without priority the last task spawned would wait out the
    // whole batch
    assert!(
        waited * 4 < total,
        "the top priority task waited {:?} of the batch's {:?}",
        waited,
        total,
    );
}
