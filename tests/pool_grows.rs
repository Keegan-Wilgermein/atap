mod common;

use atap::{Runtime, Sleep, SleepMode};
use common::cores;
use std::{
    thread,
    time::{Duration, Instant},
};

/// Blocking tasks grow the sleep threads, and run side by side
#[test]
fn pool_grows_under_blocking_load() {
    Runtime::init();

    let tasks = cores() * 4;
    let duration = Duration::from_millis(400);

    let started = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(duration).mode(SleepMode::Relaxed)).spawn())
        .collect();

    // Long enough for the offloads to have found threads, and
    // far short of the sleeps finishing
    thread::sleep(Duration::from_millis(100));

    let stats = Runtime::workers();

    println!(
        "{} blocking tasks got {} sleep threads, {} busy, {} still queued",
        tasks, stats.sleep_threads(), stats.sleep_busy(), stats.blocking_queued(),
    );

    let mut slept = Duration::ZERO;

    for handle in handles {
        slept += handle.join().expect("every task finishes");
    }

    let elapsed = started.elapsed();

    println!(
        "{} blocking sleeps of {:?} took {:?}, {:?} slept between them",
        tasks, duration, elapsed, slept,
    );

    assert!(
        stats.sleep_threads() >= tasks / 2,
        "{} blocking tasks in flight got only {} threads",
        tasks,
        stats.sleep_threads(),
    );

    assert!(
        slept > elapsed * 2,
        "{} blocking sleeps took {:?} of wall clock but only {:?} between them, \
         so they were barely overlapping",
        tasks,
        elapsed,
        slept,
    );
}
