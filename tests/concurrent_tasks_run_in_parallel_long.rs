mod common;

use atap::{Runtime, Sleep, SleepMode};
use common::cores;
use std::time::Duration;
use std::time::Instant;

/// Long sleeps spawned together overlap rather than running
/// one after another
#[test]
fn concurrent_tasks_run_in_parallel_long() {
    Runtime::init();

    let duration = Duration::from_secs(5);
    let tasks = cores();

    let started = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(duration).mode(SleepMode::Relaxed)).spawn())
        .collect();

    let mut slept = Duration::ZERO;

    for handle in handles {
        slept += handle.join().expect("every task finishes");
    }

    let elapsed = started.elapsed();

    println!(
        "{} tasks of {:?} took {:?}, {:?} slept between them",
        tasks, duration, elapsed, slept,
    );

    // Sleeps that ran one after another put this ratio at 1
    assert!(
        slept > elapsed * 2,
        "{} sleeps took {:?} of wall clock but only {:?} between them, \
         so they were barely overlapping",
        tasks,
        elapsed,
        slept,
    );
}
