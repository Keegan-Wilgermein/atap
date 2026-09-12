mod common;

use atap::{Runtime, RuntimeError, Sleep, SleepMode};
use common::report;
use std::{
    thread,
    time::{Duration, Instant},
};

/// Cancelled sleeps give their sleep threads straight back
#[test]
fn cancelling_hands_the_thread_back() {
    Runtime::init();

    let sleeps = 16;
    let patience = Duration::from_secs(5);

    // Counted from whatever the pool was already doing
    let before = Runtime::workers().sleep_busy();

    // Too long for any of them to finish by itself
    let handles: Vec<_> = (0..sleeps)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_secs(30)).mode(SleepMode::Relaxed)).spawn())
        .collect();

    let waiting = Instant::now();

    while Runtime::workers().sleep_busy() < before + sleeps && waiting.elapsed() < patience {
        thread::sleep(Duration::from_micros(200));
    }

    let sleeping = Runtime::workers().sleep_busy().saturating_sub(before);
    report("all sleeping");

    assert_eq!(
        sleeping, sleeps,
        "only {} of {} sleeps ever reached a thread",
        sleeping, sleeps,
    );

    let started = Instant::now();

    for handle in handles.iter() {
        handle.clone().cancel();
    }

    while Runtime::workers().sleep_busy() > before && started.elapsed() < patience {
        thread::sleep(Duration::from_micros(200));
    }

    let freed = started.elapsed();
    let left = Runtime::workers().sleep_busy().saturating_sub(before);

    report("all cancelled");

    for handle in handles {
        assert_eq!(
            handle.join(),
            Err(RuntimeError::Cancelled),
            "a cancelled task hands nothing out",
        );
    }

    println!(
        "{} sleeps of 30s cancelled, every thread back in {:?}",
        sleeps, freed,
    );

    assert_eq!(
        left, 0,
        "{} threads were still inside a wait after being cancelled",
        left,
    );

    assert!(
        freed < Duration::from_secs(1),
        "threads took {:?} to come back from a cancel",
        freed,
    );
}
