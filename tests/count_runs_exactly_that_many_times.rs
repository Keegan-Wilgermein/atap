mod common;

use atap::{Runtime, Sleep, SleepMode};
use common::drain;
use std::time::Duration;

/// A count runs exactly that many times
#[test]
fn count_runs_exactly_that_many_times() {
    let _ = Runtime::init();

    let runs = 5;

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .repeat()
        .every(Duration::from_millis(30))
        .count(runs)
        .spawn();

    let seen = drain(&handle, Duration::from_secs(10));

    println!("saw {seen} runs against a count of {runs}");

    assert!(handle.is_finished(), "the series never reported finishing");
    assert_eq!(seen as u32, runs, "saw {seen} runs, not {runs}");

    assert!(!handle.is_failed(), "running out is not failing");
}
