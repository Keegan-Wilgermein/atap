mod common;

use atap::{Runtime, RuntimeError, Sleep, SleepMode};
use common::drain;
use std::time::Duration;

/// A drained bounded series reads `Finished`, where a one shot
/// that was already taken reads `AlreadyTaken`
#[test]
fn finished_and_already_taken_are_different_endings() {
    let _ = Runtime::init();

    // A one shot, taken twice
    let once =
        Runtime::task(Sleep::sleep(Duration::from_millis(5)).mode(SleepMode::Relaxed)).spawn();
    let watcher = once.clone();

    once.take().expect("the value moves out");

    assert_eq!(
        watcher.maybe_take(),
        Err(RuntimeError::AlreadyTaken),
        "a one shot says somebody was first, not that a series ended",
    );

    // A bounded repeat, drained to the end
    let bounded = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .repeat()
        .every(Duration::from_millis(10))
        .count(3)
        .spawn();

    let seen = drain(&bounded, Duration::from_secs(10));

    assert_eq!(seen, 3, "saw {seen} of 3 runs");

    assert_eq!(
        bounded.maybe_take(),
        Err(RuntimeError::Finished),
        "a series that ran out says so rather than looking like a lost race",
    );

    assert!(bounded.is_finished());
    assert!(!bounded.is_failed());
}
