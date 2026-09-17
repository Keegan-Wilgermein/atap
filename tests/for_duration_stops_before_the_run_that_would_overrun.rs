mod common;

use atap::{
    Runtime,
    sleep::{Sleep, SleepMode},
};
use common::drain;
use std::time::Duration;

/// A run that would begin past the deadline is never begun
///
/// A 750ms gap bounded to a second has room for two runs, not
/// one and not three
#[test]
fn for_duration_stops_before_the_run_that_would_overrun() {
    let _ = Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .repeat()
        .every(Duration::from_millis(750))
        .for_duration(Duration::from_secs(1))
        .spawn();

    let seen = drain(&handle, Duration::from_secs(10));

    println!("a 750ms gap bounded to 1s ran {seen} times");

    assert!(handle.is_finished());
    assert_eq!(seen, 2, "expected the runs at 0ms and 750ms and no more");
}
