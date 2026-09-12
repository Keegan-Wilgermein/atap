use atap::{Runtime, Sleep, SleepMode};
use std::time::Duration;
use std::time::Instant;

/// A bare chain is a task that runs once, now
#[test]
fn bare_chain_runs_once_now() {
    Runtime::init();

    let duration = Duration::from_millis(50);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(duration).mode(SleepMode::Relaxed)).spawn();
    let slept = handle.join().expect("it finishes");

    println!("slept {slept:?} against {duration:?}");

    assert!(slept >= duration, "slept {slept:?}, which is short");

    // Once, and no more
    assert!(
        started.elapsed() < duration * 10,
        "a bare chain took far longer than one run of it",
    );
}
