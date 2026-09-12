use atap::{Runtime, Sleep, SleepMode};
use std::time::Duration;
use std::time::Instant;

/// A timeout that isn't needed returns as soon as the task
/// does, not when the timeout runs out
#[test]
fn join_with_timeout_returns_as_soon_as_the_task_does() {
    Runtime::init();

    let timeout = Duration::from_secs(10);
    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(20)).mode(SleepMode::Relaxed)).spawn();

    let started = Instant::now();
    let result = handle.join_with_timeout(timeout);
    let waited = started.elapsed();

    assert!(result.is_ok(), "the task finished, so it reads: {result:?}");

    println!("waited {waited:?} of a {timeout:?} timeout");

    assert!(
        waited < Duration::from_secs(1),
        "came back after {waited:?}, which is the timeout being waited out rather than the task",
    );
}
