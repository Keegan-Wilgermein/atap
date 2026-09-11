use atap::{Runtime, RuntimeError, Sleep};
use std::time::Duration;
use std::time::Instant;

/// A delay that is cancelled never runs at all
#[test]
fn after_can_be_cancelled_before_it_starts() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).after(Duration::from_millis(300)).spawn();

    let watcher = handle.clone();
    handle.cancel();

    let started = Instant::now();
    let result = watcher.join();
    let waited = started.elapsed();

    assert_eq!(
        result,
        Err(RuntimeError::Cancelled),
        "a task cancelled before its delay was up never ran",
    );

    assert!(
        waited < Duration::from_millis(200),
        "the reader waited {waited:?}, so the cancel didn't land until the delay did",
    );
}
