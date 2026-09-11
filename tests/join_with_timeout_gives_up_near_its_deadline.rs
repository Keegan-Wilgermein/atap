use atap::{Runtime, RuntimeError, Sleep};
use std::time::Duration;
use std::time::Instant;

/// A timeout that runs out gives up close to its deadline
#[test]
fn join_with_timeout_gives_up_near_its_deadline() {
    Runtime::init();

    let timeout = Duration::from_millis(100);
    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(5), false)).spawn();

    let started = Instant::now();
    let result = handle.join_with_timeout(timeout);
    let waited = started.elapsed();

    assert_eq!(
        result,
        Err(RuntimeError::NotReady),
        "nowhere near long enough, and it says so",
    );

    println!("gave up after {waited:?} against a {timeout:?} timeout");

    assert!(waited >= timeout, "came back early, after only {waited:?}");

    assert!(
        waited < timeout * 10,
        "took {waited:?} over a {timeout:?} timeout, so something is restarting the wait",
    );

    handle.cancel();
}
