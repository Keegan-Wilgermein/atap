use atap::{Runtime, RuntimeError, Sleep, TaskHandle};
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// Drains a bounded series, counting what it published
fn drain(handle: &TaskHandle<Duration>, patience: Duration) -> usize {
    let deadline = Instant::now() + patience;
    let mut seen = 0;

    while Instant::now() < deadline {
        match handle.maybe_take() {
            Ok(_) => seen += 1,

            // Between runs, or one still going
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::NotReady) => {
                thread::sleep(Duration::from_millis(1))
            }

            // `Finished` and every other error are endings
            Err(_) => break,
        }
    }

    seen
}

/// A run that would begin past the deadline is never begun
///
/// A 750ms gap bounded to a second has room for two runs, not
/// one and not three
#[test]
fn for_duration_stops_before_the_run_that_would_overrun() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(Duration::from_millis(750))
        .for_duration(Duration::from_secs(1))
        .spawn();

    let seen = drain(&handle, Duration::from_secs(10));

    println!("a 750ms gap bounded to 1s ran {seen} times");

    assert!(handle.is_finished());
    assert_eq!(seen, 2, "expected the runs at 0ms and 750ms and no more");
}
