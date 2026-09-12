use atap::{Runtime, RuntimeError, Sleep, SleepMode, TaskHandle};
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

/// A count runs exactly that many times
#[test]
fn count_runs_exactly_that_many_times() {
    Runtime::init();

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
