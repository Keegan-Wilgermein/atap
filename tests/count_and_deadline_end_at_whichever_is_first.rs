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

/// A count and a deadline end at whichever comes first
#[test]
fn count_and_deadline_end_at_whichever_is_first() {
    Runtime::init();

    let gap = Duration::from_millis(20);

    // The count ends this one
    let counted = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(gap)
        .count(3)
        .for_duration(Duration::from_secs(10))
        .spawn();

    // And the deadline ends this one
    let timed = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(gap)
        .count(1_000_000)
        .for_duration(Duration::from_millis(120))
        .spawn();

    let by_count = drain(&counted, Duration::from_secs(10));
    let by_time = drain(&timed, Duration::from_secs(10));

    println!("count won at {by_count} runs, deadline won at {by_time} runs");

    assert_eq!(by_count, 3, "the count should have ended this one");

    assert!(timed.is_finished(), "the deadline never ended it");
    assert!(
        by_time > 0 && by_time < 1_000,
        "{by_time} runs is not a 120ms window at a 20ms gap",
    );
}
