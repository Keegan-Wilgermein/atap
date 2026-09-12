use atap::{JoinPolicy, Runtime, Sleep, SleepMode, TaskHandle};
use std::time::Duration;
use std::time::Instant;

/// A sleep of a given length, spawned
fn sleeping(millis: u64) -> TaskHandle<Duration> {
    Runtime::task(Sleep::sleep(Duration::from_millis(millis)).mode(SleepMode::Relaxed)).spawn()
}

/// A task that settles while `join_first` is registering is
/// still found promptly
#[test]
fn a_task_that_settles_during_registration_is_still_found() {
    Runtime::init();

    // Short enough to finish while `join_first` is still registering
    for _ in 0..32 {
        let racing = sleeping(0);
        let slow: Vec<_> = (0..8).map(|_| sleeping(4000)).collect();

        let started = Instant::now();

        let (first, _) = Runtime::join_first(
            std::iter::once(racing).chain(slow),
            JoinPolicy::Cancel,
        );

        let waited = started.elapsed();

        assert!(first.settled(), "the winner should be settled");

        assert!(
            waited < Duration::from_millis(40),
            "the race took {:?}, which is the ceiling rather than a wake",
            waited,
        );
    }
}
