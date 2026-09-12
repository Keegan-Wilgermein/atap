use atap::{Runtime, RuntimeError, Sleep, SleepMode};
use std::time::Duration;

/// A second read of an unbounded repeat reports `AlreadyTaken`
/// rather than `Finished`, since another run is coming
#[test]
fn an_unbounded_repeat_reports_a_lost_race() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .repeat()
        .every(Duration::from_millis(50))
        .spawn();

    handle.wait().expect("a run publishes");
    handle.maybe_take().expect("and the first reader gets it");

    let second = handle.maybe_take();

    handle.cancel();

    assert_eq!(
        second,
        Err(RuntimeError::AlreadyTaken),
        "there is another run coming, so this is a race rather than an ending",
    );
}
