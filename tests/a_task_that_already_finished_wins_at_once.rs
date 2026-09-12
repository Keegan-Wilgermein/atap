use atap::{JoinPolicy, Runtime, Sleep, SleepMode, TaskHandle};
use std::time::Duration;
use std::time::Instant;

/// A sleep of a given length, spawned
fn sleeping(millis: u64) -> TaskHandle<Duration> {
    Runtime::task(Sleep::sleep(Duration::from_millis(millis)).mode(SleepMode::Relaxed)).spawn()
}

/// A race with a task that has already finished returns at once
#[test]
fn a_task_that_already_finished_wins_at_once() {
    Runtime::init();

    let done = sleeping(1);

    done.wait().expect("the task settles");

    let slow: Vec<_> = (0..3).map(|_| sleeping(4000)).collect();
    let done_id = done.id();

    let started = Instant::now();

    let (first, _) = Runtime::join_first(
        std::iter::once(done).chain(slow),
        JoinPolicy::Cancel,
    );

    let waited = started.elapsed();

    println!("an already settled task was found in {:?}", waited);

    assert_eq!(first.id(), done_id, "the settled task should have won");

    assert!(waited < Duration::from_millis(200), "took {:?}", waited);
}
