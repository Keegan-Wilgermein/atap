//! # Shutdown

use atap::{Runtime, RuntimeError, Sleep};
use std::{
    thread,
    time::{Duration, Instant},
};

/// A shutdown drains the backlog, refuses new work, and leaves
/// `block` working, then `init` starts the runtime again
#[test]
fn shutdown_drains_the_backlog_then_init_starts_it_again() {
    Runtime::init();

    let tasks = 2_000;

    // Half on workers and half on sleep threads
    let handles: Vec<_> = (0..tasks)
        .map(|task| Runtime::task(Sleep::sleep(Duration::from_micros(200), task % 2 == 0)).spawn())
        .collect();

    let started = Instant::now();
    Runtime::shutdown();
    let drained = started.elapsed();

    println!("shutdown drained {tasks} tasks in {drained:?}");

    // Everything spawned before it ran to the end
    for (task, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|error| panic!("task {task} was dropped by the shutdown: {error}"));
    }

    // Closed to new work
    let late = Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).spawn();
    let kept = late.clone();

    assert_eq!(
        late.join(),
        Err(RuntimeError::TaskFailed),
        "a spawn after a shutdown settles instead of waiting for something that has gone",
    );

    // Blocking calls still work
    let slept = Runtime::block(Sleep::sleep(Duration::from_millis(20), false));

    println!("a blocking call after the shutdown still slept {slept:?}");

    assert!(
        slept >= Duration::from_millis(20),
        "the blocking call came back early, so the reactor went down with the pool",
    );

    let status = Runtime::status();
    println!("{status}");

    assert!(status.shut_down(), "the status says what happened");
    assert!(!status.manager_alive(), "the manager's queue was closed");
    assert!(!status.healthy());

    // Safe twice
    Runtime::shutdown();

    for cycle in 0..2 {
        // Starts again, and only once
        assert_eq!(Runtime::init(), None, "cycle {cycle}: the runtime didn't start again");

        assert_eq!(
            Runtime::init(),
            Some(RuntimeError::AlreadyInit),
            "cycle {cycle}: a running runtime was started twice",
        );

        let status = Runtime::status();
        println!("cycle {cycle}: {status}");

        assert!(status.healthy(), "cycle {cycle}: the runtime came back degraded");

        // A handle from before the shutdown still reads what its
        // task ended with
        assert_eq!(
            kept.maybe_join(),
            Err(RuntimeError::TaskFailed),
            "cycle {cycle}: a restart changed what an old handle reads",
        );

        let quick = Runtime::task(Sleep::sleep(Duration::from_micros(200), true)).spawn();
        let blocking = Runtime::task(Sleep::sleep(Duration::from_millis(5), false)).spawn();

        // Both need the new manager's timers
        let delayed = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
            .after(Duration::from_millis(50))
            .spawn();

        let counted = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
            .repeat()
            .every(Duration::from_millis(10))
            .count(3)
            .spawn();

        quick.join().expect("a task spawned after the restart runs");
        blocking.join().expect("a blocking task spawned after the restart runs");
        delayed.join().expect("a delayed task spawned after the restart runs");

        let deadline = Instant::now() + Duration::from_secs(10);

        while !counted.is_finished() && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(1));
        }

        assert!(counted.is_finished(), "cycle {cycle}: a timed repeat never got through its count");
        assert!(!counted.is_failed(), "cycle {cycle}: a timed repeat failed after the restart");

        Runtime::shutdown();

        assert!(Runtime::status().shut_down(), "cycle {cycle}: the second shutdown didn't take");
    }
}
