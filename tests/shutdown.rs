//! # Shutdown

use atap::{Runtime, RuntimeError, Sleep};
use std::time::{Duration, Instant};

/// A shutdown drains the backlog, refuses new work, and leaves
/// `block` working
#[test]
fn shutdown_drains_the_backlog_then_refuses_new_work() {
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

    // And it can't be started again
    assert_eq!(
        Runtime::init(),
        Some(RuntimeError::ShutDown),
        "a runtime that has been shut down doesn't start again",
    );
}
