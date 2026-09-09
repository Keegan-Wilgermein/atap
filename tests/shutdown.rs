//! # Shutdown
//! A shutdown is terminal for the process it happens in, so
//! this is one test in a binary of its own. Anything sharing
//! the binary would find the runtime gone underneath it,
//! whichever order the two happened to run in

use atap::{Runtime, RuntimeError, Sleep};
use std::time::{Duration, Instant};

/// Everything queued runs, then nothing else is taken
///
/// The three things a shutdown promises, in the order they
/// happen: the backlog drains rather than being thrown away,
/// nothing new is accepted afterwards, and no listener is left
/// blocked on a task that has nowhere left to run
#[test]
fn shutdown_drains_the_backlog_then_refuses_new_work() {
    Runtime::init();

    let tasks = 2_000;

    // Half on workers and half on sleep threads, so the drain
    // has to account for both halves of the pool rather than
    // just the one
    let handles: Vec<_> = (0..tasks)
        .map(|task| Runtime::spawn(Sleep::sleep(Duration::from_micros(200), task % 2 == 0)))
        .collect();

    let started = Instant::now();
    Runtime::shutdown();
    let drained = started.elapsed();

    println!("shutdown drained {tasks} tasks in {drained:?}");

    // Drained rather than aborted. Everything spawned before it
    // ran to the end and came back to its listener normally
    for (task, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|error| panic!("task {task} was dropped by the shutdown: {error}"));
    }

    // Closed to new work, and it says so rather than leaving a
    // listener blocked on a pool that isn't there
    let late = Runtime::spawn(Sleep::sleep(Duration::from_millis(10), false));

    assert_eq!(
        late.join(),
        Err(RuntimeError::TaskFailed),
        "a spawn after a shutdown settles instead of waiting for something that has gone",
    );

    // Blocking calls are deliberately untouched. The `Reactor`
    // is left up precisely so this still works, because `block`
    // promises it can't be cancelled by any means and a
    // shutdown is not allowed to be the exception
    let slept = Runtime::block(Sleep::sleep(Duration::from_millis(20), false));

    println!("a blocking call after the shutdown still slept {slept:?}");

    assert!(
        slept >= Duration::from_millis(20),
        "the blocking call came back early, so the reactor went down with the pool",
    );

    let status = Runtime::status();
    println!("{status}");

    assert!(status.shut_down, "the status says what happened");
    assert!(!status.manager_alive, "the manager's queue was closed");
    assert!(!status.healthy());

    // Safe twice, and does nothing the second time
    Runtime::shutdown();

    // One way. Every slot in the table was handed back, so
    // starting again would give those ids to new tasks while
    // these handles are still holding them
    assert_eq!(
        Runtime::init(),
        Some(RuntimeError::ShutDown),
        "a runtime that has been shut down doesn't start again",
    );
}
