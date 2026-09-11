use atap::{Runtime, RuntimeError, Sleep};
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// Cancelling a task settles every handle to it straight away
#[test]
fn cancelling_a_spawned_task_settles_every_listener() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(30), false)).spawn();
    let watcher = handle.clone();

    // Inside the kernel wait rather than still queued
    thread::sleep(Duration::from_millis(200));

    let started = Instant::now();
    handle.cancel();

    assert_eq!(
        watcher.join(),
        Err(RuntimeError::Cancelled),
        "a cancelled task hands nothing out",
    );

    let settled = started.elapsed();

    println!("a 30 second sleep cancelled and settled in {:?}", settled);

    assert!(
        settled < Duration::from_secs(1),
        "cancelling a sleep took {:?} to settle",
        settled,
    );
}
