//! # Manager wake recovery
//! A manager that dies holding wakes doesn't take the tasks
//! waiting on them with it

use atap::{Runtime, RuntimeError, Sleep, TaskHandle};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Every timed repeat keeps running after the manager dies
/// part way through a batch of wakes
#[test]
fn a_manager_dying_on_a_batch_loses_no_waits() {
    Runtime::init();

    let waits = 32;
    let interval = Duration::from_millis(5);

    let handles: Vec<_> = (0..waits)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1))).repeat().every(interval).spawn())
        .collect();

    // Every one of them running before the manager is touched
    for handle in &handles {
        take_a_run(handle, 0);
    }

    // Under the restart limit, so it comes back every time
    Runtime::inject_manager_faults(3);

    // The deaths, their backoffs, and room for the recovered
    // waits to come round again
    thread::sleep(Duration::from_millis(500));

    for (index, handle) in handles.iter().enumerate() {
        take_a_run(handle, index);
    }

    println!("{waits} waits survived the manager dying on three batches");

    for handle in handles {
        handle.cancel();
    }
}

/// Waits for the next run of a repeating task and takes it
fn take_a_run(handle: &TaskHandle<Duration>, index: usize) -> Duration {
    let waited = Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The next run hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!("wait {index} came back with {error:?}"),
        }

        assert!(
            waited.elapsed() < Duration::from_secs(5),
            "wait {index} stopped producing runs, so its wake went down with the manager \
             and nothing put it back",
        );

        thread::sleep(Duration::from_millis(1));
    }
}
