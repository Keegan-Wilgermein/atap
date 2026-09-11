//! # Manager recovery
//! The manager going down, and coming back
//!
//! #### Note
//! The panics printed as it unwinds are the test working

use atap::{Runtime, RuntimeError, Sleep, TaskHandle};
use std::time::Duration;

/// The pool keeps working while the manager is down, and the
/// manager comes back with its timers
#[test]
fn manager_comes_back_from_going_down() {
    Runtime::init();

    let interval = Duration::from_millis(20);
    let before = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn();

    // Working beforehand
    for _ in 0..3 {
        take_a_run(&before, "before the manager went down");
    }

    before.cancel();

    // Under the restart limit, so it comes back every time
    Runtime::inject_manager_faults(3);

    // Spawned while there is no manager at all
    let during: Vec<_> = (0..256)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50), true)).spawn())
        .collect();

    for handle in during {
        handle
            .join()
            .expect("a task spawned while the manager was gone still ran");
    }

    // Three deaths and their backoffs, with room to spare
    std::thread::sleep(Duration::from_millis(400));

    let after = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn();

    for _ in 0..5 {
        take_a_run(&after, "after the manager went down three times");
    }

    after.cancel();

    let slept = Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).spawn()
        .join()
        .expect("a task spawned after the manager came back still runs");

    println!("manager survived 3 deaths, a task after it slept {slept:?}");
}

/// Waits for the next run of a repeating task and takes it
fn take_a_run(handle: &TaskHandle<Duration>, when: &str) -> Duration {
    let waited = std::time::Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The next run hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!("a timed repeat came back with {error:?} {when}"),
        }

        assert!(
            waited.elapsed() < Duration::from_secs(5),
            "a timed repeat stopped producing runs {when}, so the manager did not come back",
        );

        std::thread::sleep(Duration::from_millis(1));
    }
}
