//! # Manager recovery
//! The manager going down, and coming back
//!
//! A file of its own because killing the manager is a thing
//! that happens to the whole process. Every integration test
//! file is its own binary, so nothing else is in here to be
//! affected by it
//!
//! #### Note
//! The panics are printed as they unwind, because that is
//! Rust's default hook and a manager going down is worth
//! knowing about. Lines about a thread dying in the middle of
//! this are the test working

use atap::{Runtime, RuntimeError, Sleep, TaskHandle};
use std::time::Duration;

/// The manager comes back from going down, timers and all
///
/// ## Two halves
/// The pool is supposed to carry straight on while the manager
/// is away — it finds its own work and clears up after its own
/// dead, and stops adapting rather than stops working. So
/// tasks are spawned *during* the outage and joined there,
/// which is where that claim is either true or isn't
///
/// Then the manager coming back, which needs something only it
/// can do. A timed repeat is that something: it is driven
/// entirely by a timer on the manager's own queue and read by
/// nothing else in the process, so one that keeps producing
/// runs afterwards is a loop that is genuinely reading its
/// queue again
#[test]
fn manager_comes_back_from_going_down() {
    Runtime::init();

    let interval = Duration::from_millis(20);
    let before = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn();

    // Working beforehand, so a failure below is the manager
    // failing to come back rather than never having worked
    for _ in 0..3 {
        take_a_run(&before, "before the manager went down");
    }

    before.cancel();

    // Fewer than the restart limit, so the supervisor is
    // expected to bring it back every time
    Runtime::inject_manager_faults(3);

    // The claim the whole design rests on, tested where it is
    // actually true rather than after the fact. Three deaths
    // and their backoffs take a couple of hundred milliseconds,
    // and for most of that there is no manager in the process
    // at all — so these are spawned into a pool that is finding
    // its own work, reversing its own queue and clearing up
    // after its own dead with nothing supervising any of it
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

    // The pool never depended on the manager and shouldn't have
    // noticed any of this
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

            // The last run's output has been taken and the next
            // one hasn't landed yet
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
