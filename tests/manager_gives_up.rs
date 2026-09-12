//! # Manager teardown
//! What happens to everything depending on the manager once it
//! gives up for good
//!
//! #### Note
//! Six panics are printed as they unwind. That is the test
//! working

use atap::{Runtime, RuntimeError, Sleep, SleepMode};
use std::{thread, time::Duration};

/// A manager that gives up settles every repeat that depended
/// on it, and the pool keeps working
#[test]
fn a_manager_that_gives_up_strands_nothing() {
    Runtime::init();

    let quick = || Sleep::sleep(Duration::from_nanos(1));
    let interval = Duration::from_millis(20);

    let before = Runtime::workers();

    let schedule = Runtime::task(quick()).at_rate(interval).spawn();
    let timed = Runtime::task(quick()).repeat().every(interval).spawn();
    let looping = Runtime::task(quick()).repeat().spawn();

    // Running properly before any of this
    thread::sleep(Duration::from_millis(100));

    assert!(
        !schedule.settled() || schedule.maybe_join().is_ok(),
        "the schedule settled before the manager was touched",
    );

    // More than the restart limit, so the supervisor gives up
    Runtime::inject_manager_faults(16);

    thread::sleep(Duration::from_secs(1));

    // Drained first, so anything readable afterwards is a run
    // that started after the manager had gone
    let _ = schedule.clone().take();
    let _ = timed.clone().take();

    thread::sleep(Duration::from_millis(200));

    assert_eq!(
        schedule.clone().take(),
        Err(RuntimeError::AlreadyTaken),
        "a schedule kept starting runs after the queue driving it had closed",
    );

    assert_eq!(
        timed.clone().take(),
        Err(RuntimeError::AlreadyTaken),
        "a timed repeat kept running after the queue it waits on had closed",
    );

    assert!(
        schedule.settled() && timed.settled(),
        "something was left unsettled with nothing able to run it, pool {:?}",
        Runtime::workers(),
    );

    // The pool outlives its manager
    let slept = Runtime::task(Sleep::sleep(Duration::from_millis(10)).mode(SleepMode::Relaxed)).spawn()
        .join()
        .expect("the pool still runs tasks with no manager at all");

    looping.cancel();
    drop(schedule);
    drop(timed);

    // Every slot back
    assert!(
        settles(|| Runtime::workers().live() <= before.live()),
        "the manager gave up holding {} live tasks, up from {}",
        Runtime::workers().live(),
        before.live(),
    );

    println!(
        "manager gave up, schedule settled, pool still ran a task in {slept:?}, \
         live back to {}",
        Runtime::workers().live(),
    );
}

/// Waits for something to become true, with a cap
fn settles(mut condition: impl FnMut() -> bool) -> bool {
    let waited = std::time::Instant::now();

    while waited.elapsed() < Duration::from_secs(5) {
        if condition() {
            return true;
        }

        thread::sleep(Duration::from_millis(10));
    }

    condition()
}
