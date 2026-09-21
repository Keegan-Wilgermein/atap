//! Timeout tests
//!
//! Each test checks only the values its own tasks come back with,
//! so they share a binary and run side by side on purpose

mod common;

use atap::{Runtime, RuntimeError, TaskState, compute::Compute, sleep::Sleep};
use common::within;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A limit short enough to run out in every test that wants it to
const SHORT: Duration = Duration::from_millis(50);

/// A compute that takes too long reads as timed out, long before it
/// finishes
#[test]
fn a_slow_compute_times_out() {
    let _ = Runtime::init();

    let started = Instant::now();
    let handle = Runtime::task(Compute::compute(|()| {
        thread::sleep(Duration::from_secs(2));
        1
    }))
    .timeout(SHORT)
    .spawn();

    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Err(RuntimeError::TimedOut)
    );
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "the read waited for the compute, {:?}",
        started.elapsed()
    );
}

/// A task that finishes in time is untouched by its limit
#[test]
fn a_quick_task_keeps_its_output() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| 7))
        .timeout(PATIENCE)
        .spawn();

    assert_eq!(handle.join_with_timeout(PATIENCE), Ok(7));
}

/// A sleep past its limit is woken out of its wait and settles as
/// timed out
#[test]
fn a_long_sleep_times_out() {
    let _ = Runtime::init();

    let started = Instant::now();
    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(30)))
        .timeout(SHORT)
        .spawn();

    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Err(RuntimeError::TimedOut)
    );
    assert!(started.elapsed() >= SHORT, "timed out early");
    assert!(handle.is_timed_out());
    assert_eq!(handle.state(), TaskState::TimedOut);
}

/// A repeat's limit is per run, so quick runs carry on past it in
/// total
#[test]
fn a_repeat_is_limited_per_run() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        thread::sleep(Duration::from_millis(10));
        counted.fetch_add(1, Ordering::Relaxed)
    }))
    .repeat()
    .count(8)
    .timeout(SHORT)
    .spawn();

    let deadline = Instant::now() + PATIENCE;

    while !handle.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }

    assert_eq!(
        runs.load(Ordering::Relaxed),
        8,
        "every run finished in time"
    );
    assert!(!handle.is_timed_out());
}

/// The first run of a repeat to overrun ends the whole series
#[test]
fn an_overrunning_run_ends_the_repeat() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        let run = counted.fetch_add(1, Ordering::Relaxed);

        if run == 2 {
            thread::sleep(Duration::from_millis(500));
        }

        run
    }))
    .repeat()
    .timeout(Duration::from_millis(100))
    .spawn();

    let deadline = Instant::now() + PATIENCE;

    while !handle.is_timed_out() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }

    assert!(handle.is_timed_out(), "the slow run never timed out");
    assert_eq!(handle.try_join(), Err(RuntimeError::TimedOut));

    thread::sleep(Duration::from_millis(600));
    assert_eq!(
        runs.load(Ordering::Relaxed),
        3,
        "the series carried on after its timeout"
    );
}

/// A schedule's runs are each limited, and one that overruns ends
/// the schedule
#[test]
fn an_overrunning_run_ends_the_schedule() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        if counted.fetch_add(1, Ordering::Relaxed) == 1 {
            thread::sleep(Duration::from_secs(1));
        }
    }))
    .at_rate(Duration::from_millis(20))
    .timeout(Duration::from_millis(100))
    .spawn();

    let deadline = Instant::now() + PATIENCE;

    while !handle.is_timed_out() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }

    assert!(handle.is_timed_out(), "the schedule never timed out");
    assert_eq!(handle.try_join(), Err(RuntimeError::TimedOut));

    let seen = runs.load(Ordering::Relaxed);
    thread::sleep(Duration::from_millis(200));

    assert_eq!(
        runs.load(Ordering::Relaxed),
        seen,
        "the schedule kept running"
    );
}

/// A start delay isn't counted against a run's limit
#[test]
fn a_start_delay_is_not_counted() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| 3))
        .timeout(SHORT)
        .after(Duration::from_millis(200))
        .spawn();

    assert_eq!(handle.join_with_timeout(PATIENCE), Ok(3));
}

/// A waiting task is limited on each run, not while it waits for a
/// give
#[test]
fn a_waiting_task_is_only_limited_while_it_runs() {
    let _ = Runtime::init();

    let doubler = Runtime::task(Compute::compute(|value: u64| {
        if value == 0 {
            thread::sleep(Duration::from_secs(1));
        }

        value * 2
    }))
    .wait_for::<u64>()
    .timeout(Duration::from_millis(100))
    .spawn();

    thread::sleep(Duration::from_millis(200));

    doubler.give(4).expect("a give while waiting");
    assert_eq!(doubler.join_with_timeout(PATIENCE), Ok(8));

    let deadline = Instant::now() + PATIENCE;

    while !doubler.is_waiting() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    doubler.give(0).expect("a give that overruns");

    let deadline = Instant::now() + PATIENCE;

    while !doubler.is_timed_out() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }

    assert!(doubler.is_timed_out());
    assert_eq!(doubler.give(1), Err(RuntimeError::TimedOut));
}

/// A timed out task can still be cancelled, which changes nothing
#[test]
fn cancelling_a_timed_out_task_keeps_its_ending() {
    let _ = Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(30)))
        .timeout(SHORT)
        .spawn();

    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Err(RuntimeError::TimedOut)
    );

    handle.clone().cancel();

    assert_eq!(handle.state(), TaskState::TimedOut);
}

/// The helper the other suites use answers with the task's own
/// error, or the timeout
#[test]
fn within_flattens_the_answer() {
    let _ = Runtime::init();

    assert_eq!(
        within(Compute::compute(|()| Ok::<_, RuntimeError>(5)), PATIENCE),
        Ok(5)
    );
    assert_eq!(
        within(
            Compute::compute(|()| Err::<u8, _>(RuntimeError::BadPath)),
            PATIENCE
        ),
        Err(RuntimeError::BadPath)
    );
}
