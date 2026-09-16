//! Waiting repeat tests
//!
//! Tasks spawned with `wait_for` and a kind chained after it, where
//! every give starts a series. Each test checks only its own tasks'
//! values, so they share a binary and run side by side on purpose

mod common;

use atap::{Compute, Runtime, RuntimeError};
use common::settles;
use std::{
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Long enough to be sure nothing more is coming
const QUIET: Duration = Duration::from_millis(100);

/// Takes `count` values off `runs`, failing if any is late
fn collect<T>(runs: &mpsc::Receiver<T>, count: usize) -> Vec<T> {
    (0..count)
        .map(|index| {
            runs.recv_timeout(PATIENCE)
                .unwrap_or_else(|_| panic!("run {} of {} never came", index + 1, count))
        })
        .collect()
}

/// Each give starts a series of the repeat's own count, and the count
/// of gives finishes the task
#[test]
fn each_give_starts_a_series_of_its_own_count() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .count(3)
    .repeat()
    .count(5)
    .spawn();

    for value in 1..=3 {
        assert!(
            settles(|| handle.is_waiting()),
            "series {} found the task still busy",
            value
        );

        handle
            .give(value)
            .expect("a give that starts a series was refused");

        assert_eq!(
            collect(&runs, 5),
            vec![value; 5],
            "series {} ran wrong",
            value
        );
    }

    assert!(
        settles(|| handle.is_finished()),
        "three series didn't finish the task"
    );
    assert!(
        runs.recv_timeout(QUIET).is_err(),
        "a series ran past its count"
    );
    assert_eq!(handle.give(4), Err(RuntimeError::Finished));
}

/// A give during a series only replaces the value the rest of it is
/// handed, and starts nothing of its own
#[test]
fn a_give_mid_series_only_replaces_the_value() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .repeat()
    .every(Duration::from_millis(20))
    .count(4)
    .spawn();

    handle.give(1).expect("the first give was refused");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(1));

    handle.give(2).expect("a give mid series was refused");

    let rest = collect(&runs, 3);

    assert_eq!(
        rest.last(),
        Some(&2),
        "the rest of the series never saw the newer value: {:?}",
        rest
    );
    assert!(
        runs.recv_timeout(QUIET).is_err(),
        "a give mid series started another series"
    );
    assert!(
        settles(|| handle.is_waiting()),
        "the task didn't wait again after its series"
    );

    handle.give(3).expect("a give after the series was refused");

    assert_eq!(collect(&runs, 4), vec![3; 4]);
}

/// A give racing the end of a series either replaces the value or
/// starts one new series, never two and never none
#[test]
fn a_give_racing_the_end_of_a_series_never_starts_two() {
    let _ = Runtime::init();

    for round in 0..200u64 {
        let (ran, runs) = mpsc::channel();

        let handle = Runtime::task(Compute::compute(move |value: u64| {
            let _ = ran.send(value);
            value
        }))
        .wait_for::<u64>()
        .repeat()
        .count(2)
        .spawn();

        handle.give(round).expect("the first give was refused");

        // Lands somewhere around the end of the series
        thread::sleep(Duration::from_micros(round % 400));

        handle
            .give(round + 1_000_000)
            .expect("a give racing the end of a series was refused");

        assert!(
            settles(|| handle.is_waiting()),
            "round {} never went back to waiting",
            round
        );

        let seen: Vec<u64> = runs.try_iter().collect();

        assert!(
            seen.len() == 2 || seen.len() == 4,
            "round {} ran {} times, which is neither one series nor two: {:?}",
            round,
            seen.len(),
            seen,
        );
    }
}

/// Every series gets its whole count and keeps its gaps
#[test]
fn every_gap_and_count_start_afresh_for_each_series() {
    let _ = Runtime::init();

    let gap = Duration::from_millis(15);
    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send((value, Instant::now()));
        value
    }))
    .wait_for::<u32>()
    .repeat()
    .every(gap)
    .count(3)
    .spawn();

    for series in 0..2 {
        assert!(
            settles(|| handle.is_waiting()),
            "series {} found the task still busy",
            series
        );

        handle.give(series).expect("a give was refused");

        let seen = collect(&runs, 3);

        assert!(
            seen.iter().all(|(value, _)| *value == series),
            "series {} saw another's value",
            series
        );

        for pair in seen.windows(2) {
            assert!(
                pair[1].1 - pair[0].1 >= gap,
                "series {} ran two runs {:?} apart, inside its {:?} gap",
                series,
                pair[1].1 - pair[0].1,
                gap,
            );
        }
    }

    assert!(
        runs.recv_timeout(QUIET).is_err(),
        "a series ran past its count"
    );
}

/// Every series gets the whole of its span, however long after the
/// last one it starts
#[test]
fn for_duration_starts_afresh_for_each_series() {
    let _ = Runtime::init();

    let span = Duration::from_millis(300);
    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .repeat()
    .every(Duration::from_millis(10))
    .for_duration(span)
    .spawn();

    let mut counts = Vec::new();

    for series in 0..2 {
        assert!(
            settles(|| handle.is_waiting()),
            "series {} found the task still busy",
            series
        );

        // Well past the first series' deadline
        if series == 1 {
            thread::sleep(span);
        }

        handle.give(series).expect("a give was refused");

        assert_eq!(runs.recv_timeout(PATIENCE), Ok(series));
        assert!(
            settles(|| handle.is_waiting()),
            "series {} never ended",
            series
        );

        counts.push(1 + runs.try_iter().count());
    }

    assert!(
        counts[1] >= 2,
        "the second series got no deadline of its own: {:?}",
        counts
    );
}

/// A schedule a give starts runs its count, then waits for the next
#[test]
fn an_at_rate_series_runs_for_each_give() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .at_rate(Duration::from_millis(10))
    .count(3)
    .spawn();

    for series in 0..2 {
        assert!(
            settles(|| handle.is_waiting()),
            "series {} found the task still busy",
            series
        );

        handle.give(series).expect("a give was refused");

        assert_eq!(
            collect(&runs, 3),
            vec![series; 3],
            "series {} ran wrong",
            series
        );
    }

    assert!(
        runs.recv_timeout(QUIET).is_err(),
        "a schedule ran past its count"
    );
}

/// An unbounded repeat keeps running, with whatever was given last
#[test]
fn an_unbounded_repeat_keeps_running_with_the_latest_value() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .repeat()
    .every(Duration::from_millis(5))
    .spawn();

    handle.give(1).expect("the first give was refused");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(1));

    handle.give(2).expect("the second give was refused");

    loop {
        match runs.recv_timeout(PATIENCE) {
            Ok(2) => break,
            Ok(1) => continue,
            other => panic!("the repeat came back with {:?}", other),
        }
    }

    for _ in 0..5 {
        assert_eq!(
            runs.recv_timeout(PATIENCE),
            Ok(2),
            "an older value ran after a newer one"
        );
    }

    assert!(!handle.is_waiting(), "an unbounded repeat stopped to wait");

    handle.cancel();
}

/// `after` is waited out before every series a give starts
#[test]
fn after_is_waited_out_before_each_series() {
    let _ = Runtime::init();

    let delay = Duration::from_millis(25);
    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |given: Instant| {
        let _ = ran.send(given.elapsed());
    }))
    .wait_for::<Instant>()
    .repeat()
    .count(2)
    .after(delay)
    .spawn();

    for series in 0..2 {
        assert!(
            settles(|| handle.is_waiting()),
            "series {} found the task still busy",
            series
        );

        handle.give(Instant::now()).expect("a give was refused");

        let first = runs
            .recv_timeout(PATIENCE)
            .expect("a delayed series never ran");

        assert!(
            first >= delay,
            "series {} started {:?} after its give, inside its delay",
            series,
            first
        );

        runs.recv_timeout(PATIENCE)
            .expect("a delayed series stopped after one run");
    }
}
