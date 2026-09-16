//! Compute task tests
//!
//! Each test checks only the values its own tasks come back with,
//! so they share a binary and run side by side on purpose

mod common;

use atap::{Compute, Runtime, RuntimeError};
use common::settles;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A compute whose closure takes nothing runs once and hands back
/// what it returned
#[test]
fn a_compute_that_takes_nothing_runs_once() {
    let _ = Runtime::init();

    let answer = Runtime::task(Compute::compute(|()| 6 * 7)).spawn();

    assert_eq!(answer.join_with_timeout(PATIENCE), Ok(42));
}

/// Blocking on a compute runs it on the calling thread
#[test]
fn blocking_on_a_compute_runs_it_here() {
    let _ = Runtime::init();

    let caller = thread::current().id();
    let ran_on = Runtime::block(Compute::compute(|()| thread::current().id()));

    assert_eq!(
        ran_on, caller,
        "a blocked on compute ran on some other thread"
    );
}

/// A repeated compute runs exactly as many times as its count
#[test]
fn a_repeated_compute_runs_exactly_its_count() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        counted.fetch_add(1, Ordering::Relaxed)
    }))
    .repeat()
    .count(5)
    .spawn();

    assert!(
        settles(|| handle.is_finished()),
        "the series never finished"
    );
    assert_eq!(
        runs.load(Ordering::Relaxed),
        5,
        "a count of five ran a different number of times"
    );
}

/// A compute on a rate runs once a period until its count is up
#[test]
fn a_compute_on_a_rate_runs_its_count() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        counted.fetch_add(1, Ordering::Relaxed);
    }))
    .at_rate(Duration::from_millis(10))
    .count(4)
    .spawn();

    assert!(
        settles(|| handle.is_finished() && runs.load(Ordering::Relaxed) == 4),
        "a schedule of four ran {} times",
        runs.load(Ordering::Relaxed),
    );
}

/// Runs of an `every` compute start at least the gap apart
#[test]
fn every_spaces_compute_runs_by_at_least_its_gap() {
    let _ = Runtime::init();

    let gap = Duration::from_millis(20);
    let starts = Arc::new(Mutex::new(Vec::new()));
    let noted = Arc::clone(&starts);

    let handle = Runtime::task(Compute::compute(move |()| {
        noted.lock().unwrap().push(Instant::now());
    }))
    .repeat()
    .every(gap)
    .count(3)
    .spawn();

    assert!(
        settles(|| handle.is_finished()),
        "the series never finished"
    );

    let starts = starts.lock().unwrap();

    assert_eq!(
        starts.len(),
        3,
        "a count of three ran {} times",
        starts.len()
    );

    for pair in starts.windows(2) {
        assert!(
            pair[1] - pair[0] >= gap,
            "two runs started {:?} apart, inside the {:?} gap",
            pair[1] - pair[0],
            gap,
        );
    }
}

/// A compute that panics fails alone, and the pool carries on
#[test]
fn a_panicking_compute_fails_and_the_pool_carries_on() {
    let _ = Runtime::init();

    let doomed = Runtime::task(Compute::compute(|()| -> u8 {
        panic!("this compute is meant to go down")
    }))
    .spawn();

    assert_eq!(
        doomed.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );

    let after = Runtime::task(Compute::compute(|()| 7u8)).spawn();

    assert_eq!(
        after.join_with_timeout(PATIENCE),
        Ok(7),
        "the pool stopped after a panic"
    );
}

/// An output too big to sit beside a slot's header comes back
/// whole
#[test]
fn an_output_bigger_than_a_slot_comes_back_whole() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| {
        let mut big = [0u64; 64];

        for (index, cell) in big.iter_mut().enumerate() {
            *cell = index as u64 * 31;
        }

        big
    }))
    .spawn();

    let big = handle
        .join_with_timeout(PATIENCE)
        .expect("the big output never came");

    assert!(
        big.iter()
            .enumerate()
            .all(|(index, cell)| *cell == index as u64 * 31),
        "the big output came back scrambled",
    );
}

/// What a compute captured is dropped once the task is done with
/// it
#[test]
fn a_compute_drops_what_it_captured_once_it_is_done() {
    let _ = Runtime::init();

    let held = Arc::new(());
    let captured = Arc::clone(&held);

    let handle = Runtime::task(Compute::compute(move |()| Arc::strong_count(&captured))).spawn();

    let seen = handle
        .join_with_timeout(PATIENCE)
        .expect("the compute never ran");

    assert!(
        seen >= 2,
        "the compute didn't hold its own claim while it ran"
    );

    assert!(
        settles(|| Arc::strong_count(&held) == 1),
        "the task kept what it captured after it was done",
    );
}

/// An output that can't be cloned can still be moved out
#[test]
fn an_output_that_cannot_be_cloned_can_be_taken() {
    let _ = Runtime::init();

    /// Owns its bytes, and can't be copied
    struct Owned(Vec<u8>);

    let handle = Runtime::task(Compute::compute(|()| Owned(vec![1, 2, 3]))).spawn();

    let owned = handle
        .take_with_timeout(PATIENCE)
        .expect("the output never came");

    assert_eq!(owned.0, vec![1, 2, 3]);
}

/// A compute that says it blocks still runs to its end
#[test]
fn a_blocking_compute_runs_to_its_end() {
    let _ = Runtime::init();

    let asked = Duration::from_millis(20);

    let handle = Runtime::task(
        Compute::compute(move |()| {
            let started = Instant::now();
            thread::sleep(asked);
            started.elapsed()
        })
        .blocking(),
    )
    .spawn();

    let slept = handle
        .join_with_timeout(PATIENCE)
        .expect("the blocking compute never came back");

    assert!(
        slept >= asked,
        "a blocking compute came back after {:?} of {:?}",
        slept,
        asked
    );
}

/// A compute can spawn another compute and wait for its answer
#[test]
fn a_compute_can_spawn_another_and_wait_for_it() {
    let _ = Runtime::init();

    let outer = Runtime::task(Compute::compute(|()| {
        let inner = Runtime::task(Compute::compute(|()| 20)).spawn();

        inner.join().expect("the inner compute never came back") + 1
    }))
    .spawn();

    assert_eq!(outer.join_with_timeout(PATIENCE), Ok(21));
}

/// A delayed compute doesn't run before its delay is up
#[test]
fn a_delayed_compute_waits_out_its_delay() {
    let _ = Runtime::init();

    let delay = Duration::from_millis(30);
    let spawned = Instant::now();

    let handle = Runtime::task(Compute::compute(move |()| spawned.elapsed()))
        .after(delay)
        .spawn();

    let waited = handle
        .join_with_timeout(PATIENCE)
        .expect("the delayed compute never ran");

    assert!(
        waited >= delay,
        "a compute delayed {:?} ran after {:?}",
        delay,
        waited
    );
}

/// A compute cancelled before its delay is up never runs
#[test]
fn a_compute_cancelled_before_its_delay_never_runs() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        counted.fetch_add(1, Ordering::Relaxed);
    }))
    .after(Duration::from_millis(100))
    .spawn();

    handle.clone().cancel();

    thread::sleep(Duration::from_millis(200));

    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "a cancelled compute ran anyway"
    );
    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Err(RuntimeError::Cancelled)
    );
}

/// Thousands of computes each come back with their own answer
#[test]
fn every_compute_comes_back_with_its_own_answer() {
    let _ = Runtime::init();

    let handles: Vec<_> = (0..10_000u64)
        .map(|index| {
            (
                index,
                Runtime::task(Compute::compute(move |()| index * index)).spawn(),
            )
        })
        .collect();

    for (index, handle) in handles {
        assert_eq!(
            handle.join_with_timeout(PATIENCE),
            Ok(index * index),
            "compute {} came back with another's answer",
            index,
        );
    }
}
