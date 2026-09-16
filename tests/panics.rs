//! Panic tests
//!
//! Computes that panic, and what happens to everything around them:
//! the tasks waiting on them, the tasks inside them, and the pool.
//! Each test checks only its own tasks, so they share a binary and
//! run side by side on purpose
//!
//! #### Note
//! Every panic prints as it unwinds. That is the test working

mod common;

use atap::{Compute, Runtime, RuntimeError};
use common::settles;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A compute that panics settles failed rather than taking anything
/// down with it
#[test]
fn a_panicking_compute_fails() {
    let _ = Runtime::init();

    let doomed = Runtime::task(Compute::compute(|()| -> u32 {
        panic!("a compute went down")
    }))
    .spawn();

    assert_eq!(
        doomed.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );

    // The worker it ran on is still taking work
    let after: Vec<_> = (0..256u32)
        .map(|value| Runtime::task(Compute::compute(move |()| value)).spawn())
        .collect();

    for (value, handle) in after.into_iter().enumerate() {
        assert_eq!(handle.join_with_timeout(PATIENCE), Ok(value as u32));
    }
}

/// A panic inside a child comes back to the parent as an error it can
/// deal with
#[test]
fn a_parent_sees_its_child_fail() {
    let _ = Runtime::init();

    let parent = Runtime::task(Compute::compute(|()| {
        let child =
            Runtime::task(Compute::compute(|()| -> u8 { panic!("a child went down") })).spawn();

        match child.join() {
            Err(RuntimeError::TaskFailed) => "handled",
            _ => "missed",
        }
    }))
    .spawn();

    assert_eq!(parent.join_with_timeout(PATIENCE), Ok("handled"));
}

/// A panic deep inside a recursive split fails only the branch it
/// was in
#[test]
fn a_panic_deep_in_a_split_fails_its_branch() {
    let _ = Runtime::init();

    fn split(depth: u32, doomed: u32) -> Result<u32, RuntimeError> {
        if depth == 0 {
            if doomed == 0 {
                panic!("a leaf went down");
            }

            return Ok(1);
        }

        let left = Runtime::task(Compute::compute(move |()| split(depth - 1, doomed))).spawn();
        let right = Runtime::task(Compute::compute(move |()| split(depth - 1, u32::MAX))).spawn();

        let left = left.join().and_then(|inner| inner);
        let right = right.join().and_then(|inner| inner);

        Ok(left.unwrap_or(0) + right?)
    }

    let intact = Runtime::task(Compute::compute(|()| split(8, u32::MAX))).spawn();
    let broken = Runtime::task(Compute::compute(|()| split(8, 0))).spawn();

    assert_eq!(intact.join_with_timeout(PATIENCE), Ok(Ok(256)));

    // Every left most branch loses its leaf, and nothing else does
    assert_eq!(broken.join_with_timeout(PATIENCE), Ok(Ok(255)));
}

/// A repeat that panics part way ends there
#[test]
fn a_repeat_that_panics_ends() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |()| {
        if counted.fetch_add(1, Ordering::SeqCst) == 3 {
            panic!("the fourth run went down");
        }
    }))
    .repeat()
    .every(Duration::from_millis(2))
    .spawn();

    assert!(
        settles(|| handle.is_finished()),
        "a repeat kept going after a run panicked"
    );

    assert_eq!(handle.maybe_join(), Err(RuntimeError::TaskFailed));

    std::thread::sleep(Duration::from_millis(50));

    assert_eq!(
        runs.load(Ordering::SeqCst),
        4,
        "a repeat ran again after it failed"
    );
}

/// A waiting task that panics on a give fails, and refuses the gives
/// after it
#[test]
fn a_waiting_task_that_panics_refuses_later_gives() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|value: i32| {
        if value < 0 {
            panic!("given a negative");
        }

        value
    }))
    .wait_for::<i32>()
    .spawn();

    handle.give(-1).expect("the first give was refused");

    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );

    assert!(
        settles(|| handle.give(1).is_err()),
        "a failed waiting task still took gives"
    );
}

/// A receive whose source panics finishes rather than waiting forever
#[test]
fn a_receive_from_a_panicking_source_finishes() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|()| -> u32 {
        panic!("the source went down")
    }))
    .spawn();

    let (ran, runs) = mpsc::channel();

    let receiver = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
    }))
    .receive(source.clone())
    .spawn();

    assert!(
        settles(|| receiver.is_finished()),
        "a receive outlived a source that could never publish"
    );

    assert!(
        runs.try_recv().is_err(),
        "the receive ran with a value a failed source never made"
    );
}

/// A receive that panics leaves its source untouched
#[test]
fn a_panicking_receive_leaves_its_source_alone() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|()| 12u32)).spawn();

    let receiver = Runtime::task(Compute::compute(|_: u32| -> u32 {
        panic!("the receive went down")
    }))
    .receive(source.clone())
    .count(1)
    .spawn();

    assert_eq!(
        receiver.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );
    assert_eq!(source.join_with_timeout(PATIENCE), Ok(12));
}

/// A task given outputs by `give_to` survives its source panicking on
/// some of its runs
#[test]
fn give_to_skips_the_runs_that_panicked() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let sink = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
    }))
    .wait_for::<u32>()
    .spawn();

    let sources: Vec<_> = (0..16u32)
        .map(|value| {
            Runtime::task(Compute::compute(move |()| {
                if value % 4 == 0 {
                    panic!("source {} went down", value);
                }

                value
            }))
            .give_to(&sink)
            .spawn()
        })
        .collect();

    for handle in sources {
        let _ = handle.join_with_timeout(PATIENCE);
    }

    std::thread::sleep(Duration::from_millis(100));

    let seen: Vec<u32> = runs.try_iter().collect();

    assert!(
        !seen.is_empty(),
        "no source that succeeded reached the sink"
    );
    assert!(
        seen.iter().all(|value| value % 4 != 0),
        "the sink ran with a value from a source that panicked: {:?}",
        seen
    );

    sink.cancel();
}

/// Panics from many computes at once leave every other task's value
/// alone
#[test]
fn a_flood_of_panics_leaves_everything_else_right() {
    let _ = Runtime::init();

    let handles: Vec<_> = (0..2_000u32)
        .map(|value| {
            Runtime::task(Compute::compute(move |()| {
                if value % 7 == 0 {
                    panic!("value {} went down", value);
                }

                value
            }))
            .spawn()
        })
        .collect();

    for (value, handle) in handles.into_iter().enumerate() {
        let value = value as u32;

        match value % 7 {
            0 => assert_eq!(
                handle.join_with_timeout(PATIENCE),
                Err(RuntimeError::TaskFailed)
            ),
            _ => assert_eq!(handle.join_with_timeout(PATIENCE), Ok(value)),
        }
    }
}
