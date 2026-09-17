//! # A soft worker target
//! The pool settles around its target under ordinary load, grows past
//! it only when work truly can't move, never past its ceiling, and
//! gives the extra threads back quickly afterwards
//!
//! Reads the pool's size and peaks, which every other test would move,
//! so it has a binary to itself. The phases run in order, since the
//! peaks only ever go up

mod common;

use atap::{Runtime, compute::Compute};
use common::{cores, report};
use std::{
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(60);

/// Waits for something to become true, with a cap
///
/// ## Returns
/// How long it took, or `None` if it never did
fn within(cap: Duration, mut condition: impl FnMut() -> bool) -> Option<Duration> {
    let waited = Instant::now();

    while waited.elapsed() < cap {
        if condition() {
            return Some(waited.elapsed());
        }

        thread::sleep(Duration::from_millis(10));
    }

    condition().then(|| waited.elapsed())
}

/// Every phase, one after another
#[test]
fn the_worker_target_bends_under_real_need() {
    let _ = Runtime::init();

    let start = Runtime::pool();
    let floor = start.len();

    println!(
        "{} cores: {} workers to start, target {}, sleep target {}, ceiling {}",
        cores(),
        floor,
        start.target(),
        start.sleep_target(),
        start.ceiling(),
    );

    report("idle");

    // ---- phase 1: work that keeps moving stays within the target
    println!("\n== phase 1: a flood of quick computes stays within the target ==");

    let flood = cores() as u64 * 4_000;
    let started = Instant::now();

    let handles: Vec<_> = (0..flood)
        .map(|value| Runtime::task(Compute::compute(move |()| value ^ 0x5A5A)).spawn())
        .collect();

    for (value, handle) in handles.into_iter().enumerate() {
        assert_eq!(
            handle.join_with_timeout(PATIENCE),
            Ok(value as u64 ^ 0x5A5A)
        );
    }

    let moving = Runtime::pool();

    println!(
        "{} quick computes in {:?}, peak {} workers against a target of {}",
        flood,
        started.elapsed(),
        moving.peak_workers(),
        moving.target(),
    );

    assert!(
        moving.peak_workers() <= moving.target(),
        "work that kept moving grew the pool to {}, past its target of {}",
        moving.peak_workers(),
        moving.target(),
    );

    // ---- phase 2: every worker held, with work queued behind them
    println!("\n== phase 2: computes that hold their workers make it grow past the target ==");

    let target = moving.target();
    let held = target * 2;
    let hold = Duration::from_millis(400);

    let started = Instant::now();

    // Not marked blocking, so each one really holds a worker
    let holders: Vec<_> = (0..held)
        .map(|index| {
            Runtime::task(Compute::compute(move |()| {
                thread::sleep(hold);
                index
            }))
            .spawn()
        })
        .collect();

    let queued: Vec<_> = (0..2_000u64)
        .map(|value| Runtime::task(Compute::compute(move |()| value + 7)).spawn())
        .collect();

    let mut answered_after = Duration::ZERO;

    for (value, handle) in queued.into_iter().enumerate() {
        assert_eq!(handle.join_with_timeout(PATIENCE), Ok(value as u64 + 7));
        answered_after = started.elapsed();
    }

    let grown = Runtime::pool();

    report("held and grown");

    for (index, handle) in holders.into_iter().enumerate() {
        assert_eq!(handle.join_with_timeout(PATIENCE), Ok(index));
    }

    let held_for = started.elapsed();

    println!(
        "{} computes held their workers for {:?}: the queue behind them was answered after \
         {:?}, all done in {:?}, peak {} workers against a target of {}",
        held,
        hold,
        answered_after,
        held_for,
        grown.peak_workers(),
        target,
    );

    assert!(
        grown.peak_workers() > target,
        "every worker was held with work queued, and the pool never grew past {}",
        target,
    );

    // Two waves of holders one after the other would take twice as long
    assert!(
        held_for < hold * 2,
        "{} holders took {:?}, so they ran in waves rather than on the workers grown for them",
        held,
        held_for,
    );

    // ---- phase 3: the ceiling
    println!("\n== phase 3: never past the ceiling ==");

    assert!(
        grown.peak_workers() <= grown.ceiling(),
        "{} workers, past the ceiling of {}",
        grown.peak_workers(),
        grown.ceiling(),
    );

    println!(
        "peak {} workers, ceiling {}",
        grown.peak_workers(),
        grown.ceiling()
    );

    // ---- phase 4: the extra workers go quickly
    println!("\n== phase 4: workers past the target are reaped quickly ==");

    let reaped_to_target = within(Duration::from_secs(10), || Runtime::pool().len() <= target);

    report("back to target");

    println!("back to the target of {} in {:?}", target, reaped_to_target);

    assert!(
        reaped_to_target.is_some_and(|took| took < Duration::from_secs(3)),
        "workers past the target outstayed the work that earned them: {} still running",
        Runtime::pool().len(),
    );

    // ---- phase 5: below the target, idle workers go at the normal pace
    println!("\n== phase 5: idle workers below the target are reaped to where it started ==");

    let reaped_to_floor = within(Duration::from_secs(30), || Runtime::pool().len() <= floor);

    report("back where it started");

    println!("back to {} workers in {:?}", floor, reaped_to_floor);

    assert!(
        reaped_to_floor.is_some(),
        "the pool never came back to its {} workers: {}",
        floor,
        Runtime::pool().len(),
    );

    // ---- phase 6: blocking computes grow the sleep threads, not workers
    println!("\n== phase 6: blocking computes bend the sleep thread target instead ==");

    let before = Runtime::pool();
    let sleep_target = before.sleep_target();
    let waits = sleep_target * 2;
    let wait = Duration::from_millis(400);

    let started = Instant::now();

    let waiting: Vec<_> = (0..waits)
        .map(|index| {
            Runtime::task(
                Compute::compute(move |()| {
                    thread::sleep(wait);
                    index
                })
                .blocking(),
            )
            .spawn()
        })
        .collect();

    for (index, handle) in waiting.into_iter().enumerate() {
        assert_eq!(handle.join_with_timeout(PATIENCE), Ok(index));
    }

    let blocked = Runtime::pool();

    report("blocking computes done");

    println!(
        "{} blocking computes of {:?} took {:?}: peak {} sleep threads against a target of {}, \
         workers peaked at {}",
        waits,
        wait,
        started.elapsed(),
        blocked.peak_sleep_threads(),
        sleep_target,
        blocked.peak_workers(),
    );

    assert!(
        blocked.peak_sleep_threads() > sleep_target,
        "twice the sleep target of blocking computes never grew the sleep threads past {}",
        sleep_target,
    );

    assert!(
        blocked.peak_sleep_threads() <= blocked.ceiling(),
        "{} sleep threads, past the ceiling of {}",
        blocked.peak_sleep_threads(),
        blocked.ceiling(),
    );

    assert_eq!(
        blocked.peak_workers(),
        before.peak_workers(),
        "blocking computes grew the workers rather than the sleep threads",
    );

    // ---- phase 7: and the sleep threads come back down
    println!("\n== phase 7: sleep threads past their target are reaped ==");

    let reaped_sleeps = within(Duration::from_secs(10), || {
        Runtime::pool().sleep_threads() <= sleep_target
    });

    report("finished");

    println!(
        "sleep threads back within their target of {} in {:?}",
        sleep_target, reaped_sleeps
    );

    assert!(
        reaped_sleeps.is_some(),
        "{} sleep threads still running past the target of {}",
        Runtime::pool().sleep_threads(),
        sleep_target,
    );

    assert!(Runtime::healthy(), "{:?}", Runtime::status());
}
