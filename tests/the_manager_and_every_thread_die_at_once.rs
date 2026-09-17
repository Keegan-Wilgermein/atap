//! # The manager and every thread at once
//! Everything that could notice a death dies together, and the runtime
//! still settles every task and comes back
//!
//! Kills the manager and every pool thread, so it has a binary to
//! itself. The rounds run in order, each on what the last left behind
//!
//! #### Note
//! Every injected death prints as it unwinds. That is the test
//! working

mod common;

use atap::{Runtime, RuntimeError, TaskHandle, compute::Compute};
use common::{report, report_full, settles};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Longer than anything here takes, so a task still unsettled at the
/// end of it was stranded
const PATIENCE: Duration = Duration::from_secs(30);

/// Kills every thread the pool has right now
fn kill_every_thread() -> (u32, u32) {
    let stats = Runtime::pool();

    let workers = stats.len() as u32;
    let sleeps = stats.sleep_threads() as u32;

    Runtime::inject_thread_deaths(workers, sleeps);

    (workers, sleeps)
}

/// Joins every handle, allowing only the full answer or a failure
///
/// ## Returns
/// How many came back whole and how many failed
fn settle_all(handles: Vec<(u64, TaskHandle<u64>)>, when: &str) -> (usize, usize) {
    let waited = Instant::now();
    let mut whole = 0;
    let mut failed = 0;

    for (wanted, handle) in handles {
        let left = PATIENCE.saturating_sub(waited.elapsed());

        match handle.join_with_timeout(left) {
            Ok(value) => {
                assert_eq!(
                    value, wanted,
                    "a task came back with somebody else's value {when}"
                );
                whole += 1;
            }

            Err(RuntimeError::TaskFailed) => failed += 1,

            Err(error) => panic!(
                "a task came back {:?} {when}, so it was stranded, pool {}",
                error,
                Runtime::pool(),
            ),
        }
    }

    (whole, failed)
}

/// Quick computes, blocking computes, a waiting task and a receive off
/// it, all spawned together
fn a_mix_of_work(
    count: u64,
) -> (
    Vec<(u64, TaskHandle<u64>)>,
    TaskHandle<u64, atap::builder::Waiting<u64>>,
    TaskHandle<u64>,
) {
    let mut handles: Vec<(u64, TaskHandle<u64>)> = (0..count)
        .map(|value| {
            (
                value * 3,
                Runtime::task(Compute::compute(move |()| value * 3)).spawn(),
            )
        })
        .collect();

    handles.extend((0..32u64).map(|value| {
        (
            value + 1_000,
            Runtime::task(
                Compute::compute(move |()| {
                    thread::sleep(Duration::from_millis(20));
                    value + 1_000
                })
                .blocking(),
            )
            .spawn(),
        )
    }));

    let waiting = Runtime::task(Compute::compute(|value: u64| value + 1))
        .wait_for::<u64>()
        .count(1)
        .spawn();

    let received = Runtime::task(Compute::compute(|value: u64| value * 2))
        .receive(waiting.clone())
        .count(1)
        .spawn();

    (handles, waiting, received)
}

/// Every round of it, one after another
#[test]
fn the_manager_and_every_thread_die_at_once() {
    let _ = Runtime::init();

    report_full("before");

    println!("\n== round 1: the manager and every thread die, and the manager comes back ==");
    round_one();

    println!("\n== round 2: the manager is gone for good, then every thread dies ==");
    round_two();

    println!("\n== round 3: every thread dies and none can be started again ==");
    round_three();

    println!("\n== round 4: a shutdown and a start bring it all back ==");
    round_four();

    report_full("finished");
}

/// The manager and every thread go at the same moment, with work queued
fn round_one() {
    let before = Runtime::pool();
    let (handles, waiting, received) = a_mix_of_work(20_000);

    Runtime::inject_manager_faults(1);
    let (workers, sleeps) = kill_every_thread();

    report("everything killed");

    waiting
        .give(41)
        .expect("a give after every thread died was refused");

    let (whole, failed) = settle_all(handles, "after everything died at once");

    assert_eq!(received.join_with_timeout(PATIENCE), Ok(84));
    assert_eq!(waiting.join_with_timeout(PATIENCE), Ok(42));

    Runtime::inject_thread_deaths(0, 0);

    assert!(
        settles(Runtime::healthy),
        "the runtime never came back from the manager and every thread dying: {:?}",
        Runtime::status(),
    );

    let after = Runtime::pool();

    println!(
        "{} workers and {} sleep threads killed with the manager: {} tasks whole, {} failed, \
         {} deaths recorded",
        workers,
        sleeps,
        whole,
        failed,
        after.deaths() - before.deaths(),
    );

    assert!(after.deaths() > before.deaths(), "no death was recorded");
    assert_eq!(after.recovering(), 0, "dead threads were left unrecovered");

    report("round one over");
}

/// With no manager at all, every thread dies and the pool brings
/// itself back
fn round_two() {
    // Past the restart limit, so the supervisor gives up
    Runtime::inject_manager_faults(16);

    assert!(
        settles(|| !Runtime::status().manager_alive()),
        "the manager never gave up"
    );

    // The faults it never got to, which would take down the manager a
    // later start brings up
    Runtime::inject_manager_faults(0);

    report("manager gone");

    let (handles, waiting, received) = a_mix_of_work(5_000);

    let (workers, sleeps) = kill_every_thread();

    waiting
        .give(9)
        .expect("a give with no manager and no threads was refused");

    let (whole, failed) = settle_all(handles, "with no manager and every thread dead");

    assert_eq!(received.join_with_timeout(PATIENCE), Ok(20));

    Runtime::inject_thread_deaths(0, 0);

    assert!(
        settles(|| Runtime::status().pool_alive()),
        "the pool never brought itself back with no manager: {}",
        Runtime::pool(),
    );

    // Work spawned afterwards runs on the pool that came back
    let later: Vec<_> = (0..1_000u64)
        .map(|value| {
            (
                value,
                Runtime::task(Compute::compute(move |()| value)).spawn(),
            )
        })
        .collect();

    let (later_whole, later_failed) = settle_all(later, "spawned after the pool recovered");

    assert_eq!(later_failed, 0, "work spawned after recovery failed");

    println!(
        "no manager, {} workers and {} sleep threads killed: {} whole and {} failed, then {} \
         more ran on the pool that came back",
        workers, sleeps, whole, failed, later_whole,
    );

    report("round two over");
}

/// Every thread dies, still with no manager, and every start is refused,
/// so everything left is written off rather than waiting forever
fn round_three() {
    // Every thread held first, so the work behind them is still queued
    // when they die
    let stats = Runtime::pool();

    let mut handles: Vec<(u64, TaskHandle<u64>)> = (0..stats.len() * 2)
        .map(|_| {
            (
                5,
                Runtime::task(Compute::compute(|()| {
                    thread::sleep(Duration::from_millis(100));
                    5
                }))
                .spawn(),
            )
        })
        .collect();

    handles.extend((0..stats.sleep_threads() + 8).map(|_| {
        (
            6,
            Runtime::task(
                Compute::compute(|()| {
                    thread::sleep(Duration::from_millis(100));
                    6
                })
                .blocking(),
            )
            .spawn(),
        )
    }));

    let (mix, _waiting, received) = a_mix_of_work(5_000);

    handles.extend(mix);

    Runtime::inject_spawn_refusals(u32::MAX);

    let (workers, sleeps) = kill_every_thread();

    let started = Instant::now();
    let (whole, failed) = settle_all(handles, "with nothing able to start");

    // Never given anything, so it can only end by being written off
    assert_eq!(
        received.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed),
        "a receive waiting on a pool that could never run it wasn't written off",
    );

    let written_off_in = started.elapsed();

    // A pool written off stays shut until a shutdown and a start
    assert_eq!(
        Runtime::task(Compute::compute(|()| 1u8))
            .spawn()
            .join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed),
        "a spawn onto a pool that was written off didn't fail",
    );

    // Blocking needs no pool
    assert_eq!(Runtime::block(Compute::compute(|()| 7u8)), 7);

    Runtime::inject_spawn_refusals(0);
    Runtime::inject_thread_deaths(0, 0);

    assert!(
        !Runtime::healthy(),
        "a runtime with no manager and no pool read healthy"
    );

    println!(
        "{} workers and {} sleep threads killed with no way back: {} whole and {} failed, \
         everything settled in {:?}",
        workers, sleeps, whole, failed, written_off_in,
    );

    assert!(failed > 0, "nothing was written off");

    report("round three over");
}

/// A shutdown and a start bring the manager and the pool back
fn round_four() {
    Runtime::shutdown();

    assert_eq!(Runtime::init(), Ok(()), "the runtime wouldn't start again");

    assert!(
        settles(Runtime::healthy),
        "the runtime came back unhealthy: {:?}",
        Runtime::status(),
    );

    let (handles, waiting, received) = a_mix_of_work(10_000);

    waiting
        .give(1)
        .expect("a give after starting again was refused");

    let (whole, failed) = settle_all(handles, "after starting again");

    assert_eq!(
        failed, 0,
        "work failed on a runtime that had just started again"
    );
    assert_eq!(received.join_with_timeout(PATIENCE), Ok(4));

    println!(
        "started again: {} tasks whole, the give and the receive both landed",
        whole
    );
}
