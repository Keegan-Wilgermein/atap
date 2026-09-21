//! # Idle After Burst
//! What the pool costs once a burst of work is over and nothing
//! is left to run

mod common;

use atap::{Runtime, compute::Compute};
use common::{Resources, cpu_time};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Tasks the burst runs
const TASKS: usize = 100;

/// How long the pool is watched with nothing to do
const QUIET: Duration = Duration::from_secs(5);

/// A pool that has just run a burst goes back to using almost no
/// cpu
#[test]
fn idle_after_burst() {
    let _ = Runtime::init();

    let before = cpu_time();
    let started = Instant::now();

    let handles: Vec<_> = (0..TASKS)
        .map(|value| Runtime::task(Compute::compute(move |()| value * 2)).spawn())
        .collect();

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    let burst = started.elapsed();
    let burnt = cpu_time().saturating_sub(before);

    println!(
        "{} tasks took {:?}, burning {:?} of cpu, {:.0}% of one core",
        TASKS,
        burst,
        burnt,
        burnt.as_secs_f64() / burst.as_secs_f64() * 100.0,
    );
    println!("  {}", Resources::now());

    // Nothing but the sleep runs here, so whatever cpu is charged
    // belongs to the pool
    let quiet_from = cpu_time();
    let quiet_started = Instant::now();

    thread::sleep(QUIET);

    let window = quiet_started.elapsed();
    let idled = cpu_time().saturating_sub(quiet_from);

    // Against one core, since a single worker left hunting is the
    // thing worth catching
    let share = idled.as_secs_f64() / window.as_secs_f64() * 100.0;

    println!(
        "then {:?} of cpu over a {:?} window, {:.3}% of one core",
        idled, window, share,
    );
    println!("  {}", Resources::now());
    println!("  pool {}", Runtime::pool());

    assert!(
        share < 5.0,
        "the pool burnt {:?} of cpu over {:?} with nothing to run, {:.3}% of a core, \
         so a worker stayed awake after the burst",
        idled,
        window,
        share,
    );
}
