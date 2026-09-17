mod common;

use atap::{
    Runtime,
    sleep::{Sleep, SleepMode},
};
use common::take_a_run;
use std::time::Duration;
use std::time::Instant;

/// `at_rate` starts runs on its period without waiting for the
/// last one to finish
#[test]
fn every_overlaps_its_runs() {
    let _ = Runtime::init();

    let interval = Duration::from_millis(50);
    let duration = Duration::from_millis(200);
    let runs: u32 = 5;

    // Four times the period, so four runs are in flight before
    // the first one has finished
    let handle = Runtime::task(Sleep::sleep(duration).mode(SleepMode::Relaxed))
        .at_rate(interval)
        .spawn();

    // The first output lands a whole duration in, so the clock
    // starts after it
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // What it would take if each run had to finish before the
    // next one started
    let serial = duration * runs;
    let ceiling = serial / 2;

    println!(
        "{} runs of {:?} on a {:?} period took {:?}, one at a time would be {:?}",
        runs, duration, interval, elapsed, serial,
    );

    assert!(
        elapsed < ceiling,
        "{} runs of {:?} took {:?}, so they were running one at a time",
        runs,
        duration,
        elapsed,
    );
}
