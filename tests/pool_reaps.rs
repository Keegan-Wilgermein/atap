mod common;

use atap::{Runtime, Sleep, SleepMode};
use common::cores;
use std::{thread, time::Duration};

/// Sleep threads with nothing to do are reaped
#[test]
fn pool_reaps_idle_sleep_threads() {
    Runtime::init();

    let handles: Vec<_> = (0..cores() * 4)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_millis(200)).mode(SleepMode::Relaxed)).spawn())
        .collect();

    // Read while they are all still in flight
    let peak = Runtime::workers().sleep_threads();

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    // Long enough for the idle window to pass
    thread::sleep(Duration::from_millis(1500));

    let settled = Runtime::workers().sleep_threads();

    println!("{} sleep threads at peak, {} once idle", peak, settled);

    assert!(
        settled < peak,
        "sleep threads held at {} after the idle window, having peaked at {}",
        settled,
        peak,
    );
}
