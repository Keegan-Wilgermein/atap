mod common;

use atap::{Runtime, Sleep};
use common::{report, take_a_run};
use std::{thread, time::Duration};

/// A repeat waiting out its interval holds no thread and
/// grows nothing
#[test]
fn waiting_costs_no_thread() {
    Runtime::init();

    let interval = Duration::from_secs(1);

    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1))).repeat().every(interval).spawn();

    // The first run out of the way, so what follows is the wait
    take_a_run(&handle);

    // Well into the interval, and past enough manager ticks for
    // the pool to have grown if it was going to
    thread::sleep(Duration::from_millis(300));

    let stats = Runtime::workers();
    report("waiting out an interval");

    handle.clone().cancel();

    println!(
        "waiting out {:?}: {} workers busy, {} sleep threads ({} busy), {} waiting anywhere",
        interval,
        stats.busy(),
        stats.sleep_threads(),
        stats.sleep_busy(),
        stats.backlog(),
    );

    assert_eq!(
        stats.busy(),
        0,
        "a task that was only waiting had {} workers busy",
        stats.busy(),
    );

    assert_eq!(
        stats.sleep_busy(), 0,
        "a task that was only waiting had {} sleep threads busy",
        stats.sleep_busy(),
    );

    assert_eq!(
        stats.backlog(),
        0,
        "a task that was only waiting left {} queued",
        stats.backlog(),
    );
}
