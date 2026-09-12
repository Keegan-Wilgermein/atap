mod common;

use atap::{Runtime, Sleep};
use common::{max_rss, report_full};
use std::time::Duration;

/// Twelve million tasks alive at once, each holding its slot
#[test]
fn holds_a_peak_of_live_tasks() {
    Runtime::init();

    let tasks = 12_000_000;

    let baseline = max_rss();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1))).spawn())
        .collect();

    let peak = max_rss();
    let stats = Runtime::workers();

    report_full("all live, none read");

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    println!(
        "{} live tasks: {} bytes resident, {} each, was {} before, table at {} slots",
        tasks,
        peak,
        peak / tasks,
        baseline,
        stats.peak_slots(),
    );

    assert!(
        stats.peak_slots() >= tasks,
        "{} live handles but the table only handed out {} slots",
        tasks,
        stats.peak_slots(),
    );

    // Resident only, so compressed pages read under what a task
    // costs. A slot owning its own page would crash long before
    // this threshold
    assert!(
        peak < 5 * 1024 * 1024 * 1024,
        "{} live tasks put the process at {} bytes",
        tasks,
        peak,
    );
}
