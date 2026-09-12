mod common;

use atap::{Runtime, Sleep};
use common::max_rss;
use std::time::Duration;

/// Spawning and joining tasks over and over doesn't grow memory
#[test]
fn spawning_does_not_leak() {
    Runtime::init();

    let warmup = 1_000;
    let total = 200_000;
    let mut baseline = 0;

    for task in 0..total {
        let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1))).spawn();
        handle.join().expect("every task finishes");

        // Taken after the table has grown to its working size
        if task == warmup {
            baseline = max_rss();
        }
    }

    let after = max_rss();
    let growth = after.saturating_sub(baseline);

    println!(
        "baseline {} bytes, after {} bytes, growth {} bytes",
        baseline, after, growth
    );

    assert!(
        growth < 2 * 1024 * 1024,
        "{} tasks grew the process by {} bytes",
        total,
        growth,
    );
}
