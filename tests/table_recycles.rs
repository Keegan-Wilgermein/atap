mod common;

use atap::{Runtime, Sleep};
use common::report;
use std::time::Duration;

/// Waves of tasks reuse the same ids rather than growing the
/// table
#[test]
fn recycles_ids_forever() {
    Runtime::init();

    let waves = 16;
    let per_wave = 100_000;

    let wave = || {
        let handles: Vec<_> = (0..per_wave)
            .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1))).spawn())
            .collect();

        for handle in handles {
            handle.join().expect("every task finishes");
        }
    };

    // The first wave grows the table, so it is the baseline
    wave();
    let settled = Runtime::workers().peak_slots();
    report("one wave in");

    for _ in 1..waves {
        wave();
    }

    report("all waves through");

    let after = Runtime::workers().peak_slots();

    println!(
        "{} waves of {}: table settled at {} slots, ended at {}",
        waves, per_wave, settled, after,
    );

    // Slack for a spawn that lands while an automatic trim has
    // the free list out
    let slack = per_wave / 10;

    assert!(
        after <= settled + slack,
        "{} tasks through a table that only ever held {} at once grew it from {} to {}",
        waves * per_wave,
        per_wave,
        settled,
        after,
    );
}
