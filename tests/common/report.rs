//! # Report

use atap::Runtime;

/// A line of what the pool is doing at this moment
pub fn report(at: &str) {
    let stats = Runtime::pool();

    println!(
        "  [{}] {} workers ({} busy), {} sleep threads ({} busy), \
         {} queued, {} blocking, {} backlog, {} live, {} slots",
        at,
        stats.len(),
        stats.busy(),
        stats.sleep_threads(),
        stats.sleep_busy(),
        stats.queued(),
        stats.blocking_queued(),
        stats.backlog(),
        stats.live(),
        stats.peak_slots(),
    );
}
