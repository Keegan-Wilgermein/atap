//! # Shared test helpers

#![allow(dead_code)]

use atap::{Runtime, RuntimeError, TaskHandle};
use std::{
    mem, thread,
    time::{Duration, Instant},
};

/// A line of what the pool is doing at this moment
pub fn report(at: &str) {
    let stats = Runtime::workers();

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

/// Everything the pool is doing, workers and all
pub fn report_full(at: &str) {
    println!("  [{}]\n{}", at, Runtime::workers());
}

/// Online cores, which the pool sizes itself against
pub fn cores() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

/// The high water mark of the process's resident memory
pub fn max_rss() -> usize {
    let mut usage: libc::rusage = unsafe { mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };

    usage.ru_maxrss as usize
}

/// Waits for the next run of a repeating task and takes it
pub fn take_a_run(handle: &TaskHandle<Duration>) -> Duration {
    let mut polls = 0u64;
    let waited = Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The next run hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!(
                "a repeating task came back with {:?} after {} polls, pool {:?}",
                error,
                polls,
                Runtime::workers(),
            ),
        }

        polls += 1;

        // A series that stopped producing fails rather than hangs
        assert!(
            waited.elapsed() < Duration::from_secs(30),
            "a repeating task stopped producing runs after {} polls, pool {:?}",
            polls,
            Runtime::workers(),
        );

        thread::sleep(Duration::from_micros(100));
    }
}
