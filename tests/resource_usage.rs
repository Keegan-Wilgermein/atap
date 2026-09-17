//! # Resource usage
//! What the runtime costs in cpu, memory and threads through each kind
//! of work, and whether it gives it all back
//!
//! Measures the whole process, which any other test would add to, so
//! it has a binary to itself. The phases run in order and each one is
//! measured against the last
//!
//! #### Note
//! The injected deaths in phase 8 print as they unwind. That is the
//! test working

mod common;

use atap::{Runtime, TaskHandle, compute::Compute};
use common::{Resources, cores, mebibytes, report};
use std::{
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(60);

/// How long an idle window is measured over
const IDLE_WINDOW: Duration = Duration::from_secs(2);

/// The most of one core an idle runtime may use
const IDLE_SHARE: f64 = 5.0;

/// A phase's name, what it did, and what the process looked like
/// before and after
fn measured(name: &str, work: impl FnOnce() -> String) -> (Resources, Resources) {
    let before = Resources::now();
    let started = Instant::now();

    let did = work();

    let wall = started.elapsed();
    let after = Resources::now();

    println!(
        "\n== {name} ==\n  {did}\n  took {:?}, {:.0}% of a core on average\n  before: {before}\n  \
         after:  {after}\n  footprint {:+.1} MiB, threads {:+}",
        wall,
        after.cpu_share_since(&before, wall),
        mebibytes(after.footprint) - mebibytes(before.footprint),
        after.threads as i64 - before.threads as i64,
    );

    (before, after)
}

/// Waits for the pool to go quiet, then measures how much cpu it uses
/// doing nothing
fn idle_share(when: &str) -> f64 {
    let waited = Instant::now();

    while waited.elapsed() < Duration::from_secs(30) {
        let stats = Runtime::pool();

        if !stats.has_any_task() && stats.live() == 0 {
            break;
        }

        thread::sleep(Duration::from_millis(50));
    }

    // Past the reapers, so their last few ticks aren't counted
    thread::sleep(Duration::from_secs(1));

    let before = Resources::now();
    let started = Instant::now();

    thread::sleep(IDLE_WINDOW);

    let share = Resources::now().cpu_share_since(&before, started.elapsed());

    println!("  idle {when}: {share:.2}% of one core over {IDLE_WINDOW:?}");

    assert!(
        share < IDLE_SHARE,
        "an idle runtime used {share:.2}% of a core {when}"
    );

    share
}

/// Fibonacci split into a task per call
fn fibonacci(n: u64) -> u64 {
    if n < 2 {
        return n;
    }

    let left = Runtime::task(Compute::compute(move |()| fibonacci(n - 1))).spawn();
    let right = Runtime::task(Compute::compute(move |()| fibonacci(n - 2))).spawn();

    left.join().expect("a branch failed") + right.join().expect("a branch failed")
}

/// Every phase, one after another
#[test]
fn resource_usage() {
    let (_, started) = measured("phase 1: starting the runtime", || {
        let _ = Runtime::init();
        format!("{} cores", cores())
    });

    report("started");
    idle_share("after starting");

    // ---- phase 2
    let (_, flooded) = measured(
        "phase 2: a hundred thousand computes spawned and joined",
        || {
            let mut total = 0u64;

            for batch in 0..100u64 {
                let handles: Vec<_> = (0..1_000u64)
                    .map(|value| Runtime::task(Compute::compute(move |()| batch * value)).spawn())
                    .collect();

                total += Runtime::join_all(handles)
                    .into_iter()
                    .map(|result| result.expect("a compute failed"))
                    .sum::<u64>();
            }

            format!("summed to {total}")
        },
    );

    assert!(
        mebibytes(flooded.footprint.saturating_sub(started.footprint)) < 256.0,
        "a hundred thousand computes grew the footprint by {:.1} MiB",
        mebibytes(flooded.footprint.saturating_sub(started.footprint)),
    );

    idle_share("after the flood");

    // ---- phase 3
    let (before_split, split) =
        measured("phase 3: fibonacci 24 split into a task per call", || {
            let answer = Runtime::task(Compute::compute(|()| fibonacci(24)))
                .spawn()
                .join_with_timeout(PATIENCE)
                .expect("the split failed");

            assert_eq!(answer, 46_368);

            format!(
                "came back {answer}, peak {} workers",
                Runtime::pool().peak_workers()
            )
        });

    assert!(
        split.threads <= before_split.threads + 256,
        "a recursive split took the process from {} threads to {}",
        before_split.threads,
        split.threads,
    );

    // ---- phase 4
    let (_, half) = measured("phase 4: fifty thousand gives to one waiting task", || {
        let summer = Runtime::task(Compute::compute(|value: u64| value * 2))
            .wait_for::<u64>()
            .spawn();

        for value in 0..50_000u64 {
            summer.give(value).expect("a give was refused");
        }

        summer.cancel();

        String::from("given, and the last value won each time")
    });

    let (_, full) = measured(
        "phase 5: fifty thousand more, to see whether any stayed",
        || {
            let summer = Runtime::task(Compute::compute(|value: Vec<u64>| value.len()))
                .wait_for::<Vec<u64>>()
                .spawn();

            for value in 0..50_000u64 {
                summer.give(vec![value; 64]).expect("a give was refused");
            }

            summer.cancel();

            String::from("given, each one owning memory")
        },
    );

    assert!(
        mebibytes(full.footprint.saturating_sub(half.footprint)) < 64.0,
        "the second fifty thousand gives grew the footprint by {:.1} MiB, so gives are kept",
        mebibytes(full.footprint.saturating_sub(half.footprint)),
    );

    idle_share("after the gives");

    // ---- phase 6
    measured(
        "phase 6: ten thousand receives off one source, then dropped",
        || {
            let source = Runtime::task(Compute::compute(|()| vec![7u8; 4096])).spawn();

            let receivers: Vec<TaskHandle<usize>> = (0..10_000)
                .map(|_| {
                    Runtime::task(Compute::compute(|bytes: Vec<u8>| bytes.len()))
                        .receive(source.clone())
                        .count(1)
                        .spawn()
                })
                .collect();

            let total: usize = Runtime::join_all(receivers)
                .into_iter()
                .map(|result| result.expect("a receive failed"))
                .sum();

            drop(source);

            format!("{total} bytes seen between them")
        },
    );

    let live_after_receives = {
        let waited = Instant::now();

        while Runtime::pool().live() > 0 && waited.elapsed() < Duration::from_secs(10) {
            thread::sleep(Duration::from_millis(20));
        }

        Runtime::pool().live()
    };

    println!("  live after the receives were dropped: {live_after_receives}");

    assert_eq!(
        live_after_receives, 0,
        "receives kept tasks alive after every handle to them was dropped",
    );

    // ---- phase 7
    let (before_blocking, _) = measured("phase 7: a burst of blocking computes", || {
        let handles: Vec<_> = (0..cores() * 16)
            .map(|index| {
                Runtime::task(
                    Compute::compute(move |()| {
                        thread::sleep(Duration::from_millis(100));
                        index
                    })
                    .blocking(),
                )
                .spawn()
            })
            .collect();

        for handle in handles {
            handle
                .join_with_timeout(PATIENCE)
                .expect("a blocking compute failed");
        }

        format!(
            "peak {} sleep threads",
            Runtime::pool().peak_sleep_threads()
        )
    });

    let threads_back = {
        let waited = Instant::now();

        while Resources::now().threads > before_blocking.threads + 8
            && waited.elapsed() < Duration::from_secs(30)
        {
            thread::sleep(Duration::from_millis(100));
        }

        Resources::now().threads
    };

    println!(
        "  threads after reaping: {threads_back}, against {} before the burst",
        before_blocking.threads
    );

    assert!(
        threads_back <= before_blocking.threads + 8,
        "the burst's sleep threads were never given back: {threads_back} threads against {}",
        before_blocking.threads,
    );

    idle_share("after the blocking burst");

    // ---- phase 8
    let (before_deaths, _) = measured("phase 8: every worker killed and brought back", || {
        let workers = Runtime::pool().len() as u32;

        Runtime::inject_thread_deaths(workers, 0);

        let answer = Runtime::task(Compute::compute(|()| fibonacci(16)))
            .spawn()
            .join_with_timeout(PATIENCE);

        Runtime::inject_thread_deaths(0, 0);

        let waited = Instant::now();

        while !Runtime::healthy() && waited.elapsed() < Duration::from_secs(10) {
            thread::sleep(Duration::from_millis(20));
        }

        format!(
            "{workers} killed, a split afterwards came back {:?}, {} deaths so far",
            answer,
            Runtime::pool().deaths()
        )
    });

    assert!(Runtime::healthy(), "{:?}", Runtime::status());

    let after_deaths = Resources::now();

    assert!(
        after_deaths.threads <= before_deaths.threads + 16,
        "recovering from the deaths left {} threads against {} before",
        after_deaths.threads,
        before_deaths.threads,
    );

    idle_share("after the deaths");

    // ---- phase 9
    let finished = Resources::now();

    println!(
        "\n== phase 9: the whole run ==\n  started: {started}\n  finished: {finished}\n  \
         footprint {:+.1} MiB since the flood, peak resident {:.1} MiB",
        mebibytes(finished.footprint) - mebibytes(flooded.footprint),
        mebibytes(finished.peak_resident),
    );

    report("finished");

    assert!(
        mebibytes(finished.footprint.saturating_sub(flooded.footprint)) < 128.0,
        "the footprint grew {:.1} MiB between the flood and the end, so something is kept",
        mebibytes(finished.footprint.saturating_sub(flooded.footprint)),
    );
}
