//! # Memory Peak At Work
//! Twenty million tasks alive at once, nearly all of them real
//! work for a worker
//!
//! A handful of waits go in beside them, so the blocking half of
//! the pool is carrying something too

mod common;

use atap::{
    Runtime,
    compute::Compute,
    sleep::{Sleep, SleepMode},
};
use common::{max_rss, report_full};
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// How often the pool is printed while the test runs
const WATCH: Duration = Duration::from_millis(500);

/// About how long one task keeps a worker
///
/// Measured rather than counted in rounds, since the same loop is
/// many times faster with optimisations on
const UNIT: Duration = Duration::from_micros(45);

/// Tasks that run on a worker
const COMPUTES: usize = 25_000_000;

/// Waits handed to a sleep thread, kept to a trickle beside the
/// work
const RELAXED: usize = 100_000;

/// Every task the test holds at once
const TASKS: usize = COMPUTES + RELAXED;

/// Twenty million tasks that all have work to do are held live at
/// once, and every one of them finishes
#[test]
#[ignore = "Not required to run every time"]
fn holds_a_peak_of_working_tasks() {
    let _ = Runtime::init();

    let rounds = calibrate();

    println!("{rounds} rounds of work comes to about {UNIT:?} on this machine");

    let baseline = max_rss();
    let started = Instant::now();

    // Printed while the work is going, since the pool grows and
    // shrinks around it rather than holding one size
    let done = Arc::new(AtomicBool::new(false));
    let watching = watch(Arc::clone(&done), started);

    // Three shapes, and a length that varies with the index, so the
    // pool is carrying long and short work of different kinds at
    // once
    let handles: Vec<_> = (0..COMPUTES)
        .map(|index| {
            // Averages out to `rounds`, so the whole run is about
            // `UNIT` a task
            let long = rounds / 2 + (index % 8) as u64 * rounds / 8;
            let seed = index as u64;

            Runtime::task(Compute::compute(move |()| work(long, seed))).spawn()
        })
        .collect();

    let waits: Vec<_> = (0..RELAXED)
        .map(|_| {
            Runtime::task(Sleep::sleep(Duration::from_millis(2)).mode(SleepMode::Relaxed)).spawn()
        })
        .collect();

    let spawned = started.elapsed();
    let peak = max_rss();
    let stats = Runtime::pool();

    // Holding a slot is not the same as still having work to do,
    // since a handle keeps a finished task's slot alive. What is
    // waiting to run is what says the pool fell behind
    let waiting = stats.queued() + stats.backlog();

    report_full("all live, none read");

    let mut summed = 0u64;
    let mut failed = 0;

    for handle in handles {
        match handle.join() {
            Ok(value) => summed = summed.wrapping_add(value),
            Err(_) => failed += 1,
        }
    }

    for handle in waits {
        if handle.join().is_err() {
            failed += 1;
        }
    }

    let took = started.elapsed();

    done.store(true, Ordering::Relaxed);
    watching.join().expect("the watcher stops");

    let after = Runtime::pool();

    println!(
        "\n{} tasks held at once: {} on workers at {:?} each, {} waits",
        TASKS, COMPUTES, UNIT, RELAXED,
    );
    println!(
        "  {:?} to spawn them all, {:?} until the last one was read",
        spawned, took,
    );
    println!(
        "  {} bytes resident at the peak, {} each, was {} before, table at {} slots",
        peak,
        peak / TASKS,
        baseline,
        stats.peak_slots(),
    );
    println!(
        "  {} holding a slot, {} of them not run yet: {} queued and {} in worker rings",
        stats.live(),
        waiting,
        stats.queued(),
        stats.backlog(),
    );
    println!(
        "  workers peaked at {}, sleep threads at {}, checksum {}, {} failed",
        after.peak_workers(),
        after.peak_sleep_threads(),
        summed,
        failed,
    );

    report_full("finished");

    println!(
        "\n{}",
        match failed {
            0 => "every task came back",
            _ => "some tasks never came back",
        },
    );

    assert_eq!(failed, 0, "{} of {} tasks never came back", failed, TASKS);

    // The point of the mix: the pool is still carrying most of it
    // when the spawning ends
    assert!(
        waiting > TASKS / 5,
        "only {} of {} tasks were still waiting to run once they were all spawned",
        waiting,
        TASKS,
    );

    assert!(
        peak / TASKS < 512,
        "{} live tasks cost {} bytes each",
        TASKS,
        peak / TASKS,
    );

    assert!(
        peak < 10 * 1024 * 1024 * 1024,
        "{} live tasks put the process at {} bytes",
        TASKS,
        peak,
    );
}

/// One task's work
///
/// Three shapes by index: a mix that depends on its own last
/// answer, a walk over a small buffer, and a count of Collatz steps
fn work(rounds: u64, seed: u64) -> u64 {
    match seed % 3 {
        0 => mix(rounds, seed),

        1 => {
            let len = (rounds / 4).max(16) as usize;
            let mut buffer: Vec<u64> = (0..len as u64).map(|value| value ^ seed).collect();

            for index in 0..len {
                buffer[index] = buffer[index].wrapping_add(buffer[(index * 7 + 1) % len]);
            }

            buffer.iter().fold(0u64, |sum, value| sum ^ value)
        }

        // Bounded by the same count as the others, since a walk to
        // one from a large seed is far longer than it looks
        _ => {
            let mut value = seed | 1;

            for step in 0..rounds {
                value = match value % 2 {
                    0 => value / 2,
                    _ => value.wrapping_mul(3).wrapping_add(1),
                };

                if value == 1 {
                    value = (seed ^ step) | 1;
                }
            }

            value
        }
    }
}

/// A mix each round of which needs the last one's answer
fn mix(rounds: u64, seed: u64) -> u64 {
    let mut value = seed | 1;

    for _ in 0..rounds {
        value = value
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);

        value ^= value >> 29;
    }

    value
}

/// Rounds of `mix` that take about `UNIT` here
fn calibrate() -> u64 {
    // Warmed first, since a cold run reads many times slower and
    // would size every task far too small
    black_box(mix(100_000, 1));

    let mut rounds = 1_024u64;

    loop {
        let mut best = Duration::MAX;

        for _ in 0..5 {
            let started = Instant::now();

            black_box(mix(rounds, 1));

            best = best.min(started.elapsed());
        }

        if best >= UNIT / 4 {
            let scaled = rounds as f64 * UNIT.as_secs_f64() / best.as_secs_f64();

            return (scaled as u64).max(16);
        }

        rounds *= 4;
    }
}

/// Prints what the pool is carrying until it is told to stop
fn watch(done: Arc<AtomicBool>, started: Instant) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        while !done.load(Ordering::Relaxed) {
            let stats = Runtime::pool();

            println!(
                "  [{:>6.1}s] {} workers ({} busy), {} sleep threads, \
                 {} queued, {} in rings, {} live",
                started.elapsed().as_secs_f32(),
                stats.len(),
                stats.busy(),
                stats.sleep_threads(),
                stats.queued(),
                stats.backlog(),
                stats.live(),
            );

            thread::sleep(WATCH);
        }
    })
}
