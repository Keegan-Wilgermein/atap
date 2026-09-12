//! A program, rather than a test of one
//!
//! Ordinary work at an ordinary pace for half a minute, mixing
//! spawned tasks with blocking ones, to catch leaks, drift and
//! schedules that quietly stop

use atap::{DEFAULT_PRIORITY, File, Runtime, RuntimeError, Sleep, SleepMode};
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

/// Roughly how long the program runs for
const RUNNING: Duration = Duration::from_secs(30);

/// How long one pass through the main loop aims to take
const TICK: Duration = Duration::from_millis(25);

/// Where its files live
fn workspace() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    root
}

/// Half a minute of ordinary work gets done, and the runtime
/// still works afterwards
#[test]
fn a_program_that_just_runs() {
    Runtime::init();

    // ---- startup

    let root = workspace();
    let tag = std::process::id();

    let config = root.join(format!("everyday-config-{}.txt", tag));
    let log = root.join(format!("everyday-log-{}.txt", tag));
    let report = root.join(format!("everyday-report-{}.txt", tag));

    fs::write(&config, b"workers = 4\nverbose = false\n").expect("could not write the config");
    fs::write(&log, b"").expect("could not open the log");

    let line: Arc<[u8]> = Arc::from(b"tick\n".as_slice());

    // Watches the config the way a program that reloads on
    // change would
    let watcher = Runtime::task(File::read(&config))
        .repeat()
        .every(Duration::from_millis(500))
        .spawn();

    // A heartbeat in the log
    let heartbeat = Runtime::task(File::append(&log, Arc::clone(&line)))
        .repeat()
        .every(Duration::from_secs(1))
        .spawn();

    // Something a program does once, a moment after starting
    let warmup = Runtime::task(Sleep::sleep(Duration::from_millis(5)).mode(SleepMode::Relaxed))
        .after(Duration::from_millis(250))
        .spawn();

    println!("started, running for {:?}\n", RUNNING);

    // ---- the main loop

    let started = Instant::now();

    let mut ticks = 0u64;
    let mut served = 0u64;
    let mut reloads = 0u64;
    let mut sweeps = 0u64;

    while started.elapsed() < RUNNING {
        let tick_began = Instant::now();

        ticks += 1;

        // A batch of work, one item of which matters more than the rest
        let batch: Vec<_> = (0..8u64)
            .map(|index| {
                let priority = match index {
                    0 => 200,
                    _ => DEFAULT_PRIORITY,
                };

                Runtime::task(Sleep::sleep(Duration::from_micros(index * 200 + 50)))
                    .priority(priority)
                    .spawn()
            })
            .collect();

        served += Runtime::join_all(batch)
            .into_iter()
            .filter(|result| result.is_ok())
            .count() as u64;

        // Pick up a config reload if the watcher has one ready
        if let Ok(Ok(bytes)) = watcher.maybe_take() {
            assert!(!bytes.is_empty(), "the config came back empty");

            reloads += 1;
        }

        // Housekeeping, about once a second, blocking
        if ticks % 40 == 0 {
            sweeps += 1;

            let meta = Runtime::block(File::metadata(&log)).expect("could not stat the log");

            assert!(meta.is_file(), "the log stopped being a file");

            let listing = Runtime::block(File::read_dir(&root)).expect("could not list the files");

            assert!(!listing.is_empty(), "the directory came back empty");

            // A snapshot rather than a history
            let snapshot = format!(
                "tick {}, served {}, reloads {}, log {} bytes\n",
                ticks,
                served,
                reloads,
                meta.len(),
            );

            Runtime::block(File::write(&report, snapshot.into_bytes()))
                .expect("could not write the report");
        }

        // A bounded retry job, now and then
        if ticks % 200 == 0 {
            let retries = Runtime::task(Sleep::sleep(Duration::from_millis(2)).mode(SleepMode::Relaxed))
                .repeat()
                .every(Duration::from_millis(20))
                .count(3)
                .spawn();

            // Left to run itself out
            drop(retries);

            println!(
                "  [{:>5.1}s] {} ticks, {} served, {} reloads, {} sweeps",
                started.elapsed().as_secs_f32(),
                ticks,
                served,
                reloads,
                sweeps,
            );
        }

        // Wait out the rest of the tick
        if let Some(left) = TICK.checked_sub(tick_began.elapsed()) {
            Runtime::sleep(left);
        }
    }

    // ---- shutdown

    watcher.cancel();
    heartbeat.cancel();

    // Started before the loop and long since done
    let warmed = warmup.join();

    assert!(warmed.is_ok(), "the warmup never landed: {:?}", warmed);

    let ran = started.elapsed();
    let written = fs::metadata(&log).expect("the log went missing").len();

    println!(
        "\nfinished after {:?}\n  {} ticks, {} served, {} reloads, {} sweeps, {} bytes logged",
        ran, ticks, served, reloads, sweeps, written,
    );

    println!("\n{}", Runtime::workers());

    // ---- what any of that was worth

    assert!(
        Runtime::healthy(),
        "the runtime did not survive an ordinary half minute: {:?}",
        Runtime::status(),
    );

    assert!(ticks > 100, "only {} ticks in {:?}", ticks, ran);

    assert_eq!(served, ticks * 8, "{} of {} batches came back short", ticks * 8 - served, ticks * 8);

    // The two schedules ran for the whole run
    assert!(reloads > 10, "the config watcher only ran {} times", reloads);

    assert!(
        written >= line.len() as u64 * 10,
        "the heartbeat wrote {} bytes over {:?}",
        written,
        ran,
    );

    // Still takes work afterwards, spawned and blocking alike
    let last: Result<Vec<u8>, RuntimeError> =
        Runtime::task(File::read(&config)).spawn().join().expect("the runtime stopped taking work");

    assert!(last.is_ok(), "the last read failed: {:?}", last);

    for path in [&config, &log, &report] {
        let _ = fs::remove_file(path);
    }
}
