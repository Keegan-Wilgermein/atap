//! A program, rather than a test of one
//!
//! Ordinary work at an ordinary pace for half a minute, to catch
//! leaks, drift and schedules that quietly stop

use atap::{
    DEFAULT_PRIORITY, Runtime, RuntimeError,
    compute::Compute,
    fs::File,
    sleep::{Sleep, SleepMode},
};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

/// Roughly how long the program runs for
const RUNNING: Duration = Duration::from_secs(30);

/// How long one pass through the main loop aims to take
const TICK: Duration = Duration::from_millis(25);

/// Sums a range by splitting it into a task per half, down to chunks
/// small enough to add up in place
fn split_sum(from: u64, to: u64) -> u64 {
    if to - from <= 1_000 {
        return (from..to).sum();
    }

    let middle = from + (to - from) / 2;

    let left = Runtime::task(Compute::compute(move |()| split_sum(from, middle))).spawn();
    let right = split_sum(middle, to);

    left.join().expect("half of a split failed") + right
}

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
    let _ = Runtime::init();

    // ---- startup

    let root = workspace();
    let tag = std::process::id();

    let config = root.join(format!("everyday-config-{}.txt", tag));
    let log = root.join(format!("everyday-log-{}.txt", tag));
    let report = root.join(format!("everyday-report-{}.txt", tag));

    fs::write(&config, b"workers = 4\nverbose = false\n").expect("could not write the config");
    fs::write(&log, b"").expect("could not open the log");

    let line: Arc<[u8]> = Arc::from(b"tick\n".as_slice());

    // Watches the config, spaced since a slot holds the latest output
    // rather than a queue of them
    let watcher = Runtime::task(File::watch(&config))
        .repeat()
        .every(Duration::from_millis(50))
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

    // Settings the parser below hands every parse of the config to
    let settings = Arc::new(Mutex::new(HashMap::<String, String>::new()));

    let applier = {
        let settings = Arc::clone(&settings);

        Runtime::task(Compute::compute(move |parsed: HashMap<String, String>| {
            let mut settings = settings.lock().expect("the settings were poisoned");

            *settings = parsed;

            settings.len()
        }))
        .wait_for::<HashMap<String, String>>()
        .spawn()
    };

    // Given the config's bytes each time it is reloaded
    let parser = Runtime::task(Compute::compute(|bytes: Vec<u8>| {
        String::from_utf8_lossy(&bytes)
            .lines()
            .filter_map(|line| line.split_once(" = "))
            .map(|(key, value)| (key.trim().to_string(), value.trim().to_string()))
            .collect::<HashMap<String, String>>()
    }))
    .wait_for::<Vec<u8>>()
    .give_to(&applier)
    .spawn();

    // A running total the loop gives its counts to now and then
    let totals = Runtime::task(Compute::compute(|(served, checksums): (u64, u64)| {
        served + checksums
    }))
    .wait_for::<(u64, u64)>()
    .spawn();

    // The first parse, so the settings are there from the start
    parser
        .give(Runtime::block(File::read(&config)).expect("could not read the config"))
        .expect("the parser refused the config");

    println!("started, running for {:?}\n", RUNNING);

    // ---- the main loop

    let started = Instant::now();

    let mut ticks = 0u64;
    let mut served = 0u64;
    let mut edits = 0u64;
    let mut reloads = 0u64;
    let mut sweeps = 0u64;
    let mut checksums = 0u64;
    let mut splits = 0u64;

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

        // A little real work beside the sleeps, checked against the plain answer
        let tick = ticks;

        let sums: Vec<_> = (0..4u64)
            .map(|index| {
                Runtime::task(Compute::compute(move |()| {
                    (0..2_000u64)
                        .map(|value| value * (index + tick))
                        .sum::<u64>()
                }))
                .spawn()
            })
            .collect();

        for (index, result) in Runtime::join_all(sums).into_iter().enumerate() {
            let wanted = (0..2_000u64)
                .map(|value| value * (index as u64 + tick))
                .sum::<u64>();

            assert_eq!(
                result,
                Ok(wanted),
                "a checksum came back wrong on tick {}",
                tick
            );

            checksums += 1;
        }

        // The config is edited now and then, the way a running
        // program's is
        if ticks % 20 == 0 {
            edits += 1;

            let body = format!("workers = 4\nverbose = false\nedit = {}\n", edits);

            Runtime::block(File::write(&config, body.into_bytes()))
                .expect("could not edit the config");
        }

        // Pick up a config reload if the watch caught one
        if let Ok(Ok(change)) = watcher.try_take() {
            assert!(
                !change.removed(),
                "the config went away rather than changing",
            );

            let reloaded = Runtime::block(File::read(&config)).expect("could not reload");

            assert!(!reloaded.is_empty(), "the config came back empty");

            // Parsed on a worker and applied by the task it hands its output to
            parser
                .give(reloaded)
                .expect("the parser stopped taking the config");

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

            totals
                .give((served, checksums))
                .expect("the totals stopped taking counts");
        }

        // A report gathered from a split and a look at the settings
        if ticks % 80 == 0 {
            let split = Runtime::task(Compute::compute(|()| split_sum(0, 100_000))).spawn();

            let keys = {
                let settings = Arc::clone(&settings);

                Runtime::task(Compute::compute(move |()| {
                    settings.lock().expect("the settings were poisoned").len()
                }))
                .spawn()
            };

            let gathered = Runtime::task(Compute::compute(|(sum, keys): (u64, usize)| {
                format!("split {sum} with {keys} settings")
            }))
            .receive((split, keys))
            .count(1)
            .spawn()
            .join()
            .expect("the report never came together");

            assert!(
                gathered.starts_with(&format!("split {}", (0..100_000u64).sum::<u64>())),
                "a split came back wrong: {}",
                gathered,
            );

            splits += 1;
        }

        // A bounded retry job, now and then
        if ticks % 200 == 0 {
            let retries =
                Runtime::task(Sleep::sleep(Duration::from_millis(2)).mode(SleepMode::Relaxed))
                    .repeat()
                    .every(Duration::from_millis(20))
                    .count(3)
                    .spawn();

            // Left to run itself out
            drop(retries);

            println!(
                "  [{:>5.1}s] {} ticks, {} served, {} checksums, {} splits, {} edits, {} reloads, \
                 {} sweeps, {} settings",
                started.elapsed().as_secs_f32(),
                ticks,
                served,
                checksums,
                splits,
                edits,
                reloads,
                sweeps,
                settings.lock().expect("the settings were poisoned").len(),
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

    // The last reload made it through the parser to the settings
    let deadline = Instant::now() + Duration::from_secs(10);

    let applied = loop {
        let settings = settings.lock().expect("the settings were poisoned").clone();

        if settings
            .get("edit")
            .is_some_and(|edit| edit == &edits.to_string())
            || Instant::now() >= deadline
        {
            break settings;
        }

        Runtime::sleep(Duration::from_millis(5));
    };

    // The last counts given by hand come back added up
    totals
        .give((served, checksums))
        .expect("the totals stopped taking counts");

    let total = loop {
        match totals.take_with_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(total) if total == served + checksums => break total,
            Ok(_) | Err(RuntimeError::AlreadyTaken) if Instant::now() < deadline => {
                std::thread::yield_now()
            }
            other => panic!("the totals never caught up: {:?}", other),
        }
    };

    parser.cancel();
    applier.cancel();
    totals.cancel();

    println!(
        "\nfinished after {:?}\n  {} ticks, {} served, {} checksums, {} splits, {} edits, {} \
         reloads, {} sweeps, {} bytes logged, total {}\n  settings applied: {:?}",
        ran, ticks, served, checksums, splits, edits, reloads, sweeps, written, total, applied,
    );

    println!("\n{}", Runtime::pool());

    // ---- what any of that was worth

    assert!(
        Runtime::healthy(),
        "the runtime did not survive an ordinary half minute: {:?}",
        Runtime::status(),
    );

    assert!(ticks > 100, "only {} ticks in {:?}", ticks, ran);

    assert_eq!(
        checksums,
        ticks * 4,
        "{} of {} checksums never came back",
        ticks * 4 - checksums,
        ticks * 4
    );

    assert!(splits > 0, "no split ever ran");

    assert_eq!(
        applied.get("edit").map(String::as_str),
        Some(edits.to_string().as_str()),
        "the settings never took the last edit: {:?}",
        applied,
    );

    assert_eq!(
        served,
        ticks * 8,
        "{} of {} batches came back short",
        ticks * 8 - served,
        ticks * 8
    );

    // The two schedules ran for the whole run
    assert!(
        reloads > 10,
        "{} edits to the config and the watch caught only {}",
        edits,
        reloads,
    );

    assert!(
        written >= line.len() as u64 * 10,
        "the heartbeat wrote {} bytes over {:?}",
        written,
        ran,
    );

    // Still takes work afterwards, spawned and blocking alike
    let last: Result<Vec<u8>, RuntimeError> = Runtime::task(File::read(&config))
        .spawn()
        .join()
        .expect("the runtime stopped taking work");

    assert!(last.is_ok(), "the last read failed: {:?}", last);

    for path in [&config, &log, &report] {
        let _ = fs::remove_file(path);
    }
}
