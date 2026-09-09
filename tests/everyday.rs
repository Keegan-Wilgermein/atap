//! A program, rather than a test of one
//!
//! Nothing here is trying to break anything. It is the other
//! shape of coverage from `stress` — a runtime doing ordinary
//! work at an ordinary pace for half a minute, mixing spawned
//! tasks with blocking ones the way real code does, because it
//! happens to want both and not because either is being
//! examined
//!
//! The point is what it would catch: a leak, a drift, a
//! schedule that quietly stops after a few minutes, a slot
//! count that only climbs. None of those show up in a test that
//! runs for a tenth of a second and asserts on one number
//!
//! So there is very little asserted at the end, and what there
//! is says the program got through its work rather than that
//! any particular call did what it was told

use atap::{DEFAULT_PRIORITY, File, Runtime, RuntimeError, Sleep};
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
///
/// `tests/files` is ignored whole, and git doesn't track
/// directories, so an ignored one doesn't survive a clone
fn workspace() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    root
}

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
    // change would. Nothing here ever changes it, which is the
    // ordinary case — a watcher mostly finds the same bytes it
    // found last time
    let watcher = Runtime::task(File::read(&config))
        .repeat()
        .every(Duration::from_millis(500))
        .spawn();

    // A heartbeat in the log. Appends rather than writes, so
    // it can't lose what came before it
    let heartbeat = Runtime::task(File::append(&log, Arc::clone(&line)))
        .repeat()
        .every(Duration::from_secs(1))
        .spawn();

    // The sort of thing a program does once, a moment after
    // starting, when it doesn't want to do it during startup
    let warmup = Runtime::task(Sleep::sleep(Duration::from_millis(5), false))
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

        // A batch of work, one item of which matters more than
        // the rest. Exactly the reason priorities exist, and
        // exactly how little ceremony asking for one takes
        let batch: Vec<_> = (0..8u64)
            .map(|index| {
                let priority = match index {
                    0 => 200,
                    _ => DEFAULT_PRIORITY,
                };

                Runtime::task(Sleep::sleep(Duration::from_micros(index * 200 + 50), true))
                    .priority(priority)
                    .spawn()
            })
            .collect();

        served += Runtime::join_all(batch)
            .into_iter()
            .filter(|result| result.is_ok())
            .count() as u64;

        // Pick up a config reload if the watcher has one ready.
        // Between runs it has nothing, which is not a problem
        // and not worth a branch of its own
        if let Ok(Ok(bytes)) = watcher.maybe_take() {
            assert!(!bytes.is_empty(), "the config came back empty");

            reloads += 1;
        }

        // Housekeeping, about once a second. Slower work, and
        // blocking rather than spawned, because there is nothing
        // else this thread wants to be doing while it happens
        if ticks % 40 == 0 {
            sweeps += 1;

            let meta = Runtime::block(File::metadata(&log)).expect("could not stat the log");

            assert!(meta.is_file(), "the log stopped being a file");

            let listing = Runtime::block(File::read_dir(&root)).expect("could not list the files");

            assert!(!listing.is_empty(), "the directory came back empty");

            // Written every sweep rather than appended, so it
            // is a snapshot rather than a history
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

        // A retry-shaped job, now and then. Bounded, so nothing
        // has to remember to stop it
        if ticks % 200 == 0 {
            let retries = Runtime::task(Sleep::sleep(Duration::from_millis(2), false))
                .repeat()
                .every(Duration::from_millis(20))
                .count(3)
                .spawn();

            // Left to run itself out rather than waited on. The
            // program has other things to do, and a bounded job
            // that nobody reads still ends
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

        // Wait out the rest of the tick, the way a loop with
        // nothing left to do this time round would
        if let Some(left) = TICK.checked_sub(tick_began.elapsed()) {
            Runtime::sleep(left);
        }
    }

    // ---- shutdown

    watcher.cancel();
    heartbeat.cancel();

    // Started before the loop and long since done, so this is
    // the ordinary case of joining something you set going and
    // then forgot about
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

    // The two schedules did their jobs for the whole run rather
    // than for the first second of it
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
