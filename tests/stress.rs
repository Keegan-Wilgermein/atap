//! One test, and its whole job is to be unfair
//!
//! `monolithic` puts the runtime through everything it can do,
//! but it does it a phase at a time — each section gets the
//! machine to itself, and the accounting either side of it is
//! only readable because nothing else is moving
//!
//! This is the other half of that. Every kind of work runs at
//! once, on threads that know nothing about each other: sleeps
//! and file reads land in the same queues, schedules keep
//! firing while the table is being churned, handles are cloned
//! across threads and cancelled out from under runs still going
//! behind them. Nothing here waits for anything else to finish
//!
//! So the assertions are deliberately few. Almost nothing about
//! a system under this much concurrent load is worth asserting
//! an exact number about — what matters is that it is still
//! standing at the end, still takes work, and gives its slots
//! back once the noise stops

use atap::{File, JoinPolicy, Runtime, Sleep, SleepTask};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

/// How long every crew keeps going for
const RUNNING: Duration = Duration::from_secs(20);

/// How long to give the wind down before giving up on it
const QUIET: Duration = Duration::from_secs(30);

/// Counters, so a crew that quietly stopped doing anything is
/// distinguishable from one that worked the whole time
///
/// Every one is only ever incremented, and nothing reads them
/// until every crew has been joined, so `Relaxed` is all any of
/// them needs
#[derive(Default)]
struct Tally {
    spawned: AtomicU64,
    joined: AtomicU64,
    taken: AtomicU64,
    cancelled: AtomicU64,
    blocked: AtomicU64,
    raced: AtomicU64,
    errors: AtomicU64,
}

impl Tally {
    fn bump(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn get(counter: &AtomicU64) -> u64 {
        counter.load(Ordering::Relaxed)
    }

    fn report(&self) {
        println!(
            "\n  spawned {}, joined {}, taken {}, cancelled {}, blocked {}, raced {}, errors {}",
            Self::get(&self.spawned),
            Self::get(&self.joined),
            Self::get(&self.taken),
            Self::get(&self.cancelled),
            Self::get(&self.blocked),
            Self::get(&self.raced),
            Self::get(&self.errors),
        );
    }
}

/// Somewhere to put the files, made rather than assumed
///
/// `tests/files` is ignored whole, and git doesn't track
/// directories, so an ignored one doesn't survive a clone
fn fixtures() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    root
}

/// Writes a file of `size` bytes and gives back its path
fn fixture(name: &str, size: usize) -> PathBuf {
    let path = fixtures().join(format!("stress-{}-{}.txt", name, std::process::id()));
    let body: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();

    fs::write(&path, body).expect("could not write the fixture");

    path
}

/// A sleep short enough to be worth spawning thousands of
fn quick(nanos: u64) -> SleepTask {
    Sleep::sleep(Duration::from_nanos(nanos), true)
}

#[test]
fn everything_at_once() {
    Runtime::init();

    let tally = Arc::new(Tally::default());
    let stop = Arc::new(AtomicBool::new(false));

    let small = Arc::new(fixture("small", 64));
    let large = Arc::new(fixture("large", 512 * 1024));
    let scratch = Arc::new(fixtures());

    let before = Runtime::workers();

    println!("starting with {} slots handed out", before.slots);

    let mut crews = Vec::new();

    // ---- the watcher, the only thing here that reads anything
    // back to the person running the test
    crews.push({
        let stop = Arc::clone(&stop);

        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(1));

                println!("{}", Runtime::workers());
            }
        })
    });

    // ---- sleeps, spawned in floods and joined in bulk
    //
    // The background hum. Nothing clever, just enough of it
    // that every other crew is always competing for a queue
    for crew in 0..3u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);

        crews.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let batch: Vec<_> = (0..64u64)
                    .map(|index| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(quick(index * 40 + 1))
                            .priority((crew * 80 + 40) as u8)
                            .spawn()
                    })
                    .collect();

                for result in Runtime::join_all(batch) {
                    match result {
                        Ok(_) => Tally::bump(&tally.joined),
                        Err(_) => Tally::bump(&tally.errors),
                    }
                }
            }
        }));
    }

    // ---- file reads, of a file that fits in one chunk and one
    // that very much doesn't
    //
    // These are the ones holding sleep threads, so they are what
    // makes the crews above wait for something other than each
    // other
    for crew in 0..3u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let small = Arc::clone(&small);
        let large = Arc::clone(&large);

        crews.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let path = match crew % 2 {
                    0 => small.as_path(),
                    _ => large.as_path(),
                };

                let handles: Vec<_> = (0..8)
                    .map(|_| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(File::read(path)).spawn()
                    })
                    .collect();

                for handle in handles {
                    // Cloned first, so the take below is racing
                    // a live second reference rather than being
                    // the only thing that ever looks at the slot
                    let copy = handle.clone();

                    match handle.join() {
                        Ok(Ok(_)) => Tally::bump(&tally.joined),
                        Ok(Err(_)) | Err(_) => Tally::bump(&tally.errors),
                    }

                    // Already taken by the join above as often
                    // as not, which is the point — losing this
                    // race is an ordinary outcome, not a fault,
                    // so it is counted rather than asserted on
                    if copy.maybe_take().is_ok() {
                        Tally::bump(&tally.taken);
                    }
                }
            }
        }));
    }

    // ---- writes, the only crew putting anything on the disk
    //
    // A file per thread, so two writes never race for the same
    // bytes. What they do race for is a place on the blocking
    // queue, against every read above
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let scratch = Arc::clone(&scratch);

        crews.push(thread::spawn(move || {
            let path = scratch.join(format!(
                "stress-write-{}-{}.txt",
                std::process::id(),
                crew,
            ));

            let body: Arc<[u8]> = Arc::from(vec![b'x'; 96 * 1024]);

            while !stop.load(Ordering::Relaxed) {
                Tally::bump(&tally.spawned);

                match Runtime::task(File::write(&path, Arc::clone(&body)))
                    .spawn()
                    .join()
                {
                    Ok(Ok(count)) => {
                        assert_eq!(count, body.len(), "a write came back short");

                        Tally::bump(&tally.joined);
                    }
                    Ok(Err(_)) | Err(_) => Tally::bump(&tally.errors),
                }

                Tally::bump(&tally.spawned);

                match Runtime::task(File::append(&path, Arc::clone(&body)))
                    .spawn()
                    .join()
                {
                    Ok(Ok(_)) => Tally::bump(&tally.joined),
                    Ok(Err(_)) | Err(_) => Tally::bump(&tally.errors),
                }
            }

            let _ = fs::remove_file(&path);
        }));
    }

    // ---- schedules and repeats, started and abandoned
    //
    // The crew that leaves the most behind. Every one of these
    // is still firing when the next is started, and cancelling
    // one only takes effect at its next tick — so the table is
    // always holding schedules part way through stopping
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let small = Arc::clone(&small);

        crews.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                Tally::bump(&tally.spawned);

                let repeat = Runtime::task(quick(1))
                    .repeat()
                    .every(Duration::from_millis(2))
                    .spawn();

                Tally::bump(&tally.spawned);

                let rate = Runtime::task(quick(500))
                    .at_rate(Duration::from_millis(3))
                    .spawn();

                Tally::bump(&tally.spawned);

                // Bounded, and left entirely alone. Nothing
                // reads it, so every run but the last has its
                // output dropped by the recycle the next does
                let bounded = Runtime::task(File::read(small.as_path()))
                    .repeat()
                    .every(Duration::from_millis(4))
                    .count(6)
                    .spawn();

                Tally::bump(&tally.spawned);

                // A delay that mostly outlives the cancel below,
                // so the manager keeps finding armed slots for
                // tasks nobody wants any more
                let delayed = Runtime::task(quick(1))
                    .after(Duration::from_millis(40))
                    .priority(200)
                    .spawn();

                thread::sleep(Duration::from_millis(10 + crew * 5));

                for _ in 0..3 {
                    if repeat.maybe_take().is_ok() {
                        Tally::bump(&tally.taken);
                    }
                }

                if rate.maybe_take().is_ok() {
                    Tally::bump(&tally.taken);
                }

                // Cancelled from a thread that isn't the one
                // running them, while runs are still in flight
                repeat.cancel();
                rate.cancel();
                delayed.cancel();

                Tally::bump(&tally.cancelled);
                Tally::bump(&tally.cancelled);
                Tally::bump(&tally.cancelled);

                // Dropped rather than cancelled or read, so the
                // only thing that ends it is running out
                drop(bounded);
            }
        }));
    }

    // ---- blocking calls, which never touch the `Executor` at
    // all and are here to hold real threads while it works
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let large = Arc::clone(&large);

        crews.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                Runtime::block(Sleep::sleep(Duration::from_millis(1), crew == 0));

                Tally::bump(&tally.blocked);

                match Runtime::block(File::read(large.as_path())) {
                    Ok(_) => Tally::bump(&tally.blocked),
                    Err(_) => Tally::bump(&tally.errors),
                }

                Runtime::sleep(Duration::from_micros(200));

                Tally::bump(&tally.blocked);
            }
        }));
    }

    // ---- races, which are the only crew that holds a claim on
    // a slot it isn't going to read
    //
    // Every set mixes lengths so the winner is never the same
    // one twice, and the policies rotate so all three ways out
    // of a race are exercised against everything else running
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let small = Arc::clone(&small);

        crews.push(thread::spawn(move || {
            let mut round = crew;

            while !stop.load(Ordering::Relaxed) {
                round = round.wrapping_add(1);

                let racers: Vec<_> = (0..6u64)
                    .map(|index| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(quick(index * 300 + 1)).spawn()
                    })
                    .collect();

                let policy = match round % 3 {
                    0 => JoinPolicy::Cancel,
                    1 => JoinPolicy::Drop,
                    _ => JoinPolicy::PassBack,
                };

                let (first, rest) = Runtime::join_first(racers, policy);

                Tally::bump(&tally.raced);

                assert!(first.settled(), "a race produced an unsettled winner");

                // Read as often as not, so the winner is
                // sometimes taken and sometimes abandoned
                if round % 2 == 0 && first.maybe_take().is_ok() {
                    Tally::bump(&tally.taken);
                }

                if let Some(losers) = rest {
                    assert_eq!(losers.len(), 5, "PassBack lost track of a loser");

                    // Handed back and then dropped unread,
                    // which is the case nothing else covers
                    drop(losers);
                }

                // A race across two kinds of work is the same
                // shape, and this one is against a file so the
                // set spans both pools at once
                let mixed: Vec<_> = (0..3)
                    .map(|_| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(File::read(small.as_path())).spawn()
                    })
                    .collect();

                let (won, _) = Runtime::join_first(mixed, JoinPolicy::Cancel);

                Tally::bump(&tally.raced);

                assert!(won.settled(), "a file race produced an unsettled winner");
            }
        }));
    }

    // ---- the churn, which exists to make the table grow and
    // shrink underneath everything above
    crews.push({
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);

        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let wave: Vec<_> = (0..2048)
                    .map(|_| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(quick(1)).spawn()
                    })
                    .collect();

                for handle in wave {
                    match handle.join() {
                        Ok(_) => Tally::bump(&tally.joined),
                        Err(_) => Tally::bump(&tally.errors),
                    }
                }

                // Asked for while the crews above are still
                // filling the table, which is the case it is
                // least likely to be able to do anything with.
                // Refusing is a fine answer and the only wrong
                // one is falling over
                let _ = Runtime::trim();
            }
        })
    });

    let started = Instant::now();

    thread::sleep(RUNNING);

    stop.store(true, Ordering::Relaxed);

    for crew in crews {
        crew.join().expect("a crew went down");
    }

    println!("\nran for {:?}, winding down", started.elapsed());

    tally.report();

    // Everything abandoned mid flight gets its chance to end.
    // A cancelled schedule stops at its next tick and a bounded
    // one when it runs out, so neither is over the moment the
    // thread that started it stopped asking
    let waited = Instant::now();
    let mut last = Runtime::workers().live;

    while waited.elapsed() < QUIET {
        thread::sleep(Duration::from_millis(100));

        let now = Runtime::workers();

        if now.live == last && !now.has_any_task() {
            break;
        }

        last = now.live;
    }

    let after = Runtime::workers();

    println!("\nsettled at:\n{}", after);

    // ---- the few things worth asserting after all that

    assert!(
        Runtime::healthy(),
        "the runtime did not survive: {:?}",
        Runtime::status(),
    );

    // Nothing above cancels anything it later reads, and no
    // spawn here can be refused, so every error is a real one
    assert_eq!(
        Tally::get(&tally.errors),
        0,
        "work came back as an error under load",
    );

    assert!(
        Tally::get(&tally.joined) > 0,
        "nothing was ever joined, so nothing was ever really running",
    );

    // Slots come back. Not to nothing — the pool keeps its
    // threads and the table keeps its pages — but to a number
    // that isn't still holding thousands of tasks nobody is
    // waiting for
    assert!(
        after.live < 256,
        "{} tasks still live long after everything stopped",
        after.live,
    );

    // Still takes work, which is the part a count of live slots
    // can't tell you
    match Runtime::task(File::read(small.as_path())).spawn().join() {
        Ok(Ok(bytes)) => println!("\nstill working: read {} bytes after the storm", bytes.len()),
        other => panic!(
            "the runtime stopped taking spawned work: {:?}",
            other.map(|read| read.map(|bytes| bytes.len())),
        ),
    }

    assert!(
        Runtime::block(File::read(large.as_path())).is_ok(),
        "blocking calls stopped working",
    );

    let _ = fs::remove_file(small.as_path());
    let _ = fs::remove_file(large.as_path());
}
