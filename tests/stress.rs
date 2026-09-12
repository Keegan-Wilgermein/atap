//! One test, and its whole job is to be unfair
//!
//! Every kind of work runs at once on threads that know nothing
//! about each other, while the manager is killed every few
//! seconds. Every output is checked against what its own task
//! was asked for, every join has a ceiling, and the only errors
//! allowed are cancels and lost races. Once it goes quiet, the
//! runtime has to go idle, give its slots back, still grow for
//! a burst, stop burning cpu, shut down, and start again
//!
//! ## Knobs
//! `ATAP_STRESS_SECS` runs it longer than the default twenty
//! seconds. `ATAP_STRESS_SEED` replays the choices of a run that
//! failed, and every run prints the seed it used

use atap::{
    File, JoinPolicy, Process, Runtime, RuntimeError, Sleep, SleepMode, SleepTask, TaskHandle,
};
use std::{
    collections::HashMap,
    fs,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

/// How long every crew keeps going for, unless
/// `ATAP_STRESS_SECS` says otherwise
const RUNNING: Duration = Duration::from_secs(20);

/// How long to give the wind down before giving up on it
const QUIET: Duration = Duration::from_secs(30);

/// The longest any single task here may take before it counts
/// as stranded rather than slow
const STALL: Duration = Duration::from_secs(30);

/// Gap between one burst of manager deaths and the next
///
/// Longer than the supervisor's restart window, so it never
/// gives up
const FAULT_GAP: Duration = Duration::from_secs(6);

/// Manager deaths per burst, well under the restart limit
const FAULTS_PER_BURST: u32 = 2;

/// Files with distinct contents, for the crews that check a
/// read came back from the file it was asked for
const IDENTITIES: usize = 64;

/// Counters, so a crew that quietly stopped doing anything is
/// distinguishable from one that worked the whole time
#[derive(Default)]
struct Tally {
    spawned: AtomicU64,
    joined: AtomicU64,
    taken: AtomicU64,
    cancelled: AtomicU64,
    blocked: AtomicU64,
    raced: AtomicU64,
    children: AtomicU64,
    faults: AtomicU64,

    /// Outputs checked against what their task was asked for
    verified: AtomicU64,

    /// Reads that lost fairly, to a cancel or to another reader
    refused: AtomicU64,

    /// Failures of any kind that nobody asked for
    errors: AtomicU64,

    /// Outputs that belonged to some other task
    crossed: AtomicU64,

    /// Sleeps that came back before they had slept
    early: AtomicU64,

    /// Tasks that were never going to finish
    stalled: AtomicU64,
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
            "\n  spawned {}, joined {}, taken {}, cancelled {}, blocked {}, raced {}, \
             children {}, manager deaths {}",
            Self::get(&self.spawned),
            Self::get(&self.joined),
            Self::get(&self.taken),
            Self::get(&self.cancelled),
            Self::get(&self.blocked),
            Self::get(&self.raced),
            Self::get(&self.children),
            Self::get(&self.faults),
        );

        println!(
            "  verified {}, refused {} | errors {}, crossed {}, early {}, stalled {}",
            Self::get(&self.verified),
            Self::get(&self.refused),
            Self::get(&self.errors),
            Self::get(&self.crossed),
            Self::get(&self.early),
            Self::get(&self.stalled),
        );
    }

    /// Books a read that should have produced exactly `wanted`
    fn check_bytes(&self, got: &[u8], wanted: &[u8], what: &str) {
        if got == wanted {
            Self::bump(&self.verified);
            return;
        }

        Self::bump(&self.crossed);

        eprintln!(
            "  CROSSED: {} came back with {} bytes starting {:?}, wanted {} starting {:?}",
            what,
            got.len(),
            String::from_utf8_lossy(&got[..got.len().min(24)]),
            wanted.len(),
            String::from_utf8_lossy(&wanted[..wanted.len().min(24)]),
        );
    }

    /// Books a sleep that was asked to take at least `asked`
    fn check_slept(&self, slept: Duration, asked: Duration, what: &str) {
        if slept >= asked {
            Self::bump(&self.verified);
            return;
        }

        Self::bump(&self.early);

        eprintln!("  EARLY: {} asked for {:?} and came back after {:?}", what, asked, slept);
    }

    /// Books a task that ran and reported a failure of its own
    ///
    /// Nothing here asks a task to fail, so any error inside an
    /// output is a fault
    fn failed(&self, error: RuntimeError, what: &str) {
        Self::bump(&self.errors);

        eprintln!("  ERROR: {} reported {:?}", what, error);
    }

    /// Books the error half of a read
    ///
    /// A cancel and a lost race are the only refusals allowed
    fn refusal(&self, error: RuntimeError, what: &str) {
        match error {
            RuntimeError::Cancelled | RuntimeError::AlreadyTaken => Self::bump(&self.refused),

            RuntimeError::NotReady => {
                Self::bump(&self.stalled);
                eprintln!("  STALLED: {} was still unsettled after {:?}", what, STALL);
            }

            other => {
                Self::bump(&self.errors);
                eprintln!("  ERROR: {} failed with {:?}", what, other);
            }
        }
    }
}

/// The largest numbers the watcher saw while the storm ran
#[derive(Default)]
struct Peaks {
    live: AtomicUsize,
    workers: AtomicUsize,
    sleep_threads: AtomicUsize,
    queued: AtomicUsize,
    blocking_queued: AtomicUsize,
}

impl Peaks {
    fn raise(peak: &AtomicUsize, seen: usize) {
        peak.fetch_max(seen, Ordering::Relaxed);
    }
}

/// A small, fast, reproducible source of choices
struct Dice(u64);

impl Dice {
    fn new(seed: u64, crew: u64) -> Self {
        Self((seed ^ crew.wrapping_mul(0x9E37_79B9_7F4A_7C15)) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;

        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;

        self.0 = x;

        x
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

/// Somewhere to put the files
fn fixtures() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    root
}

/// The bytes the fixture of `size` holds
fn pattern(size: usize) -> Vec<u8> {
    (0..size).map(|index| (index % 251) as u8).collect()
}

/// Writes a file of `size` bytes and gives back its path
fn fixture(name: &str, size: usize) -> PathBuf {
    let path = fixtures().join(format!("stress-{}-{}.txt", name, std::process::id()));

    fs::write(&path, pattern(size)).expect("could not write the fixture");

    path
}

/// A file whose contents and length say which one it is
fn identity(index: usize) -> (PathBuf, Vec<u8>) {
    let path = fixtures().join(format!("stress-identity-{}-{}.txt", std::process::id(), index));

    let stamp = format!("identity {} of process {} | ", index, std::process::id());
    let length = 64 + (index * 97) % 8000;
    let body: Vec<u8> = stamp.bytes().cycle().take(length).collect();

    fs::write(&path, &body).expect("could not write an identity fixture");

    (path, body)
}

/// A sleep short enough to be worth spawning thousands of
///
/// Spun rather than slept, so it never leaves a worker
fn quick(nanos: u64) -> SleepTask {
    Sleep::sleep(Duration::from_nanos(nanos))
}

/// A sleep that waits in the kernel on a sleep thread for
/// the whole of its duration
fn kernel_sleep(asked: Duration) -> SleepTask {
    Sleep::sleep(asked).mode(SleepMode::Relaxed)
}

/// Process time burnt so far, user and system together
fn cpu_time() -> Duration {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };

    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Duration::ZERO;
    }

    let seconds = |time: libc::timeval| {
        Duration::from_secs(time.tv_sec as u64) + Duration::from_micros(time.tv_usec as u64)
    };

    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}

/// Lifts the soft limit on open descriptors as far as the hard
/// limit allows, so running out isn't counted as a fault
///
/// ## Returns
/// The limit it ended up with, for the log
fn raise_descriptor_limit() -> u64 {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };

    // `OPEN_MAX`, which is as high as macOS lets a process ask for
    let wanted = limit.rlim_max.min(10_240);

    if wanted > limit.rlim_cur {
        let raised = libc::rlimit {
            rlim_cur: wanted,
            rlim_max: limit.rlim_max,
        };

        unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) };
    }

    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };

    limit.rlim_cur
}

/// Online cores, which the pool sizes itself against
fn cores() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

/// Waits for the pool to have nothing left to do and the live
/// count to stop moving
fn settle() -> Duration {
    let waited = Instant::now();
    let mut last = Runtime::workers().live();

    while waited.elapsed() < QUIET {
        thread::sleep(Duration::from_millis(100));

        let now = Runtime::workers();

        if now.live() == last && !now.has_any_task() {
            break;
        }

        last = now.live();
    }

    waited.elapsed()
}

/// Every kind of work at once, then a quiet runtime afterwards
#[test]
fn everything_at_once() {
    Runtime::init();

    let descriptors = raise_descriptor_limit();

    let running = std::env::var("ATAP_STRESS_SECS")
        .ok()
        .and_then(|secs| secs.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(RUNNING);

    let seed = std::env::var("ATAP_STRESS_SEED")
        .ok()
        .and_then(|seed| seed.parse::<u64>().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|since| since.as_nanos() as u64)
                .unwrap_or(0x5EED)
        });

    println!(
        "stress for {:?} on {} cores, seed {} (ATAP_STRESS_SEED={} replays it), {} descriptors",
        running,
        cores(),
        seed,
        seed,
        descriptors,
    );

    let tally = Arc::new(Tally::default());
    let peaks = Arc::new(Peaks::default());
    let stop = Arc::new(AtomicBool::new(false));

    let small = Arc::new(fixture("small", 64));
    let large = Arc::new(fixture("large", 512 * 1024));
    let small_body = Arc::new(pattern(64));
    let large_body = Arc::new(pattern(512 * 1024));
    let scratch = Arc::new(fixtures());

    let identities: Arc<Vec<(PathBuf, Vec<u8>)>> =
        Arc::new((0..IDENTITIES).map(identity).collect());

    let before = Runtime::workers();

    println!("starting with {} slots handed out", before.peak_slots());

    let mut crews = Vec::new();

    // ---- the watcher, the only thing here that prints as it goes
    let watcher = {
        let stop = Arc::clone(&stop);
        let peaks = Arc::clone(&peaks);

        thread::spawn(move || {
            let mut printed = Instant::now();

            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(50));

                let stats = Runtime::workers();

                Peaks::raise(&peaks.live, stats.live());
                Peaks::raise(&peaks.workers, stats.len());
                Peaks::raise(&peaks.sleep_threads, stats.sleep_threads());
                Peaks::raise(&peaks.queued, stats.queued());
                Peaks::raise(&peaks.blocking_queued, stats.blocking_queued());

                if printed.elapsed() >= Duration::from_secs(1) {
                    println!("{}", stats);
                    printed = Instant::now();
                }
            }
        })
    };

    // ---- sleeps, spawned in floods and joined in bulk
    //
    // One crew per priority band, so every band is carrying work
    // at once
    for crew in 0..4u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);

        crews.push(thread::spawn(move || {
            let priority = (crew * 85).min(255) as u8;

            while !stop.load(Ordering::Relaxed) {
                let asked: Vec<u64> = (0..64u64).map(|index| index * 40 + 1).collect();

                let batch: Vec<_> = asked
                    .iter()
                    .map(|nanos| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(quick(*nanos)).priority(priority).spawn()
                    })
                    .collect();

                for (nanos, result) in asked.iter().zip(Runtime::join_all(batch)) {
                    match result {
                        Ok(slept) => {
                            Tally::bump(&tally.joined);
                            tally.check_slept(slept, Duration::from_nanos(*nanos), "a flood sleep");
                        }
                        Err(error) => tally.refusal(error, "a flood sleep"),
                    }
                }
            }
        }));
    }

    // ---- file reads, of a file that fits in one chunk and one
    // that very much doesn't
    for crew in 0..3u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let small = Arc::clone(&small);
        let large = Arc::clone(&large);
        let small_body = Arc::clone(&small_body);
        let large_body = Arc::clone(&large_body);

        crews.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (path, wanted) = match crew % 2 {
                    0 => (small.as_path(), small_body.as_slice()),
                    _ => (large.as_path(), large_body.as_slice()),
                };

                let handles: Vec<_> = (0..8)
                    .map(|_| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(File::read(path)).spawn()
                    })
                    .collect();

                for handle in handles {
                    // Cloned first, so the take below races a live second reference
                    let copy = handle.clone();

                    match handle.join_with_timeout(STALL) {
                        Ok(Ok(bytes)) => {
                            Tally::bump(&tally.joined);
                            tally.check_bytes(&bytes, wanted, "a fixture read");
                        }
                        Ok(Err(error)) => tally.failed(error, "a fixture read"),
                        Err(error) => tally.refusal(error, "a fixture read"),
                    }

                    // The join above only cloned, so this gets the same value
                    match copy.maybe_take() {
                        Ok(Ok(bytes)) => {
                            Tally::bump(&tally.taken);
                            tally.check_bytes(&bytes, wanted, "a fixture read taken after a join");
                        }
                        Ok(Err(error)) => tally.failed(error, "a fixture take"),
                        Err(error) => tally.refusal(error, "a fixture take"),
                    }
                }
            }
        }));
    }

    // ---- writes, a file per thread, read back now and then
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

            let body: Arc<[u8]> = Arc::from(vec![b'a' + crew as u8; 96 * 1024]);
            let doubled: Vec<u8> = body.iter().chain(body.iter()).copied().collect();

            let mut round = 0u64;

            while !stop.load(Ordering::Relaxed) {
                round += 1;

                Tally::bump(&tally.spawned);

                match Runtime::task(File::write(&path, Arc::clone(&body)))
                    .spawn()
                    .join_with_timeout(STALL)
                {
                    Ok(Ok(count)) if count == body.len() => Tally::bump(&tally.joined),
                    Ok(Ok(count)) => {
                        Tally::bump(&tally.errors);
                        eprintln!("  ERROR: a write reported {} of {} bytes", count, body.len());
                    }
                    Ok(Err(error)) => tally.failed(error, "a write"),
                    Err(error) => tally.refusal(error, "a write"),
                }

                Tally::bump(&tally.spawned);

                match Runtime::task(File::append(&path, Arc::clone(&body)))
                    .spawn()
                    .join_with_timeout(STALL)
                {
                    Ok(Ok(_)) => Tally::bump(&tally.joined),
                    Ok(Err(error)) => tally.failed(error, "an append"),
                    Err(error) => tally.refusal(error, "an append"),
                }

                if round % 8 == 0 {
                    Tally::bump(&tally.spawned);

                    match Runtime::task(File::read(&path)).spawn().join_with_timeout(STALL) {
                        Ok(Ok(bytes)) => tally.check_bytes(&bytes, &doubled, "a write read back"),
                        Ok(Err(error)) => tally.failed(error, "a read back"),
                        Err(error) => tally.refusal(error, "a read back"),
                    }
                }
            }

            let _ = fs::remove_file(&path);
        }));
    }

    // ---- schedules and repeats, started and abandoned
    //
    // Every one is still firing when the next is started, so the
    // table always holds schedules part way through stopping
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

                // Bounded, and never read
                let bounded = Runtime::task(File::read(small.as_path()))
                    .repeat()
                    .every(Duration::from_millis(4))
                    .count(6)
                    .spawn();

                Tally::bump(&tally.spawned);

                // A delay that mostly outlives the cancel below
                let delayed = Runtime::task(quick(1))
                    .after(Duration::from_millis(40))
                    .priority(200)
                    .spawn();

                thread::sleep(Duration::from_millis(10 + crew * 5));

                for _ in 0..3 {
                    if let Ok(slept) = repeat.maybe_take() {
                        Tally::bump(&tally.taken);
                        tally.check_slept(slept, Duration::from_nanos(1), "a repeat run");
                    }
                }

                if let Ok(slept) = rate.maybe_take() {
                    Tally::bump(&tally.taken);
                    tally.check_slept(slept, Duration::from_nanos(500), "a scheduled run");
                }

                // Cancelled while runs are still in flight
                repeat.cancel();
                rate.cancel();
                delayed.cancel();

                Tally::bump(&tally.cancelled);
                Tally::bump(&tally.cancelled);
                Tally::bump(&tally.cancelled);

                // Dropped, so only running out ends it
                drop(bounded);
            }
        }));
    }

    // ---- kernel sleeps, cancelled as they finish
    //
    // Bursts go past what the sleep pool holds at once, and a
    // third of each is cancelled at about the moment it would
    // have finished. Every sleep that comes back has to have
    // slept at least as long as it asked, cancelled or not
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);

        crews.push(thread::spawn(move || {
            let mut dice = Dice::new(seed, 100 + crew);
            let burst = cores() * 6;

            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();

                let mut batch: Vec<(Duration, bool, TaskHandle<Duration>)> = (0..burst)
                    .map(|_| {
                        let asked = Duration::from_millis(1 + dice.below(30));
                        let doomed = dice.below(3) == 0;

                        Tally::bump(&tally.spawned);

                        (asked, doomed, Runtime::task(kernel_sleep(asked)).spawn())
                    })
                    .collect();

                // Soonest first, so the cancels can walk forward in time
                batch.sort_by_key(|(asked, _, _)| *asked);

                for (asked, doomed, handle) in batch.iter() {
                    if !doomed {
                        continue;
                    }

                    // Within a millisecond either side of when it would have
                    // finished if it started straight away
                    let jitter = Duration::from_micros(dice.below(2_000));
                    let target = started + asked.saturating_sub(Duration::from_millis(1)) + jitter;

                    if let Some(gap) = target.checked_duration_since(Instant::now()) {
                        thread::sleep(gap);
                    }

                    handle.clone().cancel();
                    Tally::bump(&tally.cancelled);
                }

                for (asked, _, handle) in batch {
                    match handle.join_with_timeout(STALL) {
                        Ok(slept) => {
                            Tally::bump(&tally.joined);
                            tally.check_slept(slept, asked, "a kernel sleep");
                        }
                        Err(error) => tally.refusal(error, "a kernel sleep"),
                    }
                }
            }
        }));
    }

    // ---- child processes, checked and cancelled
    //
    // Every child's output is checked for the token it was given,
    // and each round cancels one child as it exits and one long
    // before it would
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);

        crews.push(thread::spawn(move || {
            let mut dice = Dice::new(seed, 200 + crew);
            let mut round = 0u64;

            while !stop.load(Ordering::Relaxed) {
                round += 1;

                let tokens: Vec<String> = (0..4)
                    .map(|slot| format!("{}-{}-{}-{}", std::process::id(), crew, round, slot))
                    .collect();

                let echoes: Vec<_> = tokens
                    .iter()
                    .map(|token| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(Process::output("/bin/echo", [token.as_str()])).spawn()
                    })
                    .collect();

                Tally::bump(&tally.spawned);

                let fed = Runtime::task(
                    Process::output("/bin/cat", Process::NO_ARGS)
                        .input(tokens[0].clone().into_bytes()),
                )
                .spawn();

                // One cancelled as it exits, and one while it has seconds left
                Tally::bump(&tally.spawned);
                let racing = Runtime::task(Process::run("/bin/sleep", ["0.02"])).spawn();

                Tally::bump(&tally.spawned);
                let doomed = Runtime::task(Process::run("/bin/sleep", ["5"])).spawn();

                thread::sleep(Duration::from_millis(15 + dice.below(10)));

                racing.clone().cancel();
                doomed.clone().cancel();

                Tally::bump(&tally.cancelled);
                Tally::bump(&tally.cancelled);

                for (token, handle) in tokens.iter().zip(echoes) {
                    Tally::bump(&tally.children);

                    match handle.join_with_timeout(STALL) {
                        Ok(Ok(out)) if out.status().success() => {
                            Tally::bump(&tally.joined);
                            tally.check_bytes(out.stdout(), format!("{}\n", token).as_bytes(), "an echo");
                        }
                        Ok(Ok(out)) => {
                            Tally::bump(&tally.errors);
                            eprintln!("  ERROR: echo exited {:?}", out.status());
                        }
                        Ok(Err(error)) => tally.failed(error, "an echo"),
                        Err(error) => tally.refusal(error, "an echo"),
                    }
                }

                Tally::bump(&tally.children);

                match fed.join_with_timeout(STALL) {
                    Ok(Ok(out)) => {
                        Tally::bump(&tally.joined);
                        tally.check_bytes(out.stdout(), tokens[0].as_bytes(), "a child fed its input");
                    }
                    Ok(Err(error)) => tally.failed(error, "a fed child"),
                    Err(error) => tally.refusal(error, "a fed child"),
                }

                // Finished before the cancel landed, or cancelled
                Tally::bump(&tally.children);

                match racing.join_with_timeout(STALL) {
                    Ok(Ok(_)) => Tally::bump(&tally.joined),
                    Ok(Err(error)) => tally.failed(error, "a racing child"),
                    Err(error) => tally.refusal(error, "a racing child"),
                }

                // Anything but a cancel is a cancel that didn't reach it
                Tally::bump(&tally.children);

                match doomed.join_with_timeout(STALL) {
                    Err(RuntimeError::Cancelled) => Tally::bump(&tally.refused),
                    Ok(_) => {
                        Tally::bump(&tally.errors);
                        eprintln!("  ERROR: a cancelled five second child ran to the end");
                    }
                    Err(error) => tally.refusal(error, "a cancelled child"),
                }
            }
        }));
    }

    // ---- one read, four threads fighting over it
    //
    // Each handle is cloned three ways and sent to threads that
    // join, take, and cancel or drop it. The read is of one of
    // sixty four different files, so a value reaching the wrong
    // reader comes back as the wrong file
    {
        let (to_joiner, joiner_rx) = mpsc::sync_channel::<(usize, TaskHandle<Result<Vec<u8>, RuntimeError>>)>(64);
        let (to_taker, taker_rx) = mpsc::sync_channel::<(usize, TaskHandle<Result<Vec<u8>, RuntimeError>>)>(64);
        let (to_third, third_rx) = mpsc::sync_channel::<(usize, TaskHandle<Result<Vec<u8>, RuntimeError>>)>(64);

        crews.push({
            let stop = Arc::clone(&stop);
            let tally = Arc::clone(&tally);
            let identities = Arc::clone(&identities);

            thread::spawn(move || {
                let mut dice = Dice::new(seed, 300);

                while !stop.load(Ordering::Relaxed) {
                    let pick = dice.below(IDENTITIES as u64) as usize;

                    Tally::bump(&tally.spawned);

                    let handle = Runtime::task(File::read(&identities[pick].0)).spawn();

                    // A closed channel is a fighter that went down
                    if to_joiner.send((pick, handle.clone())).is_err()
                        || to_taker.send((pick, handle.clone())).is_err()
                        || to_third.send((pick, handle)).is_err()
                    {
                        break;
                    }
                }

                // Dropping the senders lets the fighters run out of work
            })
        });

        crews.push({
            let tally = Arc::clone(&tally);
            let identities = Arc::clone(&identities);

            thread::spawn(move || {
                for (pick, handle) in joiner_rx {
                    match handle.join_with_timeout(STALL) {
                        Ok(Ok(bytes)) => {
                            Tally::bump(&tally.joined);
                            tally.check_bytes(&bytes, &identities[pick].1, "a fought over join");
                        }
                        Ok(Err(error)) => tally.failed(error, "a fought over join"),
                        Err(error) => tally.refusal(error, "a fought over join"),
                    }
                }
            })
        });

        crews.push({
            let tally = Arc::clone(&tally);
            let identities = Arc::clone(&identities);

            thread::spawn(move || {
                for (pick, handle) in taker_rx {
                    // A take that loses to a cancel or the join is a refusal,
                    // and one that wins has to carry the right file
                    match handle.take_with_timeout(STALL) {
                        Ok(Ok(bytes)) => {
                            Tally::bump(&tally.taken);
                            tally.check_bytes(&bytes, &identities[pick].1, "a fought over take");
                        }
                        Ok(Err(error)) => tally.failed(error, "a fought over take"),
                        Err(error) => tally.refusal(error, "a fought over take"),
                    }
                }
            })
        });

        crews.push({
            let tally = Arc::clone(&tally);

            thread::spawn(move || {
                let mut dice = Dice::new(seed, 301);

                for (_, handle) in third_rx {
                    if dice.below(4) == 0 {
                        if dice.below(2) == 0 {
                            thread::yield_now();
                        }

                        handle.cancel();
                        Tally::bump(&tally.cancelled);
                    } else {
                        drop(handle);
                    }
                }
            })
        });
    }

    // ---- blocking calls, holding real threads while the pool works
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let large = Arc::clone(&large);
        let large_body = Arc::clone(&large_body);

        crews.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let asked = Duration::from_millis(1);

                // One crew each way
                let mode = match crew == 0 {
                    true => SleepMode::Precise,
                    false => SleepMode::Relaxed,
                };

                let slept = Runtime::block(Sleep::sleep(asked).mode(mode));

                Tally::bump(&tally.blocked);
                tally.check_slept(slept, asked, "a blocking sleep");

                match Runtime::block(File::read(large.as_path())) {
                    Ok(bytes) => {
                        Tally::bump(&tally.blocked);
                        tally.check_bytes(&bytes, &large_body, "a blocking read");
                    }
                    Err(error) => tally.refusal(error, "a blocking read"),
                }

                let asked = Duration::from_micros(200);
                let slept = Runtime::sleep(asked);

                Tally::bump(&tally.blocked);
                tally.check_slept(slept, asked, "a blocking precise sleep");
            }
        }));
    }

    // ---- races, including two threads racing the same set
    //
    // Lengths are mixed and the policies rotate, and a second
    // thread races clones of the same handles at the same moment
    for crew in 0..2u64 {
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);
        let small = Arc::clone(&small);
        let small_body = Arc::clone(&small_body);

        crews.push(thread::spawn(move || {
            let mut round = crew;

            while !stop.load(Ordering::Relaxed) {
                round = round.wrapping_add(1);

                let mut asked = HashMap::new();

                let racers: Vec<_> = (0..6u64)
                    .map(|index| {
                        Tally::bump(&tally.spawned);

                        let nanos = index * 300 + 1;
                        let handle = Runtime::task(quick(nanos)).spawn();

                        asked.insert(handle.id(), nanos);

                        handle
                    })
                    .collect();

                let shadow: Vec<_> = racers.iter().cloned().collect();

                let policy = match round % 3 {
                    0 => JoinPolicy::Cancel,
                    1 => JoinPolicy::Drop,
                    _ => JoinPolicy::PassBack,
                };

                let shadow_settled = thread::scope(|scope| {
                    let shadow_race = scope.spawn(move || {
                        let (first, _) = Runtime::join_first(shadow, JoinPolicy::Drop);

                        first.settled()
                    });

                    let (first, rest) = Runtime::join_first(racers, policy);

                    Tally::bump(&tally.raced);

                    assert!(first.settled(), "a race produced an unsettled winner");

                    // Read as often as not, and when it is read it has to be the
                    // sleep that handle was spawned for
                    if round % 2 == 0 {
                        if let Ok(slept) = first.maybe_take() {
                            Tally::bump(&tally.taken);

                            let wanted = asked.get(&first.id()).copied().unwrap_or(u64::MAX);
                            tally.check_slept(slept, Duration::from_nanos(wanted), "a race winner");
                        }
                    }

                    if let Some(losers) = rest {
                        assert_eq!(losers.len(), 5, "PassBack lost track of a loser");

                        // Handed back and then dropped unread
                        drop(losers);
                    }

                    shadow_race.join().expect("the shadow race went down")
                });

                Tally::bump(&tally.raced);

                assert!(shadow_settled, "a race over shared handles produced an unsettled winner");

                // A race against a file, so the set spans both pools
                let mixed: Vec<_> = (0..3)
                    .map(|_| {
                        Tally::bump(&tally.spawned);

                        Runtime::task(File::read(small.as_path())).spawn()
                    })
                    .collect();

                let (won, _) = Runtime::join_first(mixed, JoinPolicy::Cancel);

                Tally::bump(&tally.raced);

                assert!(won.settled(), "a file race produced an unsettled winner");

                if let Ok(Ok(bytes)) = won.maybe_take() {
                    tally.check_bytes(&bytes, &small_body, "a file race winner");
                }
            }
        }));
    }

    // ---- the churn, making the table grow and shrink underneath
    // everything above
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
                        Err(error) => tally.refusal(error, "a churned task"),
                    }
                }

                // Refusing is a fine answer
                let _ = Runtime::trim();
            }
        })
    });

    // ---- the manager, killed out from under all of it
    crews.push({
        let stop = Arc::clone(&stop);
        let tally = Arc::clone(&tally);

        thread::spawn(move || {
            // Everything else under way before the first death
            let mut next = Instant::now() + Duration::from_secs(2);

            while !stop.load(Ordering::Relaxed) {
                if Instant::now() >= next {
                    Runtime::inject_manager_faults(FAULTS_PER_BURST);

                    for _ in 0..FAULTS_PER_BURST {
                        Tally::bump(&tally.faults);
                    }

                    next = Instant::now() + FAULT_GAP;
                }

                thread::sleep(Duration::from_millis(50));
            }
        })
    });

    let started = Instant::now();

    thread::sleep(running);

    stop.store(true, Ordering::Relaxed);

    for crew in crews {
        crew.join().expect("a crew went down");
    }

    watcher.join().expect("the watcher went down");

    println!("\nran for {:?}, winding down", started.elapsed());

    tally.report();

    println!(
        "  peaks: {} live, {} workers, {} sleep threads, {} queued, {} blocking queued",
        peaks.live.load(Ordering::Relaxed),
        peaks.workers.load(Ordering::Relaxed),
        peaks.sleep_threads.load(Ordering::Relaxed),
        peaks.queued.load(Ordering::Relaxed),
        peaks.blocking_queued.load(Ordering::Relaxed),
    );

    // Everything abandoned mid flight gets its chance to end
    let took = settle();
    let after = Runtime::workers();

    println!("\nsettled in {:?} at:\n{}", took, after);

    // ---- what has to have held all the way through

    assert!(
        Runtime::healthy(),
        "the runtime did not survive: {:?}",
        Runtime::status(),
    );

    assert_eq!(
        Tally::get(&tally.crossed),
        0,
        "outputs reached tasks they didn't belong to",
    );

    assert_eq!(
        Tally::get(&tally.early),
        0,
        "sleeps came back before they had slept",
    );

    assert_eq!(
        Tally::get(&tally.stalled),
        0,
        "tasks were stranded with nothing ever going to run them",
    );

    // Cancels are counted as refusals, so every error is a real one
    assert_eq!(
        Tally::get(&tally.errors),
        0,
        "work came back as an error under load",
    );

    // Each part of the storm actually happened
    assert!(Tally::get(&tally.verified) > 0, "no output was ever checked");
    assert!(Tally::get(&tally.refused) > 0, "no cancel or race ever refused a read");
    assert!(Tally::get(&tally.children) > 0, "no child was ever run");
    assert!(Tally::get(&tally.raced) > 0, "no race was ever run");

    assert!(
        Tally::get(&tally.faults) > 0 || running < Duration::from_secs(3),
        "the manager was never killed",
    );

    // ---- what is only true once it has gone quiet

    // Idle means idle
    assert!(
        !after.has_any_task() && after.queued() == 0 && after.blocking_queued() == 0,
        "the pool never went quiet after the storm: {} queued, {} blocking, {} backlog, \
         {} workers busy, {} sleep threads busy",
        after.queued(),
        after.blocking_queued(),
        after.backlog(),
        after.busy(),
        after.sleep_busy(),
    );

    // Slots come back, to a number that isn't still holding
    // thousands of abandoned tasks
    assert!(
        after.live() < 256,
        "{} tasks still live long after everything stopped",
        after.live(),
    );

    // Every sleep thread the storm left behind, in use at once,
    // and still overlapping
    let burst = cores() * 8;
    let asked = Duration::from_millis(50);
    let burst_started = Instant::now();

    let sleeps: Vec<_> = (0..burst)
        .map(|_| Runtime::task(kernel_sleep(asked)).spawn())
        .collect();

    let mut slept_total = Duration::ZERO;

    for handle in sleeps {
        match handle.join_with_timeout(STALL) {
            Ok(slept) => {
                tally.check_slept(slept, asked, "a sleep after the storm");
                slept_total += slept;
            }
            Err(error) => tally.refusal(error, "a sleep after the storm"),
        }
    }

    let burst_took = burst_started.elapsed();

    println!(
        "\n{} sleeps of {:?} after the storm took {:?}, {:?} slept between them",
        burst, asked, burst_took, slept_total,
    );

    assert_eq!(
        Tally::get(&tally.early) + Tally::get(&tally.stalled) + Tally::get(&tally.errors),
        0,
        "the pool left over from the storm cut sleeps short, stranded them or failed them",
    );

    assert!(
        slept_total > burst_took * 2,
        "{} sleeps took {:?} of wall clock but only {:?} between them, so the blocking \
         pool no longer grows for a burst",
        burst,
        burst_took,
        slept_total,
    );

    // A child after the storm, on the threads the cancelled
    // children ran on
    let token = format!("after-the-storm-{}", std::process::id());

    match Runtime::task(Process::output("/bin/echo", [token.as_str()]))
        .spawn()
        .join_with_timeout(STALL)
    {
        Ok(Ok(out)) => assert_eq!(
            out.stdout(),
            format!("{}\n", token).as_bytes(),
            "a child after the storm came back with somebody else's output",
        ),
        other => panic!("a child after the storm didn't run: {:?}", other.map(|out| out.map(|out| out.status()))),
    }

    // Quiet means the cpu is quiet too
    settle();
    thread::sleep(Duration::from_secs(1));

    let cpu_before = cpu_time();
    let window_started = Instant::now();

    thread::sleep(Duration::from_secs(2));

    let burnt = cpu_time() - cpu_before;
    let window = window_started.elapsed();
    let share = burnt.as_secs_f64() / window.as_secs_f64() * 100.0;

    println!("idle after the storm: {:?} of cpu over {:?}, {:.2}% of one core", burnt, window, share);

    assert!(
        share < 10.0,
        "an idle pool burnt {:?} of cpu over {:?}, {:.2}% of a core, so something after the \
         storm is awake that should have been parked",
        burnt,
        window,
        share,
    );

    // Still takes work
    match Runtime::task(File::read(small.as_path())).spawn().join() {
        Ok(Ok(bytes)) => {
            assert_eq!(bytes.as_slice(), small_body.as_slice(), "a read after the storm was wrong");
            println!("still working: read {} bytes after the storm", bytes.len());
        }
        other => panic!(
            "the runtime stopped taking spawned work: {:?}",
            other.map(|read| read.map(|bytes| bytes.len())),
        ),
    }

    assert!(
        Runtime::block(File::read(large.as_path())).is_ok(),
        "blocking calls stopped working",
    );

    // ---- and it shuts down
    //
    // On its own thread with a ceiling, so a drain that never
    // ends fails rather than hangs
    let (done, finished) = mpsc::channel();

    thread::spawn(move || {
        Runtime::shutdown();
        let _ = done.send(());
    });

    assert!(
        finished.recv_timeout(STALL).is_ok(),
        "shutting down after the storm never finished draining",
    );

    assert_eq!(
        Runtime::task(quick(1)).spawn().join(),
        Err(RuntimeError::TaskFailed),
        "a spawn after shutting down should be refused rather than run or left waiting",
    );

    assert!(
        Runtime::block(File::read(small.as_path())).is_ok(),
        "blocking calls stopped working after the shutdown, which they are promised not to",
    );

    println!("shut down cleanly after the storm");

    // ---- and starts again
    assert_eq!(Runtime::init(), None, "the runtime wouldn't start again after the storm");

    match Runtime::task(File::read(small.as_path())).spawn().join() {
        Ok(Ok(bytes)) => {
            assert_eq!(bytes.as_slice(), small_body.as_slice(), "a read after starting again was wrong");
        }
        other => panic!(
            "the runtime took no spawned work after starting again: {:?}",
            other.map(|read| read.map(|bytes| bytes.len())),
        ),
    }

    println!("started again after the storm");

    let _ = fs::remove_file(small.as_path());
    let _ = fs::remove_file(large.as_path());

    for (path, _) in identities.iter() {
        let _ = fs::remove_file(path);
    }
}
