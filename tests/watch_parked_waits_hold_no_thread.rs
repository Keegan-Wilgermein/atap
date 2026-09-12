//! Its own binary, since it asserts on the whole pool and any
//! other test running beside it would show up in the counts

mod common;

use atap::{File, Runtime};
use common::{cores, report};
use std::{
    fs,
    path::PathBuf,
    process,
    thread,
    time::{Duration, Instant},
};

/// Paths watched at once, each with a watch parked on it
const WAITING: usize = 300;

/// How long the test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(20);

/// Lets the process hold a descriptor per watch, since the
/// default soft limit is 256
fn raise_descriptor_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };

    let wanted = (WAITING as libc::rlim_t) * 2 + 256;
    limit.rlim_cur = limit.rlim_cur.max(wanted.min(limit.rlim_max));

    unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
}

/// A directory of watchable files that cleans itself up
struct TestDir(PathBuf);

impl TestDir {
    /// Makes the directory and fills it with `WAITING` files
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/files")
            .join(format!("parked-{}", process::id()));

        fs::create_dir_all(&root).expect("could not make the test directory");

        for index in 0..WAITING {
            fs::write(root.join(format!("{}.txt", index)), b"before")
                .expect("could not make a watchable file");
        }

        Self(root)
    }

    /// The path of one of the files in it
    fn file(&self, index: usize) -> PathBuf {
        self.0.join(format!("{}.txt", index))
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Hundreds of watches waiting on files nothing is touching hold
/// no worker and no sleep thread, and every one of them still
/// finishes once its own file is written to
#[test]
fn parked_watches_hold_no_thread() {
    raise_descriptor_limit();
    Runtime::init();

    let dir = TestDir::new();

    let handles: Vec<_> = (0..WAITING)
        .map(|index| Runtime::task(File::watch(dir.file(index))).spawn())
        .collect();

    // Past every one reaching its park, and past enough manager
    // ticks for the pool to have grown if it was going to
    thread::sleep(Duration::from_millis(300));

    let stats = Runtime::workers();
    report("watches parked");

    let parked = handles.iter().filter(|handle| handle.is_running()).count();

    println!(
        "{} watches waiting: {} parked, {} workers busy, {} sleep threads ({} busy), cap {}",
        WAITING,
        parked,
        stats.busy(),
        stats.sleep_threads(),
        stats.sleep_busy(),
        cores() * 8,
    );

    assert_eq!(parked, WAITING, "every watch is waiting on its file");

    // The point of the whole family: a watch is nearly all waiting,
    // so it must not sit on a sleep thread the way every other file
    // task does
    assert_eq!(
        stats.sleep_threads(),
        0,
        "parked watches started {} sleep threads",
        stats.sleep_threads(),
    );
    assert_eq!(
        stats.sleep_busy(),
        0,
        "parked watches held {} sleep threads",
        stats.sleep_busy(),
    );

    // Not zero busy workers, which is what a parked socket would
    // give. A watch wakes on its own backstop every so often to look
    // at the path again, so with hundreds of them a few are always
    // part way through a step. What matters is that the pool serves
    // all of them from its usual handful of workers rather than
    // growing a thread per watch
    assert!(
        stats.len() < WAITING / 4,
        "{} watches grew the pool to {} workers",
        WAITING,
        stats.len(),
    );
    assert!(
        stats.busy() < WAITING / 10,
        "{} of {} watches were holding a worker at once",
        stats.busy(),
        WAITING,
    );

    for index in 0..WAITING {
        fs::write(dir.file(index), b"before and after").expect("could not write a watched file");
    }

    let deadline = Instant::now() + PATIENCE;

    for (index, handle) in handles.into_iter().enumerate() {
        let left = deadline.saturating_duration_since(Instant::now());

        let change = handle
            .take_with_timeout(left)
            .unwrap_or_else(|_| panic!("watch {} never woke", index))
            .unwrap_or_else(|_| panic!("watch {} failed", index));

        assert!(change.written(), "watch {} saw its write", index);
    }
}
