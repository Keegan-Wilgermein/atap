//! `join_first`, the race, and what becomes of the losers

use atap::{File, JoinPolicy, Runtime, RuntimeError, Sleep, SleepMode, TaskHandle, TaskState};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::Duration,
};

/// Keeps test file names apart
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A file with a known length, cleaned up on drop
struct TestFile(PathBuf);

impl TestFile {
    fn new(tag: &str, body: &[u8]) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

        fs::create_dir_all(&root).expect("could not make tests/files");

        let path = root.join(format!(
            "join-{}-{}-{}.txt",
            tag,
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));

        fs::write(&path, body).expect("could not write the fixture");

        Self(path)
    }

    fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TestFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// A sleep of a given length, spawned
fn sleeping(millis: u64) -> TaskHandle<Duration> {
    Runtime::task(Sleep::sleep(Duration::from_millis(millis)).mode(SleepMode::Relaxed)).spawn()
}

/// `Cancel` cancels every task that didn't win
#[test]
fn cancel_stops_the_losers() {
    Runtime::init();

    let quick = sleeping(5);

    // Cloned before the race, so they can still be looked at
    let slow: Vec<_> = (0..3).map(|_| sleeping(4000)).collect();
    let watching: Vec<_> = slow.iter().cloned().collect();

    let (first, rest) = Runtime::join_first(
        std::iter::once(quick).chain(slow),
        JoinPolicy::Cancel,
    );

    assert!(rest.is_none(), "Cancel should not hand the losers back");
    assert!(first.settled(), "the winner should be settled");

    for handle in &watching {
        let state = handle.wait().expect("a cancelled task still settles");

        assert_eq!(state, TaskState::Cancelled, "a loser was not cancelled");
        assert!(handle.is_cancelled(), "is_cancelled disagrees with the state");
    }
}

/// `Drop` leaves every task that didn't win running
#[test]
fn drop_leaves_the_losers_running() {
    Runtime::init();

    let quick = sleeping(5);
    let slow: Vec<_> = (0..3).map(|_| sleeping(200)).collect();
    let watching: Vec<_> = slow.iter().cloned().collect();

    let (first, rest) = Runtime::join_first(
        std::iter::once(quick).chain(slow),
        JoinPolicy::Drop,
    );

    assert!(rest.is_none(), "Drop should not hand the losers back");
    assert!(first.settled(), "the winner should be settled");

    // Dropped, not cancelled, so they run to the end and publish
    for handle in &watching {
        let state = handle.wait().expect("a dropped loser still settles");

        assert_ne!(state, TaskState::Cancelled, "a dropped loser was cancelled");
        assert!(handle.is_ready() || handle.is_taken(), "a dropped loser never finished");
    }
}

/// An empty set gives back a handle to no task
#[test]
fn an_empty_set_gives_back_a_dead_handle() {
    Runtime::init();

    let (first, rest) = Runtime::join_first(Vec::<TaskHandle<Duration>>::new(), JoinPolicy::Cancel);

    assert!(rest.is_none(), "Cancel should not hand anything back");

    assert_eq!(
        first.maybe_join(),
        Err(RuntimeError::NoSuchTask),
        "a dead handle should refuse rather than wait",
    );

    let (_, rest) = Runtime::join_first(Vec::<TaskHandle<Duration>>::new(), JoinPolicy::PassBack);

    assert_eq!(
        rest.map(|losers| losers.len()),
        Some(0),
        "PassBack over nothing should hand back nothing, not None",
    );
}

/// A set of one gives back that one task
#[test]
fn a_set_of_one_is_just_a_join() {
    Runtime::init();

    let only = sleeping(20);
    let only_id = only.id();

    let (first, rest) = Runtime::join_first(vec![only], JoinPolicy::PassBack);

    assert_eq!(first.id(), only_id, "the only task should have won");
    assert_eq!(rest.map(|losers| losers.len()), Some(0), "there are no losers");

    first.join().expect("the winner still has its output");
}

/// File reads can race each other, and the losers still read
#[test]
fn file_reads_race_each_other() {
    Runtime::init();

    let small = TestFile::new("small", b"quick");
    let large = TestFile::new("large", &vec![b'x'; 8 * 1024 * 1024]);

    let handles = vec![
        Runtime::task(File::read(small.path())).spawn(),
        Runtime::task(File::read(large.path())).spawn(),
        Runtime::task(File::read(large.path())).spawn(),
    ];

    let (first, rest) = Runtime::join_first(handles, JoinPolicy::PassBack);

    let winner = first.join().expect("the winner joins").expect("the read worked");

    println!("the winning read was {} bytes", winner.len());

    // Nothing is asserted about which one won
    for handle in rest.expect("PassBack hands the losers back") {
        handle.join().expect("a loser joins").expect("the read worked");
    }
}

/// Two threads racing the same set both get an answer
#[test]
fn a_race_from_several_threads_at_once() {
    Runtime::init();

    // The same set, raced by two threads
    let shared: Vec<_> = (0..4).map(|_| sleeping(60)).collect();

    let crews: Vec<_> = (0..2)
        .map(|_| {
            let mine: Vec<_> = shared.iter().cloned().collect();

            thread::spawn(move || {
                let (first, rest) = Runtime::join_first(mine, JoinPolicy::PassBack);

                assert!(first.settled(), "the winner should be settled");
                assert_eq!(rest.map(|losers| losers.len()), Some(3), "wrong loser count");

                first.id()
            })
        })
        .collect();

    for crew in crews {
        crew.join().expect("a racing thread went down");
    }

    for handle in shared {
        handle.join().expect("every task still finishes");
    }
}
