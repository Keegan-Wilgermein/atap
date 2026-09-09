//! `join_first` — the race, and what becomes of the losers
//!
//! Its own binary because most of these want the pool quiet
//! enough that "the short one finished first" is a statement
//! about the tasks rather than about what else was queued

use atap::{File, JoinPolicy, Runtime, RuntimeError, Sleep, TaskHandle, TaskState};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

/// Names apart, so tests running side by side don't collide
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
    Runtime::task(Sleep::sleep(Duration::from_millis(millis), false)).spawn()
}

#[test]
fn the_quickest_one_wins() {
    Runtime::init();

    let quick = sleeping(5);
    let quick_id = quick.id();

    let slow: Vec<_> = (0..4).map(|_| sleeping(4000)).collect();

    let started = Instant::now();

    let (first, rest) = Runtime::join_first(
        std::iter::once(quick).chain(slow),
        JoinPolicy::Cancel,
    );

    let waited = started.elapsed();

    println!("the race took {:?}", waited);

    assert_eq!(first.id(), quick_id, "the wrong task won");
    assert!(rest.is_none(), "Cancel should not hand the losers back");

    // Well short of the four seconds the losers were asked for,
    // which is the whole claim being made
    assert!(waited < Duration::from_secs(2), "the race took {:?}", waited);

    assert!(first.settled(), "the winner should be settled");
}

#[test]
fn cancel_stops_the_losers() {
    Runtime::init();

    let quick = sleeping(5);

    // Cloned before the race, so there is still a way to look
    // at them after `join_first` has taken the originals
    let slow: Vec<_> = (0..3).map(|_| sleeping(4000)).collect();
    let watching: Vec<_> = slow.iter().cloned().collect();

    let (first, rest) = Runtime::join_first(
        std::iter::once(quick).chain(slow),
        JoinPolicy::Cancel,
    );

    assert!(rest.is_none(), "Cancel should not hand the losers back");
    assert!(first.settled(), "the winner should be settled");

    // A cancelled sleep is taken back out of the kernel, so
    // this lands quickly — but it is the state that is being
    // asserted, not the speed
    for handle in &watching {
        let state = handle.wait().expect("a cancelled task still settles");

        assert_eq!(state, TaskState::Cancelled, "a loser was not cancelled");
        assert!(handle.is_cancelled(), "is_cancelled disagrees with the state");
    }
}

#[test]
fn pass_back_hands_the_losers_over_in_order() {
    Runtime::init();

    let quick = sleeping(5);
    let quick_id = quick.id();

    let slow: Vec<_> = (0..4).map(|_| sleeping(300)).collect();
    let order: Vec<_> = slow.iter().map(|handle| handle.id()).collect();

    let (first, rest) = Runtime::join_first(
        std::iter::once(quick).chain(slow),
        JoinPolicy::PassBack,
    );

    assert_eq!(first.id(), quick_id, "the wrong task won");

    let rest = rest.expect("PassBack should hand the losers back");

    assert_eq!(rest.len(), 4, "expected four losers, got {}", rest.len());

    let handed: Vec<_> = rest.iter().map(|handle| handle.id()).collect();

    assert_eq!(handed, order, "the losers came back in a different order");

    // Untouched by having lost, so every one of them still
    // finishes and still has an output to give
    for handle in rest {
        handle.join().expect("a loser should still finish");
    }
}

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

    // Dropped, not cancelled. They run to the end and publish,
    // and the clones held here are what proves it
    for handle in &watching {
        let state = handle.wait().expect("a dropped loser still settles");

        assert_ne!(state, TaskState::Cancelled, "a dropped loser was cancelled");
        assert!(handle.is_ready() || handle.is_taken(), "a dropped loser never finished");
    }
}

#[test]
fn a_task_that_already_finished_wins_at_once() {
    Runtime::init();

    let done = sleeping(1);

    done.wait().expect("the task settles");

    let slow: Vec<_> = (0..3).map(|_| sleeping(4000)).collect();
    let done_id = done.id();

    let started = Instant::now();

    let (first, _) = Runtime::join_first(
        std::iter::once(done).chain(slow),
        JoinPolicy::Cancel,
    );

    let waited = started.elapsed();

    println!("an already settled task was found in {:?}", waited);

    assert_eq!(first.id(), done_id, "the settled task should have won");

    // The fast path, before anything is registered. If this
    // ever needed a notification it would have waited for one
    assert!(waited < Duration::from_millis(200), "took {:?}", waited);
}

#[test]
fn a_task_that_settles_during_registration_is_still_found() {
    Runtime::init();

    // Deliberately in the window: short enough that it can
    // finish while `join_first` is still walking the set and
    // registering, so its poke lands before anything is
    // listening for it. The second look is what has to catch it
    for _ in 0..32 {
        let racing = sleeping(0);
        let slow: Vec<_> = (0..8).map(|_| sleeping(4000)).collect();

        let started = Instant::now();

        let (first, _) = Runtime::join_first(
            std::iter::once(racing).chain(slow),
            JoinPolicy::Cancel,
        );

        let waited = started.elapsed();

        assert!(first.settled(), "the winner should be settled");

        // A missed notification would show up as the poll
        // ceiling rather than as a hang, which is exactly why
        // it would be easy to miss
        assert!(
            waited < Duration::from_millis(40),
            "the race took {:?}, which is the ceiling rather than a wake",
            waited,
        );
    }
}

#[test]
fn an_empty_set_gives_back_a_dead_handle() {
    Runtime::init();

    let (first, rest) = Runtime::join_first(Vec::<TaskHandle<Duration>>::new(), JoinPolicy::Cancel);

    assert!(rest.is_none(), "Cancel should not hand anything back");

    // No task, and there never was one, so it says so rather
    // than blocking on something that is never coming
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

    // Nothing is asserted about which one won. Three reads off
    // a warm page cache is a race the test has no business
    // predicting — what matters is that one of them did, and
    // that the other two are still good
    for handle in rest.expect("PassBack hands the losers back") {
        handle.join().expect("a loser joins").expect("the read worked");
    }
}

#[test]
fn a_race_from_several_threads_at_once() {
    Runtime::init();

    // The same set, raced by two threads. Only one of them can
    // register on any given slot, so the other is running on
    // the second look and the ceiling alone — which has to
    // reach the same answer, just less promptly
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
