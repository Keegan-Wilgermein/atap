//! File watch task tests

mod common;

use atap::{
    Runtime, RuntimeError, TaskHandle,
    fs::{Change, File},
};
use common::{TestPath, next_run, until_started, within};
use std::{
    fs,
    path::PathBuf,
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Waits until a spawned task has reached its park
///
/// The extra pause afterwards is for the registration itself,
/// which happens after the task stops being pending
fn until_parked<T>(handle: &TaskHandle<T>) {
    until_started(handle, PATIENCE);

    thread::sleep(Duration::from_millis(10));
}

/// Adds bytes to the end of a file, which is what every test
/// here changes a path with
///
/// #### Note
/// An append rather than a `fs::write`, which truncates first and
/// can be caught half way through
fn touch(path: &PathBuf, bytes: &[u8]) {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("could not open the watched file");

    file.write_all(bytes)
        .expect("could not write the watched file");
}

/// A write to a watched file wakes the task waiting on it
#[test]
fn a_write_wakes_the_watch() {
    let _ = Runtime::init();

    let file = TestPath::new("written");
    fs::write(file.path(), b"before").unwrap();

    let handle = Runtime::task(File::watch(file.path())).spawn();
    until_parked(&handle);

    assert!(handle.is_running(), "the watch is waiting on the file");

    touch(file.path(), b" and after");

    let change = handle
        .take_with_timeout(PATIENCE)
        .expect("the watch never woke")
        .expect("the watch failed");

    println!("a write reported {:?}", change);

    assert!(change.written(), "a write must report as a write");
    assert!(change.extended(), "the file got longer");
    assert!(!change.removed(), "the file is still there");
}

/// A watch with a timeout gives up on a file nothing touches
#[test]
fn a_watch_gives_up_when_asked_to() {
    let _ = Runtime::init();

    let file = TestPath::new("untouched");
    fs::write(file.path(), b"still").unwrap();

    let started = Instant::now();
    let change = within(File::watch(file.path()), Duration::from_millis(200));

    println!(
        "giving up took {:?} and gave {:?}",
        started.elapsed(),
        change
    );

    assert_eq!(
        change,
        Err(RuntimeError::TimedOut),
        "nothing touched the file"
    );
    assert!(started.elapsed() < PATIENCE, "giving up took too long");
}

/// A repeating watch reports every change, one run each
#[test]
fn a_repeating_watch_reports_every_change() {
    let _ = Runtime::init();

    let file = TestPath::new("repeated");
    fs::write(file.path(), b"one").unwrap();

    // Spaced, since a slot holds the latest output rather than a
    // queue of them
    let handle = Runtime::task(File::watch(file.path()))
        .repeat()
        .every(Duration::from_millis(50))
        .count(3)
        .spawn();

    until_parked(&handle);

    for round in 0..3 {
        touch(file.path(), b" more");

        let change = next_run(&handle, PATIENCE)
            .unwrap_or_else(|| panic!("run {} never reported", round))
            .expect("the watch failed");

        println!("run {} reported {:?}", round, change);

        assert!(change.written(), "run {} saw a write", round);
    }
}

/// A change that lands while the task is between runs is still
/// reported, rather than being lost with the run that missed it
#[test]
fn a_change_between_runs_is_not_lost() {
    let _ = Runtime::init();

    let file = TestPath::new("between");
    fs::write(file.path(), b"one").unwrap();

    // Long enough that the second write plainly lands while the
    // task is waiting out the gap rather than watching
    let gap = Duration::from_millis(400);

    let handle = Runtime::task(File::watch(file.path()))
        .repeat()
        .every(gap)
        .count(2)
        .spawn();

    until_parked(&handle);

    touch(file.path(), b" two");

    let first = next_run(&handle, PATIENCE)
        .expect("the first run never reported")
        .expect("the watch failed");

    assert!(first.written(), "the first run saw its write");

    // Straight away, so it happens inside the gap and no watch is
    // on the file to see it
    touch(file.path(), b" three");

    let second = next_run(&handle, PATIENCE)
        .expect("the change between runs was lost")
        .expect("the watch failed");

    println!("the change between runs reported {:?}", second);

    assert!(second.written(), "the second run found the write it missed");
}

/// Removing a watched file reports a removal
#[test]
fn a_removed_file_reads_as_removed() {
    let _ = Runtime::init();

    let file = TestPath::new("removed");
    fs::write(file.path(), b"here for now").unwrap();

    let handle = Runtime::task(File::watch(file.path())).spawn();
    until_parked(&handle);

    fs::remove_file(file.path()).expect("could not remove the watched file");

    let change = handle
        .take_with_timeout(PATIENCE)
        .expect("the watch never woke")
        .expect("the watch failed");

    println!("a removal reported {:?}", change);

    assert!(change.removed(), "the last name the file had went");
    assert!(!change.renamed(), "a removal is not a rename");
}

/// A removal is reported once and not again, so a repeat past
/// one waits rather than spinning on an answer it already gave
#[test]
fn a_repeating_watch_reports_a_removal_once() {
    let _ = Runtime::init();

    let file = TestPath::new("removed-once");
    fs::write(file.path(), b"here for now").unwrap();

    let handle = Runtime::task(File::watch(file.path()))
        .repeat()
        .every(Duration::from_millis(20))
        .spawn();

    until_parked(&handle);

    fs::remove_file(file.path()).expect("could not remove the watched file");

    let change = next_run(&handle, PATIENCE)
        .expect("the removal was never reported")
        .expect("the watch failed");

    assert!(change.removed(), "the last name the file had went");

    // Long enough for a spinning repeat to have run many times over
    thread::sleep(Duration::from_millis(400));

    assert_eq!(
        handle.try_take(),
        Err(RuntimeError::NotReady),
        "a file that is still gone has not changed again",
    );

    handle.cancel();
}

/// Moving a watched file out from under its name reports a
/// rename, and the watch stays on the file rather than the name
#[test]
fn a_renamed_file_reads_as_renamed() {
    let _ = Runtime::init();

    let file = TestPath::new("renamed");
    let moved = TestPath::new("renamed-to");

    fs::write(file.path(), b"about to move").unwrap();

    let handle = Runtime::task(File::watch(file.path())).spawn();
    until_parked(&handle);

    fs::rename(file.path(), moved.path()).expect("could not rename the watched file");

    let change = handle
        .take_with_timeout(PATIENCE)
        .expect("the watch never woke")
        .expect("the watch failed");

    println!("a rename reported {:?}", change);

    assert!(change.renamed(), "the path stopped leading to the file");
    assert!(!change.removed(), "the file itself is still there");
}

/// A new entry in a watched directory wakes the watch on it
#[test]
fn a_new_entry_wakes_a_directory_watch() {
    let _ = Runtime::init();

    let dir = TestPath::new("directory");
    fs::create_dir(dir.path()).expect("could not make the watched directory");

    let handle = Runtime::task(File::watch(dir.path())).spawn();
    until_parked(&handle);

    fs::write(dir.path().join("arrived.txt"), b"new").unwrap();

    let change = handle
        .take_with_timeout(PATIENCE)
        .expect("the watch never woke")
        .expect("the watch failed");

    println!("a new entry reported {:?}", change);

    assert!(
        change.written(),
        "an entry coming is a write to the directory"
    );
}

/// A watch narrowed to removals sits through a write and settles
/// on the removal
#[test]
fn a_narrowed_watch_ignores_what_it_did_not_ask_for() {
    let _ = Runtime::init();

    let file = TestPath::new("narrowed");
    fs::write(file.path(), b"before").unwrap();

    let watch = File::watch(file.path()).only(Change::REMOVED);
    let handle = Runtime::task(watch).spawn();

    until_parked(&handle);

    touch(file.path(), b" written to");
    thread::sleep(Duration::from_millis(200));

    assert!(
        handle.is_running(),
        "a write is not what this watch asked for"
    );

    fs::remove_file(file.path()).expect("could not remove the watched file");

    let change = handle
        .take_with_timeout(PATIENCE)
        .expect("the watch never woke")
        .expect("the watch failed");

    println!("the narrowed watch reported {:?}", change);

    assert_eq!(change, Change::REMOVED, "only the removal was asked for");
}

/// A path that isn't there is an error rather than a wait
#[test]
fn a_path_that_is_not_there_says_so() {
    let _ = Runtime::init();

    let file = TestPath::new("never-made");
    let change = Runtime::block(File::watch(file.path()));

    println!("a missing path gave {:?}", change);

    assert_eq!(
        change,
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        "there has to be something to watch",
    );
}

/// A blocking watch waits on its own thread and comes back with
/// the change, the same as a spawned one
#[test]
fn a_blocking_watch_waits_for_its_change() {
    let _ = Runtime::init();

    let file = TestPath::new("blocking");
    fs::write(file.path(), b"before").unwrap();

    let path = file.path().clone();

    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        touch(&path, b" and after");
    });

    let change = within(File::watch(file.path()), PATIENCE).expect("the blocking watch failed");

    writer.join().expect("the writing thread panicked");

    println!("a blocking watch reported {:?}", change);

    assert!(change.written(), "the write the other thread made");
}

/// Cancelling a parked watch settles it, rather than leaving it
/// waiting on a file nothing will touch
#[test]
fn a_cancelled_watch_settles() {
    let _ = Runtime::init();

    let file = TestPath::new("cancelled");
    fs::write(file.path(), b"never touched again").unwrap();

    let handle = Runtime::task(File::watch(file.path())).spawn();
    until_parked(&handle);

    assert!(handle.is_running(), "the watch is parked");

    // Kept, since cancelling takes the handle it is called on
    let watching = handle.clone();

    handle.cancel();

    let deadline = Instant::now() + PATIENCE;

    while !watching.settled() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    println!("the cancelled watch is {:?}", watching.state());

    assert!(watching.is_cancelled(), "a cancelled watch has to settle");
    assert_eq!(
        watching.try_take(),
        Err(RuntimeError::Cancelled),
        "a cancelled watch reports the cancel",
    );
}

/// A watch told to wait for a path settles once the path appears
#[test]
fn a_watch_can_wait_for_a_path_to_appear() {
    let _ = Runtime::init();

    let file = TestPath::new("appearing");
    let handle = Runtime::task(File::watch(file.path()).appear()).spawn();

    until_parked(&handle);
    assert!(
        handle.is_running(),
        "a missing path settled before it appeared"
    );

    fs::write(file.path(), b"here now").unwrap();

    let change = handle
        .take_with_timeout(PATIENCE)
        .expect("the watch must settle")
        .expect("the appearance must be reported");

    assert!(change.created(), "the change was {change:?}");
    assert!(!change.written());
}

/// Once a path has appeared, a repeat watches what appeared
#[test]
fn an_appeared_path_is_watched_after() {
    let _ = Runtime::init();

    let file = TestPath::new("appeared-then");
    let handle = Runtime::task(File::watch(file.path()).appear())
        .repeat()
        .every(Duration::from_millis(200))
        .spawn();

    until_parked(&handle);
    fs::write(file.path(), b"first").unwrap();

    let created = next_run(&handle, PATIENCE)
        .expect("the appearance")
        .unwrap();
    assert!(created.created());

    thread::sleep(Duration::from_millis(300));
    touch(file.path(), b" and more");

    let written = next_run(&handle, PATIENCE).expect("the write").unwrap();
    assert!(written.written(), "the change was {written:?}");
    assert!(!written.created());

    handle.cancel();
}

/// A path already there is watched as usual, appear or not
#[test]
fn appear_on_a_path_already_there_watches_it() {
    let _ = Runtime::init();

    let file = TestPath::new("already-there");
    fs::write(file.path(), b"was here").unwrap();

    let handle = Runtime::task(File::watch(file.path()).appear()).spawn();

    until_parked(&handle);
    touch(file.path(), b"!");

    let change = handle.take_with_timeout(PATIENCE).unwrap().unwrap();

    assert!(change.written());
    assert!(!change.created());
}

/// Waiting for a path still needs its directory
#[test]
fn appear_needs_the_directory() {
    let _ = Runtime::init();

    let file = TestPath::new("no-parent");

    assert_eq!(
        within(File::watch(file.path().join("child")).appear(), PATIENCE),
        Err(RuntimeError::CheckError(Some(libc::ENOENT)))
    );
}
