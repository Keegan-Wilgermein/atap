//! File watch task tests

use atap::{Change, File, Runtime, RuntimeError, TaskHandle};
use std::{
    fs,
    path::PathBuf,
    process,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Keeps test file names apart
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A path that cleans itself up
struct TestPath(PathBuf);

impl TestPath {
    /// Reserves a name nothing else in this run will use
    fn new(tag: &str) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

        fs::create_dir_all(&root).expect("could not make tests/files");

        let name = format!(
            "{}-{}-{}.txt",
            tag,
            process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );

        Self(root.join(name))
    }

    /// The path itself
    fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Waits until a spawned task has reached its park
///
/// The extra pause afterwards is for the registration itself,
/// which happens after the task stops being pending
fn until_parked<T>(handle: &TaskHandle<T>) {
    let deadline = Instant::now() + PATIENCE;

    while handle.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    thread::sleep(Duration::from_millis(30));
}

/// Takes the next output a repeat produces
///
/// ## Returns
/// `None` once the series has ended, or once `PATIENCE` has run
/// out
fn next_run<T>(handle: &TaskHandle<T>) -> Option<T> {
    let deadline = Instant::now() + PATIENCE;

    while Instant::now() < deadline {
        match handle.maybe_take() {
            Ok(value) => return Some(value),

            // Between runs, or one still going
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::NotReady) => {
                thread::sleep(Duration::from_millis(1))
            }

            // `Finished` and every other error are endings
            Err(_) => break,
        }
    }

    None
}

/// Adds bytes to the end of a file, which is what every test
/// here changes a path with
///
/// #### Note
/// An append rather than a `fs::write`, which truncates first. A
/// watch can catch a truncating write between its two halves and
/// report a file that got shorter, which is a true answer but not
/// one a test can rely on. Appending only ever grows the file
fn touch(path: &PathBuf, bytes: &[u8]) {
    use std::io::Write;

    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(path)
        .expect("could not open the watched file");

    file.write_all(bytes).expect("could not write the watched file");
}

/// A write to a watched file wakes the task waiting on it
#[test]
fn a_write_wakes_the_watch() {
    Runtime::init();

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
    Runtime::init();

    let file = TestPath::new("untouched");
    fs::write(file.path(), b"still").unwrap();

    let watch = File::watch(file.path()).timeout(Duration::from_millis(200));
    let started = Instant::now();
    let change = Runtime::block(watch);

    println!("giving up took {:?} and gave {:?}", started.elapsed(), change);

    assert_eq!(change, Err(RuntimeError::TimedOut), "nothing touched the file");
    assert!(started.elapsed() < PATIENCE, "giving up took too long");
}

/// A repeating watch reports every change, one run each
#[test]
fn a_repeating_watch_reports_every_change() {
    Runtime::init();

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

        let change = next_run(&handle)
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
    Runtime::init();

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

    let first = next_run(&handle)
        .expect("the first run never reported")
        .expect("the watch failed");

    assert!(first.written(), "the first run saw its write");

    // Straight away, so it happens inside the gap and no watch is
    // on the file to see it
    touch(file.path(), b" three");

    let second = next_run(&handle)
        .expect("the change between runs was lost")
        .expect("the watch failed");

    println!("the change between runs reported {:?}", second);

    assert!(second.written(), "the second run found the write it missed");
}

/// Removing a watched file reports a removal
#[test]
fn a_removed_file_reads_as_removed() {
    Runtime::init();

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
    Runtime::init();

    let file = TestPath::new("removed-once");
    fs::write(file.path(), b"here for now").unwrap();

    let handle = Runtime::task(File::watch(file.path()))
        .repeat()
        .every(Duration::from_millis(20))
        .spawn();

    until_parked(&handle);

    fs::remove_file(file.path()).expect("could not remove the watched file");

    let change = next_run(&handle)
        .expect("the removal was never reported")
        .expect("the watch failed");

    assert!(change.removed(), "the last name the file had went");

    // Long enough for a spinning repeat to have run many times over
    thread::sleep(Duration::from_millis(400));

    assert_eq!(
        handle.maybe_take(),
        Err(RuntimeError::NotReady),
        "a file that is still gone has not changed again",
    );

    handle.cancel();
}

/// Moving a watched file out from under its name reports a
/// rename, and the watch stays on the file rather than the name
#[test]
fn a_renamed_file_reads_as_renamed() {
    Runtime::init();

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
    Runtime::init();

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

    assert!(change.written(), "an entry coming is a write to the directory");
}

/// A watch narrowed to removals sits through a write and settles
/// on the removal
#[test]
fn a_narrowed_watch_ignores_what_it_did_not_ask_for() {
    Runtime::init();

    let file = TestPath::new("narrowed");
    fs::write(file.path(), b"before").unwrap();

    let watch = File::watch(file.path()).only(Change::REMOVED);
    let handle = Runtime::task(watch).spawn();

    until_parked(&handle);

    touch(file.path(), b" written to");
    thread::sleep(Duration::from_millis(200));

    assert!(handle.is_running(), "a write is not what this watch asked for");

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
    Runtime::init();

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
    Runtime::init();

    let file = TestPath::new("blocking");
    fs::write(file.path(), b"before").unwrap();

    let path = file.path().clone();

    let writer = thread::spawn(move || {
        thread::sleep(Duration::from_millis(150));
        touch(&path, b" and after");
    });

    let change = Runtime::block(File::watch(file.path()).timeout(PATIENCE))
        .expect("the blocking watch failed");

    writer.join().expect("the writing thread panicked");

    println!("a blocking watch reported {:?}", change);

    assert!(change.written(), "the write the other thread made");
}

/// Cancelling a parked watch settles it, rather than leaving it
/// waiting on a file nothing will touch
#[test]
fn a_cancelled_watch_settles() {
    Runtime::init();

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
        watching.maybe_take(),
        Err(RuntimeError::Cancelled),
        "a cancelled watch reports the cancel",
    );
}
