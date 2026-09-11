//! File task tests

use atap::{File, FileKind, Runtime, RuntimeError, TaskHandle};
use std::{
    ffi::OsStr,
    fs,
    path::PathBuf,
    process,
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

/// How much one read or write syscall asks for
///
/// Written out here since the crate's own is private
const CHUNK: usize = 64 * 1024;

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

/// A buffer whose contents say where in it you are
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// Takes the next output a repeat produces
///
/// ## Returns
/// `None` once the series has ended, or once `patience` has
/// run out
fn next_run<T>(handle: &TaskHandle<T>, patience: Duration) -> Option<T> {
    let deadline = Instant::now() + patience;

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

/// A blocking read gives back what was written
#[test]
fn read_gives_back_what_was_written() {
    Runtime::init();

    let file = TestPath::new("round-trip");
    fs::write(file.path(), b"the quick brown fox").unwrap();

    let read = Runtime::block(File::read(file.path())).expect("read failed");

    println!("read back {} bytes", read.len());

    assert_eq!(read.as_slice(), b"the quick brown fox".as_slice(), "the bytes came back changed");
}

/// A spawned read joins with the file's contents
#[test]
fn a_spawned_read_joins() {
    Runtime::init();

    let file = TestPath::new("spawned");
    fs::write(file.path(), b"through the builder").unwrap();

    let handle = Runtime::task(File::read(file.path())).spawn();
    let read = handle.join().expect("join failed").expect("read failed");

    assert_eq!(read.as_slice(), b"through the builder".as_slice(), "the bytes came back changed");
}

/// A missing file reports `ENOENT`
#[test]
fn read_of_a_missing_file_reports_enoent() {
    Runtime::init();

    let file = TestPath::new("never-made");

    let read = Runtime::block(File::read(file.path()));

    println!("missing file gave {:?}", read);

    assert_eq!(
        read,
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        "a missing file should say so"
    );
}

/// An empty file reads as an empty `Vec`
#[test]
fn an_empty_file_reads_as_empty() {
    Runtime::init();

    let file = TestPath::new("empty");
    fs::write(file.path(), b"").unwrap();

    let read = Runtime::block(File::read(file.path())).expect("read failed");

    assert!(read.is_empty(), "an empty file is not an error");
}

/// Files around and past the chunk size read whole
#[test]
fn a_file_bigger_than_one_chunk_reads_whole() {
    Runtime::init();

    // Either side of the chunk boundary and well past it
    for len in [CHUNK - 1, CHUNK, CHUNK + 1, CHUNK * 4 + 7] {
        let file = TestPath::new("big");
        let written = pattern(len);

        fs::write(file.path(), &written).unwrap();

        let read = Runtime::block(File::read(file.path())).expect("read failed");

        println!("asked for {} bytes, got {}", len, read.len());

        assert_eq!(read.len(), len, "wrong length at {} bytes", len);
        assert_eq!(read, written, "wrong contents at {} bytes", len);
    }
}

/// A write bigger than one chunk lands whole
#[test]
fn a_write_bigger_than_one_chunk_lands_whole() {
    Runtime::init();

    for len in [CHUNK, CHUNK + 1, CHUNK * 3 + 11] {
        let file = TestPath::new("big-write");
        let written = pattern(len);

        let count = Runtime::block(File::write(file.path(), written.as_slice()))
            .expect("write failed");

        assert_eq!(count, len, "the count came back short at {} bytes", len);

        let back = fs::read(file.path()).unwrap();

        assert_eq!(back, written, "wrong contents at {} bytes", len);
    }
}

/// Append adds to a file rather than replacing it
#[test]
fn append_adds_rather_than_replaces() {
    Runtime::init();

    let file = TestPath::new("append");

    Runtime::block(File::write(file.path(), b"first".as_slice())).expect("write failed");
    Runtime::block(File::append(file.path(), b"-second".as_slice())).expect("append failed");

    let back = Runtime::block(File::read(file.path())).expect("read failed");

    assert_eq!(back.as_slice(), b"first-second".as_slice(), "append replaced instead of adding");
}

/// `write_at` changes only the bytes it writes
#[test]
fn write_at_leaves_the_rest_alone() {
    Runtime::init();

    let file = TestPath::new("write-at");

    Runtime::block(File::write(file.path(), b"aaaaaaaaaa".as_slice())).expect("write failed");
    Runtime::block(File::write_at(file.path(), 3, b"bb".as_slice())).expect("write_at failed");

    let back = Runtime::block(File::read(file.path())).expect("read failed");

    assert_eq!(back.as_slice(), b"aaabbaaaaa".as_slice(), "a positional write moved to the end");
}

/// `read_at` reads a range, and comes back short at the end
#[test]
fn read_at_takes_a_range_and_stops_at_the_end() {
    Runtime::init();

    let file = TestPath::new("range");
    fs::write(file.path(), b"0123456789").unwrap();

    let middle = Runtime::block(File::read_at(file.path(), 3, 4)).expect("read_at failed");

    assert_eq!(middle.as_slice(), b"3456".as_slice(), "the wrong range came back");

    // Running off the end is an answer, not a failure
    let tail = Runtime::block(File::read_at(file.path(), 8, 100)).expect("read_at failed");

    assert_eq!(tail.as_slice(), b"89".as_slice(), "a range past the end should come back short");

    let past = Runtime::block(File::read_at(file.path(), 50, 10)).expect("read_at failed");

    assert!(past.is_empty(), "a range starting past the end is empty");
}

/// A path with a zero byte in it is refused
#[test]
fn a_path_with_a_zero_byte_is_refused() {
    Runtime::init();

    let read = Runtime::block(File::read("tests/files/a\0b"));

    println!("a path with a zero gave {:?}", read);

    assert_eq!(read, Err(RuntimeError::BadPath), "a zero byte must be refused");
}

/// Reading a directory errors rather than hanging
#[test]
fn reading_a_directory_errors_rather_than_hanging() {
    Runtime::init();

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");
    fs::create_dir_all(&root).unwrap();

    let read = Runtime::block(File::read(&root));

    println!("reading a directory gave {:?}", read);

    assert_eq!(
        read,
        Err(RuntimeError::CheckError(Some(libc::EISDIR))),
        "a directory should say what it is"
    );
}

/// Metadata reports the length and the kind
#[test]
fn metadata_reports_the_length_and_the_kind() {
    Runtime::init();

    let file = TestPath::new("meta");
    fs::write(file.path(), pattern(1234)).unwrap();

    let meta = Runtime::block(File::metadata(file.path())).expect("metadata failed");

    println!("metadata: {:?}", meta);

    assert_eq!(meta.len(), 1234, "the wrong length came back");
    assert_eq!(meta.kind(), FileKind::File, "a file is not a file");
    assert!(meta.is_file(), "is_file disagrees with kind");
    assert!(!meta.is_dir(), "a file is not a directory");
}

/// `read_dir` finds a new file and leaves out `.` and `..`
#[test]
fn read_dir_finds_a_new_file_and_omits_the_dot_entries() {
    Runtime::init();

    let dir = TestPath::new("listing");
    fs::create_dir_all(dir.path()).unwrap();

    let inside = dir.path().join("inside.txt");
    fs::write(&inside, b"here").unwrap();

    let listed = Runtime::block(File::read_dir(dir.path())).expect("read_dir failed");

    println!("listed {:?}", listed);

    assert!(listed.contains(&inside), "the file that is there wasn't listed");

    for entry in &listed {
        let name = entry.file_name().expect("an entry with no name");

        assert!(
            name != OsStr::new(".") && name != OsStr::new(".."),
            "the dot entries should be left out"
        );
    }
}

/// Creating, removing and renaming do what they say
#[test]
fn create_remove_and_rename_do_what_they_say() {
    Runtime::init();

    let from = TestPath::new("before-rename");
    let to = TestPath::new("after-rename");

    Runtime::block(File::write(from.path(), b"moving".as_slice())).expect("write failed");
    Runtime::block(File::rename(from.path(), to.path())).expect("rename failed");

    let moved = Runtime::block(File::read(to.path())).expect("read failed");

    assert_eq!(moved.as_slice(), b"moving".as_slice(), "the contents didn't survive the rename");

    let old = Runtime::block(File::read(from.path()));

    assert_eq!(
        old,
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        "the old path should be gone"
    );

    Runtime::block(File::remove(to.path())).expect("remove failed");

    let gone = Runtime::block(File::read(to.path()));

    assert_eq!(
        gone,
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        "the removed path should be gone"
    );
}

/// A repeated read sees the file change between runs
#[test]
fn a_repeated_read_sees_the_file_change() {
    Runtime::init();

    let file = TestPath::new("watched");
    fs::write(file.path(), b"one").unwrap();

    let handle = Runtime::task(File::read(file.path()))
        .repeat()
        .every(Duration::from_millis(30))
        .spawn();

    // Taken before the file changes, so this run can only have
    // read the first contents
    let first = next_run(&handle, Duration::from_secs(10)).expect("no first run");

    assert_eq!(
        first.expect("read failed").as_slice(),
        b"one".as_slice(),
        "wrong first read"
    );

    fs::write(file.path(), b"two").unwrap();

    let deadline = Instant::now() + Duration::from_secs(10);
    let mut saw = false;

    while Instant::now() < deadline {
        let Some(read) = next_run(&handle, Duration::from_secs(10)) else {
            break;
        };

        if read.expect("read failed").as_slice() == b"two".as_slice() {
            saw = true;
            break;
        }
    }

    handle.cancel();

    assert!(saw, "a repeat never picked the change up");
}

/// Reads on a fixed rate produce their output
#[test]
fn at_rate_reads_produce_their_output() {
    Runtime::init();

    let file = TestPath::new("rated");
    fs::write(file.path(), b"at a rate").unwrap();

    let handle = Runtime::task(File::read(file.path()))
        .at_rate(Duration::from_millis(25))
        .count(3)
        .spawn();

    let first = handle
        .take_with_timeout(Duration::from_secs(5))
        .expect("no run landed");

    assert_eq!(first.expect("read failed").as_slice(), b"at a rate".as_slice(), "wrong contents");

    handle.cancel();
}

/// Concurrent reads of one file all read the same contents
#[test]
fn concurrent_reads_of_one_file_all_agree() {
    Runtime::init();

    let file = TestPath::new("shared");
    let written = pattern(CHUNK * 2 + 9);

    fs::write(file.path(), &written).unwrap();

    let handles: Vec<_> = (0..32)
        .map(|_| Runtime::task(File::read(file.path())).spawn())
        .collect();

    for handle in handles {
        let read = handle.join().expect("join failed").expect("read failed");

        assert_eq!(read, written, "a concurrent read disagreed");
    }
}

/// Cancelling a read settles its listeners, and the file still
/// reads afterwards
#[test]
fn cancelling_a_read_settles_its_listeners() {
    Runtime::init();

    let file = TestPath::new("cancelled");
    fs::write(file.path(), pattern(CHUNK * 16)).unwrap();

    let handle = Runtime::task(File::read(file.path())).spawn();

    handle.cancel();

    let file = TestPath::new("second");
    fs::write(file.path(), b"still working").unwrap();

    let after = Runtime::block(File::read(file.path())).expect("read failed");

    assert_eq!(after.as_slice(), b"still working".as_slice(), "the runtime stopped working");
}
