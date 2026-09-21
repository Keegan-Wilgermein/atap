//! File task tests

mod common;

use atap::{
    Runtime, RuntimeError,
    fs::{File, FileKind, LockKind},
};
use common::{TestPath, next_run};
use std::{
    ffi::OsStr,
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};

/// How much one read or write syscall asks for
///
/// Written out here since the crate's own is private
const CHUNK: usize = 64 * 1024;

/// A buffer whose contents say where in it you are
fn pattern(len: usize) -> Vec<u8> {
    (0..len).map(|index| (index % 251) as u8).collect()
}

/// A blocking read gives back what was written
#[test]
fn read_gives_back_what_was_written() {
    let _ = Runtime::init();

    let file = TestPath::new("round-trip");
    fs::write(file.path(), b"the quick brown fox").unwrap();

    let read = Runtime::block(File::read(file.path())).expect("read failed");

    println!("read back {} bytes", read.len());

    assert_eq!(
        read.as_slice(),
        b"the quick brown fox".as_slice(),
        "the bytes came back changed"
    );
}

/// A spawned read joins with the file's contents
#[test]
fn a_spawned_read_joins() {
    let _ = Runtime::init();

    let file = TestPath::new("spawned");
    fs::write(file.path(), b"through the builder").unwrap();

    let handle = Runtime::task(File::read(file.path())).spawn();
    let read = handle.join().expect("join failed").expect("read failed");

    assert_eq!(
        read.as_slice(),
        b"through the builder".as_slice(),
        "the bytes came back changed"
    );
}

/// A missing file reports `ENOENT`
#[test]
fn read_of_a_missing_file_reports_enoent() {
    let _ = Runtime::init();

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
    let _ = Runtime::init();

    let file = TestPath::new("empty");
    fs::write(file.path(), b"").unwrap();

    let read = Runtime::block(File::read(file.path())).expect("read failed");

    assert!(read.is_empty(), "an empty file is not an error");
}

/// Files around and past the chunk size read whole
#[test]
fn a_file_bigger_than_one_chunk_reads_whole() {
    let _ = Runtime::init();

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
    let _ = Runtime::init();

    for len in [CHUNK, CHUNK + 1, CHUNK * 3 + 11] {
        let file = TestPath::new("big-write");
        let written = pattern(len);

        let count =
            Runtime::block(File::write(file.path(), written.as_slice())).expect("write failed");

        assert_eq!(count, len, "the count came back short at {} bytes", len);

        let back = fs::read(file.path()).unwrap();

        assert_eq!(back, written, "wrong contents at {} bytes", len);
    }
}

/// Append adds to a file rather than replacing it
#[test]
fn append_adds_rather_than_replaces() {
    let _ = Runtime::init();

    let file = TestPath::new("append");

    Runtime::block(File::write(file.path(), b"first".as_slice())).expect("write failed");
    Runtime::block(File::append(file.path(), b"-second".as_slice())).expect("append failed");

    let back = Runtime::block(File::read(file.path())).expect("read failed");

    assert_eq!(
        back.as_slice(),
        b"first-second".as_slice(),
        "append replaced instead of adding"
    );
}

/// `write_at` changes only the bytes it writes
#[test]
fn write_at_leaves_the_rest_alone() {
    let _ = Runtime::init();

    let file = TestPath::new("write-at");

    Runtime::block(File::write(file.path(), b"aaaaaaaaaa".as_slice())).expect("write failed");
    Runtime::block(File::write_at(file.path(), 3, b"bb".as_slice())).expect("write_at failed");

    let back = Runtime::block(File::read(file.path())).expect("read failed");

    assert_eq!(
        back.as_slice(),
        b"aaabbaaaaa".as_slice(),
        "a positional write moved to the end"
    );
}

/// `read_at` reads a range, and comes back short at the end
#[test]
fn read_at_takes_a_range_and_stops_at_the_end() {
    let _ = Runtime::init();

    let file = TestPath::new("range");
    fs::write(file.path(), b"0123456789").unwrap();

    let middle = Runtime::block(File::read_at(file.path(), 3, 4)).expect("read_at failed");

    assert_eq!(
        middle.as_slice(),
        b"3456".as_slice(),
        "the wrong range came back"
    );

    // Running off the end is an answer, not a failure
    let tail = Runtime::block(File::read_at(file.path(), 8, 100)).expect("read_at failed");

    assert_eq!(
        tail.as_slice(),
        b"89".as_slice(),
        "a range past the end should come back short"
    );

    let past = Runtime::block(File::read_at(file.path(), 50, 10)).expect("read_at failed");

    assert!(past.is_empty(), "a range starting past the end is empty");
}

/// A path with a zero byte in it is refused
#[test]
fn a_path_with_a_zero_byte_is_refused() {
    let _ = Runtime::init();

    let read = Runtime::block(File::read("tests/files/a\0b"));

    println!("a path with a zero gave {:?}", read);

    assert_eq!(
        read,
        Err(RuntimeError::BadPath),
        "a zero byte must be refused"
    );
}

/// Reading a directory errors rather than hanging
#[test]
fn reading_a_directory_errors_rather_than_hanging() {
    let _ = Runtime::init();

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
    let _ = Runtime::init();

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
    let _ = Runtime::init();

    let dir = TestPath::new("listing");
    fs::create_dir_all(dir.path()).unwrap();

    let inside = dir.path().join("inside.txt");
    fs::write(&inside, b"here").unwrap();

    let listed = Runtime::block(File::read_dir(dir.path())).expect("read_dir failed");

    println!("listed {:?}", listed);

    assert!(
        listed
            .iter()
            .any(|entry| entry.path() == inside && entry.kind() == FileKind::File),
        "the file that is there wasn't listed as a file"
    );

    for entry in &listed {
        let name = entry.path().file_name().expect("an entry with no name");

        assert!(
            name != OsStr::new(".") && name != OsStr::new(".."),
            "the dot entries should be left out"
        );
    }
}

/// Creating, removing and renaming do what they say
#[test]
fn create_remove_and_rename_do_what_they_say() {
    let _ = Runtime::init();

    let from = TestPath::new("before-rename");
    let to = TestPath::new("after-rename");

    Runtime::block(File::write(from.path(), b"moving".as_slice())).expect("write failed");
    Runtime::block(File::rename(from.path(), to.path())).expect("rename failed");

    let moved = Runtime::block(File::read(to.path())).expect("read failed");

    assert_eq!(
        moved.as_slice(),
        b"moving".as_slice(),
        "the contents didn't survive the rename"
    );

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
    let _ = Runtime::init();

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
    let _ = Runtime::init();

    let file = TestPath::new("rated");
    fs::write(file.path(), b"at a rate").unwrap();

    let handle = Runtime::task(File::read(file.path()))
        .at_rate(Duration::from_millis(25))
        .count(3)
        .spawn();

    let first = handle
        .take_with_timeout(Duration::from_secs(5))
        .expect("no run landed");

    assert_eq!(
        first.expect("read failed").as_slice(),
        b"at a rate".as_slice(),
        "wrong contents"
    );

    handle.cancel();
}

/// Concurrent reads of one file all read the same contents
#[test]
fn concurrent_reads_of_one_file_all_agree() {
    let _ = Runtime::init();

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
    let _ = Runtime::init();

    let file = TestPath::new("cancelled");
    fs::write(file.path(), pattern(CHUNK * 16)).unwrap();

    let handle = Runtime::task(File::read(file.path())).spawn();

    handle.cancel();

    let file = TestPath::new("second");
    fs::write(file.path(), b"still working").unwrap();

    let after = Runtime::block(File::read(file.path())).expect("read failed");

    assert_eq!(
        after.as_slice(),
        b"still working".as_slice(),
        "the runtime stopped working"
    );
}

/// A copy has the same bytes and permissions, and a copy over an
/// existing file replaces it
#[test]
fn copy_duplicates_a_file() {
    let _ = Runtime::init();

    let from = TestPath::new("copy-from");
    let to = TestPath::new("copy-to");
    let data = pattern(3 * CHUNK + 5);

    fs::write(from.path(), &data).unwrap();
    Runtime::block(File::set_permissions(from.path(), 0o640)).unwrap();

    assert_eq!(
        Runtime::block(File::copy(from.path(), to.path())),
        Ok(data.len() as u64)
    );
    assert_eq!(fs::read(to.path()).unwrap(), data);

    let mode = Runtime::block(File::metadata(to.path())).unwrap().mode();
    assert_eq!(mode & 0o777, 0o640);

    fs::write(from.path(), b"shorter").unwrap();

    assert_eq!(Runtime::block(File::copy(from.path(), to.path())), Ok(7));
    assert_eq!(fs::read(to.path()).unwrap(), b"shorter");

    // The copy is its own file
    fs::write(to.path(), b"changed").unwrap();
    assert_eq!(fs::read(from.path()).unwrap(), b"shorter");
}

/// A directory, and a file that isn't there, can't be copied
#[test]
fn copy_refuses_what_it_cannot_copy() {
    let _ = Runtime::init();

    let dir = TestPath::new("copy-dir");
    let to = TestPath::new("copy-dir-to");

    fs::create_dir(dir.path()).unwrap();

    assert_eq!(
        Runtime::block(File::copy(dir.path(), to.path())),
        Err(RuntimeError::CheckError(Some(libc::EISDIR)))
    );
    assert_eq!(
        Runtime::block(File::copy(dir.path().join("missing"), to.path())),
        Err(RuntimeError::CheckError(Some(libc::ENOENT)))
    );
}

/// A symbolic link reads back as written, and resolves to its target
#[test]
fn a_symlink_points_where_it_was_told() {
    let _ = Runtime::init();

    let target = TestPath::new("link-target");
    let link = TestPath::new("link");

    fs::write(target.path(), b"behind the link").unwrap();

    Runtime::block(File::symlink(target.path(), link.path())).unwrap();

    assert_eq!(
        Runtime::block(File::read_link(link.path())),
        Ok(target.path().clone())
    );
    assert_eq!(fs::read(link.path()).unwrap(), b"behind the link");
    assert_eq!(
        Runtime::block(File::canonicalize(link.path())),
        Ok(fs::canonicalize(target.path()).unwrap())
    );
    assert_eq!(
        Runtime::block(File::symlink(target.path(), link.path())),
        Err(RuntimeError::CheckError(Some(libc::EEXIST)))
    );
    assert_eq!(
        Runtime::block(File::read_link(target.path())),
        Err(RuntimeError::CheckError(Some(libc::EINVAL)))
    );
}

/// A link to a long target reads back whole
#[test]
fn a_long_link_target_reads_back_whole() {
    let _ = Runtime::init();

    let link = TestPath::new("long-link");
    let target = PathBuf::from("x".repeat(200))
        .join("y".repeat(200))
        .join("z".repeat(200));

    Runtime::block(File::symlink(&target, link.path())).unwrap();

    assert_eq!(Runtime::block(File::read_link(link.path())), Ok(target));
}

/// A hard link is the same file under a second name
#[test]
fn a_hard_link_is_the_same_file() {
    let _ = Runtime::init();

    let first = TestPath::new("hard-first");
    let second = TestPath::new("hard-second");

    fs::write(first.path(), b"one").unwrap();

    Runtime::block(File::hard_link(first.path(), second.path())).unwrap();
    fs::write(second.path(), b"two").unwrap();

    assert_eq!(fs::read(first.path()).unwrap(), b"two");
}

/// Canonicalizing takes out `.` and `..`, and needs the path to exist
#[test]
fn canonicalize_resolves_dots() {
    let _ = Runtime::init();

    let dir = TestPath::new("canon");
    fs::create_dir(dir.path()).unwrap();

    let dotted = dir
        .path()
        .join(".")
        .join("..")
        .join(dir.path().file_name().unwrap());

    assert_eq!(
        Runtime::block(File::canonicalize(&dotted)),
        Ok(fs::canonicalize(dir.path()).unwrap())
    );
    assert_eq!(
        Runtime::block(File::canonicalize(dir.path().join("missing"))),
        Err(RuntimeError::CheckError(Some(libc::ENOENT)))
    );
}

/// Permissions are set as asked, and nonsense is refused
#[test]
fn set_permissions_sets_the_mode() {
    let _ = Runtime::init();

    let file = TestPath::new("mode");
    fs::write(file.path(), b"x").unwrap();

    Runtime::block(File::set_permissions(file.path(), 0o600)).unwrap();
    assert_eq!(
        Runtime::block(File::metadata(file.path())).unwrap().mode() & 0o7777,
        0o600
    );

    assert_eq!(
        Runtime::block(File::set_permissions(file.path(), 0o10000)),
        Err(RuntimeError::BadArgument)
    );
}

/// A length cuts a file short or pads it with zeros
#[test]
fn set_len_cuts_and_pads() {
    let _ = Runtime::init();

    let file = TestPath::new("len");
    fs::write(file.path(), b"abcdef").unwrap();

    Runtime::block(File::set_len(file.path(), 3)).unwrap();
    assert_eq!(fs::read(file.path()).unwrap(), b"abc");

    Runtime::block(File::set_len(file.path(), 5)).unwrap();
    assert_eq!(fs::read(file.path()).unwrap(), b"abc\0\0");
}

/// Every missing directory is made, and one already there is fine
#[test]
fn create_dir_all_makes_the_whole_chain() {
    let _ = Runtime::init();

    let root = TestPath::new("tree-make");
    let deep = root.path().join("a").join("b").join("c");

    Runtime::block(File::create_dir_all(&deep)).unwrap();
    assert!(deep.is_dir());

    Runtime::block(File::create_dir_all(&deep)).unwrap();
    Runtime::block(File::create_dir_all(format!("{}//", deep.display()))).unwrap();

    let file = deep.join("file");
    fs::write(&file, b"x").unwrap();

    assert_eq!(
        Runtime::block(File::create_dir_all(file.join("under"))),
        Err(RuntimeError::CheckError(Some(libc::ENOTDIR)))
    );

    Runtime::block(File::remove_dir_all(root.path())).unwrap();
}

/// A whole tree goes, but a link inside it never leads outside it
#[test]
fn remove_dir_all_stays_inside() {
    let _ = Runtime::init();

    let root = TestPath::new("tree-gone");
    let outside = TestPath::new("tree-outside");

    fs::create_dir(outside.path()).unwrap();
    fs::write(outside.path().join("keep"), b"safe").unwrap();

    for branch in 0..5 {
        let dir = root.path().join(format!("d{branch}")).join("inner");
        fs::create_dir_all(&dir).unwrap();

        for leaf in 0..20 {
            fs::write(dir.join(format!("f{leaf}")), b"leaf").unwrap();
        }
    }

    std::os::unix::fs::symlink(outside.path(), root.path().join("d0").join("escape")).unwrap();

    Runtime::block(File::remove_dir_all(root.path())).unwrap();

    assert!(!root.path().exists(), "the tree is still there");
    assert_eq!(fs::read(outside.path().join("keep")).unwrap(), b"safe");

    // A plain file, or a link, goes the same way
    let file = TestPath::new("tree-file");
    fs::write(file.path(), b"x").unwrap();

    Runtime::block(File::remove_dir_all(file.path())).unwrap();
    assert!(!file.path().exists());

    assert_eq!(
        Runtime::block(File::remove_dir_all(root.path())),
        Err(RuntimeError::CheckError(Some(libc::ENOENT)))
    );
}

/// A tree removal can be cancelled part way
#[test]
fn remove_dir_all_can_be_cancelled() {
    let _ = Runtime::init();

    let root = TestPath::new("tree-cancel");

    for branch in 0..50 {
        let dir = root.path().join(format!("d{branch}"));
        fs::create_dir_all(&dir).unwrap();

        for leaf in 0..100 {
            fs::write(dir.join(format!("f{leaf}")), b"").unwrap();
        }
    }

    let handle = Runtime::task(File::remove_dir_all(root.path())).spawn();
    handle.clone().cancel();

    let got = handle.join_with_timeout(Duration::from_secs(10));

    assert!(
        matches!(got, Err(RuntimeError::Cancelled) | Ok(Ok(()))),
        "a cancelled removal gave {got:?}"
    );

    let _ = fs::remove_dir_all(root.path());
}

/// An open file reads and writes where it is told, from any clone
#[test]
fn an_open_file_reads_and_writes_in_place() {
    let _ = Runtime::init();

    let path = TestPath::new("open-rw");

    let file = Runtime::block(File::open(path.path()).write(true).create(true))
        .expect("a new file must open");

    assert_eq!(
        Runtime::block(file.write_at(0, b"hello world".as_slice())),
        Ok(11)
    );
    assert_eq!(Runtime::block(file.write_at(6, b"there".as_slice())), Ok(5));

    let copy = file.clone();

    assert_eq!(
        Runtime::block(copy.read_at(0, 64)),
        Ok(b"hello there".to_vec())
    );
    assert_eq!(Runtime::block(copy.read_at(6, 3)), Ok(b"the".to_vec()));
    assert_eq!(Runtime::block(copy.read_at(100, 3)), Ok(Vec::new()));

    assert_eq!(Runtime::block(file.append(b"!".as_slice())), Ok(1));
    assert_eq!(fs::read(path.path()).unwrap(), b"hello there!");

    Runtime::block(file.set_len(5)).unwrap();
    Runtime::block(file.sync()).unwrap();

    let metadata = Runtime::block(file.metadata()).unwrap();

    assert_eq!(metadata.len(), 5);
    assert_eq!(metadata.kind(), FileKind::File);
}

/// A file opened to append always writes on the end
#[test]
fn an_appending_file_writes_on_the_end() {
    let _ = Runtime::init();

    let path = TestPath::new("open-append");
    fs::write(path.path(), b"start").unwrap();

    let file = Runtime::block(File::open(path.path()).read(false).append(true)).unwrap();

    Runtime::block(file.append(b"-one".as_slice())).unwrap();
    Runtime::block(file.write_at(0, b"-two".as_slice())).unwrap();

    assert_eq!(fs::read(path.path()).unwrap(), b"start-one-two");
}

/// Settings that mean nothing are refused, and so are the ones the
/// file itself forbids
#[test]
fn open_settings_are_checked() {
    let _ = Runtime::init();

    let path = TestPath::new("open-settings");

    assert_eq!(
        Runtime::block(File::open(path.path()).read(false)).map(|_| ()),
        Err(RuntimeError::BadArgument)
    );
    assert_eq!(
        Runtime::block(File::open(path.path()).create(true)).map(|_| ()),
        Err(RuntimeError::BadArgument)
    );
    assert_eq!(
        Runtime::block(File::open(path.path())).map(|_| ()),
        Err(RuntimeError::CheckError(Some(libc::ENOENT)))
    );

    let made = Runtime::block(File::open(path.path()).write(true).create_new(true));
    assert!(made.is_ok());

    assert_eq!(
        Runtime::block(File::open(path.path()).write(true).create_new(true)).map(|_| ()),
        Err(RuntimeError::CheckError(Some(libc::EEXIST)))
    );

    let read_only = Runtime::block(File::open(path.path())).unwrap();

    assert_eq!(
        Runtime::block(read_only.write_at(0, b"x".as_slice())),
        Err(RuntimeError::CheckError(Some(libc::EBADF)))
    );

    fs::write(path.path(), b"full").unwrap();
    let emptied = Runtime::block(File::open(path.path()).write(true).truncate(true)).unwrap();

    assert_eq!(Runtime::block(emptied.metadata()).unwrap().len(), 0);
}

/// A lock keeps out another open file, and goes when it is let go
#[test]
fn an_exclusive_lock_keeps_others_out() {
    let _ = Runtime::init();

    let path = TestPath::new("open-lock");
    fs::write(path.path(), b"").unwrap();

    let first = Runtime::block(File::open(path.path())).unwrap();
    let second = Runtime::block(File::open(path.path())).unwrap();

    Runtime::block(first.lock(LockKind::Exclusive)).unwrap();

    assert_eq!(
        Runtime::block(second.try_lock(LockKind::Shared)),
        Err(RuntimeError::NotReady)
    );

    let waiting = Runtime::task(second.lock(LockKind::Exclusive)).spawn();
    std::thread::sleep(Duration::from_millis(50));

    assert!(
        waiting.is_running(),
        "the lock was taken while held elsewhere"
    );

    Runtime::block(first.unlock()).unwrap();

    assert_eq!(
        waiting.join_with_timeout(Duration::from_secs(10)),
        Ok(Ok(()))
    );

    // Shared locks share
    Runtime::block(second.lock(LockKind::Shared)).unwrap();
    assert_eq!(Runtime::block(first.try_lock(LockKind::Shared)), Ok(()));
}

/// A wait for a lock can be cancelled
#[test]
fn a_lock_wait_can_be_cancelled() {
    let _ = Runtime::init();

    let path = TestPath::new("open-lock-cancel");
    fs::write(path.path(), b"").unwrap();

    let holder = Runtime::block(File::open(path.path())).unwrap();
    let other = Runtime::block(File::open(path.path())).unwrap();

    Runtime::block(holder.lock(LockKind::Exclusive)).unwrap();

    let waiting = Runtime::task(other.lock(LockKind::Exclusive)).spawn();
    std::thread::sleep(Duration::from_millis(30));

    waiting.clone().cancel();

    assert_eq!(
        waiting.join_with_timeout(Duration::from_secs(10)),
        Err(RuntimeError::Cancelled)
    );
}

/// Each entry says what it is, and a link says it is a link
#[test]
fn read_dir_says_what_each_entry_is() {
    let _ = Runtime::init();

    let dir = TestPath::new("listing-kinds");
    fs::create_dir_all(dir.path().join("sub")).unwrap();
    fs::write(dir.path().join("plain"), b"x").unwrap();
    std::os::unix::fs::symlink(dir.path().join("sub"), dir.path().join("link")).unwrap();

    let mut listed: Vec<(String, FileKind)> = Runtime::block(File::read_dir(dir.path()))
        .unwrap()
        .into_iter()
        .map(|entry| {
            let name = entry
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .into_owned();

            (name, entry.kind())
        })
        .collect();

    listed.sort();

    assert_eq!(
        listed,
        vec![
            (String::from("link"), FileKind::Symlink),
            (String::from("plain"), FileKind::File),
            (String::from("sub"), FileKind::Dir),
        ]
    );
}
