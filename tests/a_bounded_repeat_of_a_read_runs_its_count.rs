use atap::{File, Runtime, RuntimeError, TaskHandle};
use std::fs;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use std::time::Instant;

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

/// A counted repeat of a read runs exactly its count, then finishes
#[test]
fn a_bounded_repeat_of_a_read_runs_its_count() {
    Runtime::init();

    let file = TestPath::new("counted");
    fs::write(file.path(), b"counted").unwrap();

    let handle = Runtime::task(File::read(file.path()))
        .repeat()
        .every(Duration::from_millis(30))
        .count(3)
        .spawn();

    let mut runs = 0;

    while let Some(read) = next_run(&handle, Duration::from_secs(10)) {
        assert_eq!(
            read.expect("read failed").as_slice(),
            b"counted".as_slice(),
            "wrong contents"
        );

        runs += 1;
    }

    println!("saw {} runs against a count of 3", runs);

    assert!(handle.is_finished(), "the series never reported finishing");
    assert_eq!(runs, 3, "saw {} runs, not 3", runs);

    assert!(!handle.is_failed(), "running out is not failing");
}
