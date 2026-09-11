use atap::{Process, Runtime, RuntimeError, TaskHandle};
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// How long a test waits for a child that ought to be quick
const PATIENCE: Duration = Duration::from_secs(20);

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
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::NotReady) => {
                thread::sleep(Duration::from_millis(1))
            }
            Err(_) => break,
        }
    }

    None
}

/// A repeat feeds its input to every run, not just the first
#[test]
fn a_repeat_feeds_every_run() {
    Runtime::init();

    let handle = Runtime::task(Process::output("/bin/cat", Process::NO_ARGS).input(b"hi".as_slice()))
        .repeat()
        .every(Duration::from_millis(30))
        .count(3)
        .spawn();

    let mut runs = 0;

    while let Some(found) = next_run(&handle, PATIENCE) {
        assert_eq!(
            found.expect("cat must run").stdout(),
            b"hi",
            "run {runs} was not fed"
        );

        runs += 1;
    }

    println!("saw {runs} fed runs against a count of 3");

    assert!(handle.is_finished(), "the series never reported finishing");
    assert_eq!(runs, 3, "saw {runs} runs, not 3");
}
