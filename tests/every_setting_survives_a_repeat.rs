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

/// A directory and an environment carry over to every run of
/// a repeat
#[test]
fn every_setting_survives_a_repeat() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "pwd; printf %s \"$V\""])
            .in_dir("/usr")
            .env([("V", "set")]),
    )
    .repeat()
    .every(Duration::from_millis(30))
    .count(2)
    .spawn();

    let mut runs = 0;

    while let Some(found) = next_run(&handle, PATIENCE) {
        let said = String::from_utf8_lossy(found.expect("sh must run").stdout()).to_string();

        assert_eq!(said, "/usr\nset", "run {runs} lost a setting, said {said:?}");

        runs += 1;
    }

    assert_eq!(runs, 2, "saw {runs} runs, not 2");
}
