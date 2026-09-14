mod common;

use atap::{Process, Runtime};
use common::next_run;
use std::time::Duration;

/// How long a test waits for a child that ought to be quick
const PATIENCE: Duration = Duration::from_secs(20);

/// A repeat feeds its input to every run, not just the first
#[test]
fn a_repeat_feeds_every_run() {
    Runtime::init();

    let handle =
        Runtime::task(Process::output("/bin/cat", Process::NO_ARGS).input(b"hi".as_slice()))
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
