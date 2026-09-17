mod common;

use atap::{Runtime, process::Process};
use common::next_run;
use std::time::Duration;

/// How long a test waits for a child that ought to be quick
const PATIENCE: Duration = Duration::from_secs(20);

/// A directory and an environment carry over to every run of
/// a repeat
#[test]
fn every_setting_survives_a_repeat() {
    let _ = Runtime::init();

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

        assert_eq!(
            said, "/usr\nset",
            "run {runs} lost a setting, said {said:?}"
        );

        runs += 1;
    }

    assert_eq!(runs, 2, "saw {runs} runs, not 2");
}
