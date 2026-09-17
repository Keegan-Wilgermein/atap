mod common;

use atap::{Runtime, fs::File};
use common::{TestPath, next_run};
use std::fs;
use std::time::Duration;

/// A counted repeat of a read runs exactly its count, then finishes
#[test]
fn a_bounded_repeat_of_a_read_runs_its_count() {
    let _ = Runtime::init();

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
