use atap::{Process, Runtime};
use std::thread;
use std::time::Duration;
use std::time::Instant;

/// A run blocked writing to a child that never reads can
/// still be cancelled
#[test]
fn a_run_blocked_on_input_can_still_be_cancelled() {
    Runtime::init();

    let fed = vec![b'z'; 4 * 1024 * 1024];

    let handle =
        Runtime::task(Process::run("/bin/sleep", ["30"]).input(fed.as_slice())).spawn();

    // Long enough that the pipe has filled and the thread is
    // waiting to write
    thread::sleep(Duration::from_millis(500));

    println!("cancelling a run wedged on a child that never reads");
    let started = Instant::now();

    handle.clone().cancel();
    let _ = handle.wait();

    assert!(
        started.elapsed() < Duration::from_secs(15),
        "a cancel must reach a task waiting on room to write, took {:?}",
        started.elapsed()
    );

    assert!(
        handle.is_cancelled(),
        "the task must settle cancelled, was {:?}",
        handle.state()
    );
}
