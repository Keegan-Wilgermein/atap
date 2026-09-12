//! Its own binary with a single test, since watching a signal is
//! process-wide
//!
//! The signals it waits on without ever sending are ones this test
//! never sends, so taking them over can't affect the test run

use atap::{Runtime, RuntimeError, Signal, SignalKind, TaskHandle};
use std::{
    process, thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Waits until a spawned task is parked
fn until_parked<T>(handle: &TaskHandle<T>) {
    let deadline = Instant::now() + PATIENCE;

    while handle.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    thread::sleep(Duration::from_millis(20));
}

/// A wait gives up at its timeout, can be cancelled while parked,
/// and two tasks on one signal both wake
#[test]
fn a_wait_gives_up_when_asked_to() {
    Runtime::init();

    // A timeout on a signal nothing sends
    let quiet = SignalKind::Quit;

    let started = Instant::now();
    let got = Runtime::block(Signal::wait(quiet).timeout(Duration::from_millis(100)));
    let took = started.elapsed();

    assert_eq!(got, Err(RuntimeError::TimedOut));
    assert!(took >= Duration::from_millis(100), "gave up early, after {took:?}");
    assert!(took < Duration::from_secs(2), "gave up late, after {took:?}");

    // A spawned one times out the same way, parked the whole time
    let spawned = Runtime::task(Signal::wait(quiet).timeout(Duration::from_millis(100))).spawn();

    assert_eq!(
        spawned.take_with_timeout(PATIENCE).expect("the timeout settles it"),
        Err(RuntimeError::TimedOut),
    );

    // Cancelling a parked wait settles it at once
    let waiting = Runtime::task(Signal::wait(SignalKind::Other(libc::SIGALRM))).spawn();
    until_parked(&waiting);

    assert!(waiting.is_running(), "the wait is parked");

    waiting.clone().cancel();

    assert_eq!(
        waiting.join_with_timeout(PATIENCE),
        Err(RuntimeError::Cancelled),
    );

    // Two tasks on one signal both wake. `SIGCHLD` is already
    // ignored by default, so taking it over changes nothing here
    let shared = SignalKind::Child;

    let first = Runtime::task(Signal::wait(shared).timeout(PATIENCE)).spawn();
    let second = Runtime::task(Signal::wait(shared).timeout(PATIENCE)).spawn();

    until_parked(&first);
    until_parked(&second);

    Runtime::block(Signal::send(process::id() as libc::pid_t, shared)).expect("the signal must go");

    assert!(
        first
            .take_with_timeout(PATIENCE)
            .expect("the first task settles")
            .is_ok(),
        "the first of two watchers woke",
    );

    assert!(
        second
            .take_with_timeout(PATIENCE)
            .expect("the second task settles")
            .is_ok(),
        "the second of two watchers woke too, rather than losing its watch",
    );
}
