//! Its own binary with a single test, since a signal's handler and
//! its count belong to the whole process

use atap::{Runtime, Signal, SignalKind};
use std::{
    process, thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// The signal this binary uses
const KIND: SignalKind = SignalKind::User2;

/// How many to send
const SENDS: u32 = 3;

/// Sends the signal to this program
fn send() {
    Runtime::block(Signal::send(process::id() as libc::pid_t, KIND)).expect("the signal must go");
}

/// A repeating wait reports every delivery, including ones that
/// land between its runs
#[test]
fn a_repeating_wait_reports_every_delivery() {
    Runtime::init();

    // Spaced, so each run's output is read before the next replaces
    // it
    let handle = Runtime::task(Signal::wait(KIND))
        .repeat()
        .every(Duration::from_millis(10))
        .spawn();

    let deadline = Instant::now() + PATIENCE;

    while handle.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    thread::sleep(Duration::from_millis(20));

    let mut total = 0;

    for _ in 0..SENDS {
        send();

        // Long enough for the run to publish and be read, and for the
        // next run to be waiting again
        let until = Instant::now() + Duration::from_secs(2);

        // `maybe_take` rather than `take`, which would wait for the
        // next run rather than poll, and wait for ever if one were
        // lost
        while Instant::now() < until {
            if let Ok(count) = handle.maybe_take() {
                total += count.expect("every run succeeds");
                break;
            }

            thread::sleep(Duration::from_millis(1));
        }

        thread::sleep(Duration::from_millis(50));
    }

    handle.cancel();

    println!("{SENDS} signals, {total} deliveries reported");

    assert_eq!(
        total, SENDS,
        "a repeating wait lost a delivery: {total} of {SENDS} reported",
    );
}
