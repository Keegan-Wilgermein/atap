//! Its own binary with a single test, since a signal's handler and
//! its count belong to the whole process, and two tests watching
//! one signal at once would read each other's deliveries

mod common;

use atap::{Runtime, Signal, SignalKind};
use common::{send_signal, until_started};
use std::{
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// The signal this binary uses
const KIND: SignalKind = SignalKind::User1;

/// A signal wakes the task waiting for it, spawned or blocking,
/// and nothing is lost when two arrive together
#[test]
fn a_signal_wakes_the_task_waiting_for_it() {
    let _ = Runtime::init();

    // Spawned: parked with no thread until the signal arrives
    let waiting = Runtime::task(Signal::wait(KIND)).spawn();
    until_started(&waiting, PATIENCE);

    assert!(
        waiting.is_running(),
        "a wait on a signal reads as running, got {:?}",
        waiting.state(),
    );

    send_signal(KIND);

    let count = waiting
        .take_with_timeout(PATIENCE)
        .expect("the signal must settle the task")
        .expect("and the task must succeed");

    assert_eq!(count, 1, "one signal is one delivery");

    // The program is still alive, which means the signal was taken
    // over rather than left to end it
    println!("took over {KIND:?} and survived it");

    // Blocking: the same wait on the calling thread
    let sender = thread::spawn(|| {
        thread::sleep(Duration::from_millis(50));
        send_signal(KIND);
    });

    let count = Runtime::block(Signal::wait(KIND).timeout(PATIENCE)).expect("the blocking wait");
    assert!(count >= 1, "the blocking wait saw {count} deliveries");

    sender.join().unwrap();

    // Two deliveries, one after the other, are both reported. Spaced,
    // since the kernel drops a second signal that arrives while the
    // same one is still pending
    let waiting = Runtime::task(Signal::wait(KIND))
        .repeat()
        .every(Duration::from_millis(10))
        .spawn();

    until_started(&waiting, PATIENCE);

    let mut total = 0;

    // One at a time, each read before the next is sent: a slot holds
    // the latest output, so a report nobody takes is thrown away by
    // the run that follows it
    for _ in 0..2 {
        send_signal(KIND);

        let until = Instant::now() + PATIENCE;

        // `maybe_take` rather than `take`, which would wait for the
        // next run rather than poll
        while Instant::now() < until {
            if let Ok(count) = waiting.maybe_take() {
                total += count.expect("every run succeeds");
                break;
            }

            thread::sleep(Duration::from_millis(1));
        }
    }

    waiting.cancel();

    assert_eq!(
        total, 2,
        "both deliveries were reported, across however many runs it took",
    );
}
