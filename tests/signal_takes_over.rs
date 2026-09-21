//! Its own binary with a single test: it takes `SIGINT` over for
//! the whole process, which would otherwise end the test run

mod common;

use atap::{
    Runtime, RuntimeError,
    signal::{Signal, SignalKind},
};
use common::{taken_over, within};
use std::{
    process, thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Ctrl-C, which normally ends the program
const KIND: SignalKind = SignalKind::Interrupt;

/// Watching Ctrl-C takes it over, so sending it wakes the task
/// instead of ending the program, and releasing gives it back
#[test]
fn watching_an_interrupt_takes_it_over() {
    let _ = Runtime::init();

    assert!(!taken_over(KIND), "nothing has touched Ctrl-C yet");

    let waiting = Runtime::task(Signal::wait(KIND)).spawn();

    let deadline = Instant::now() + PATIENCE;

    while waiting.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    thread::sleep(Duration::from_millis(20));

    assert!(taken_over(KIND), "a wait on Ctrl-C takes it over");

    // The whole point: this would end the program if the signal were
    // still doing what it normally does
    Runtime::block(Signal::send(process::id() as i32, KIND)).expect("the signal must go");

    let count = waiting
        .take_with_timeout(PATIENCE)
        .expect("Ctrl-C must settle the task")
        .expect("and the task must succeed");

    assert_eq!(count, 1);
    println!("survived a Ctrl-C and read it as {count} delivery");

    // Held by default, so it stays taken over with nothing watching
    thread::sleep(Duration::from_millis(20));
    assert!(
        taken_over(KIND),
        "the default is to keep a signal once watched"
    );

    Signal::release(KIND).expect("releasing a real signal works");

    assert!(!taken_over(KIND), "releasing hands the signal back");

    // The two nobody can take over
    assert_eq!(
        within(Signal::wait(SignalKind::Other(libc::SIGKILL)), PATIENCE),
        Err(RuntimeError::BadSignal),
    );

    assert_eq!(
        within(Signal::wait(SignalKind::Other(libc::SIGSTOP)), PATIENCE),
        Err(RuntimeError::BadSignal),
    );
}
