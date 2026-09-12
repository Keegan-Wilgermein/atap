//! Its own binary with a single test: it takes `SIGINT` over for
//! the whole process, which would otherwise end the test run

use atap::{Runtime, RuntimeError, Signal, SignalKind};
use std::{
    mem, process, ptr, thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Ctrl-C, which normally ends the program
const KIND: SignalKind = SignalKind::Interrupt;

/// Whether anything but the signal's own behaviour is installed
fn taken_over(kind: SignalKind) -> bool {
    let mut action: libc::sigaction = unsafe { mem::zeroed() };

    unsafe { libc::sigaction(kind.number(), ptr::null(), &mut action) };

    action.sa_sigaction != libc::SIG_DFL
}

/// Watching Ctrl-C takes it over, so sending it wakes the task
/// instead of ending the program, and releasing gives it back
#[test]
fn watching_an_interrupt_takes_it_over() {
    Runtime::init();

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
    Runtime::block(Signal::send(process::id() as libc::pid_t, KIND)).expect("the signal must go");

    let count = waiting
        .take_with_timeout(PATIENCE)
        .expect("Ctrl-C must settle the task")
        .expect("and the task must succeed");

    assert_eq!(count, 1);
    println!("survived a Ctrl-C and read it as {count} delivery");

    // Held by default, so it stays taken over with nothing watching
    thread::sleep(Duration::from_millis(20));
    assert!(taken_over(KIND), "the default is to keep a signal once watched");

    Signal::release(KIND).expect("releasing a real signal works");

    assert!(!taken_over(KIND), "releasing hands the signal back");

    // The two nobody can take over
    assert_eq!(
        Runtime::block(Signal::wait(SignalKind::Other(libc::SIGKILL)).timeout(PATIENCE)),
        Err(RuntimeError::BadSignal),
    );

    assert_eq!(
        Runtime::block(Signal::wait(SignalKind::Other(libc::SIGSTOP)).timeout(PATIENCE)),
        Err(RuntimeError::BadSignal),
    );
}
