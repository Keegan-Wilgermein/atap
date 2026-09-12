//! Its own binary with a single test, since taking signals over
//! and handing them back is process-wide
//!
//! Nothing here sends these signals, only watches them, so a
//! released one can't end the test run

use atap::{Runtime, SigReleasePolicy, Signal, SignalKind, TaskHandle};
use std::{
    mem, ptr, thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Whether anything but the signal's own behaviour is installed
fn taken_over(kind: SignalKind) -> bool {
    let mut action: libc::sigaction = unsafe { mem::zeroed() };

    unsafe { libc::sigaction(kind.number(), ptr::null(), &mut action) };

    action.sa_sigaction != libc::SIG_DFL
}

/// Waits until a spawned task is parked, which is also when it has
/// taken its signal over
fn until_parked<T>(handle: &TaskHandle<T>) {
    let deadline = Instant::now() + PATIENCE;

    while handle.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    thread::sleep(Duration::from_millis(20));
}

/// `OnDrop` hands a signal back once nothing watches it, `Hold`
/// keeps it, and a second watcher keeps it while it lives
#[test]
fn a_signal_goes_back_when_its_last_watcher_does() {
    Runtime::init();

    let on_drop = SignalKind::Hangup;
    let held = SignalKind::WindowChange;

    assert!(!taken_over(on_drop), "nothing has touched it yet");
    assert!(!taken_over(held), "nor this one");

    // Two watchers, so the first going isn't the last
    let first = Runtime::task(
        Signal::wait(on_drop)
            .release_policy(SigReleasePolicy::OnDrop)
            .timeout(Duration::from_millis(100)),
    )
    .spawn();

    let second = Runtime::task(
        Signal::wait(on_drop)
            .release_policy(SigReleasePolicy::OnDrop)
            .timeout(PATIENCE),
    )
    .spawn();

    until_parked(&first);
    until_parked(&second);

    assert!(taken_over(on_drop), "watching takes it over whatever the policy");

    // The first gives up at its timeout and is let go of entirely
    let _ = first.take_with_timeout(PATIENCE);
    thread::sleep(Duration::from_millis(50));

    assert!(
        taken_over(on_drop),
        "the second watcher still has it, so it stays taken over",
    );

    // The last one out, handle and all
    second.cancel();

    let deadline = Instant::now() + PATIENCE;

    while taken_over(on_drop) && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    assert!(
        !taken_over(on_drop),
        "the last watcher going hands the signal back",
    );

    // Held is the default, and outlives its task
    let keeper = Runtime::task(Signal::wait(held).timeout(Duration::from_millis(100))).spawn();
    until_parked(&keeper);

    let _ = keeper.take_with_timeout(PATIENCE);
    thread::sleep(Duration::from_millis(50));

    assert!(
        taken_over(held),
        "a held signal stays taken over once its task has gone",
    );

    Signal::release(held).expect("releasing a real signal works");

    assert!(!taken_over(held), "and only a release hands it back");
}
