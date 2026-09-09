//! # Idle CPU Usage
//! What the runtime costs while it has nothing to do but wait
//!
//! A file of its own, so the only thing this process is doing
//! is the waiting being measured. Anything else sharing it
//! would be counted as the cost of idling

use atap::{Runtime, Sleep};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Waiting costs almost nothing, and says how much
///
/// ## Why it measures itself
/// Activity Monitor samples on its own schedule and rounds to
/// a tenth of a percent, so a short window at nearly zero is a
/// window it can easily show as nothing at all. `getrusage`
/// asks the kernel what this process has actually been charged
/// since it started, which is the same question without the
/// sampling
///
/// The pid is printed anyway, and the wait is long enough to
/// find the process and watch it — but the number below is the
/// answer, not the one on screen
///
/// ## What is being waited on
/// Sleeps long enough to be handed to sleep threads rather than
/// run on workers, so every one of them is a thread parked
/// inside a `kevent` call for the duration. What is left awake
/// is the reactor, the manager on its tick, and whichever
/// workers the floor keeps alive — and none of those should be
/// doing anything at all
#[test]
fn idle_cpu_usage() {
    Runtime::init();

    let tasks = 20;
    let waiting = Duration::from_secs(5);

    println!(
        "pid {} idling {} tasks for {:?}",
        std::process::id(),
        tasks,
        waiting,
    );

    // Given a moment to get everything parked, so the threads
    // starting up aren't charged to the idling
    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::spawn(Sleep::sleep(waiting, true)))
        .collect();

    thread::sleep(Duration::from_millis(200));

    let before = cpu_time();
    let started = Instant::now();

    // What each task says it slept for, which is not the window
    // being measured — the window starts once everything is
    // parked, so it is short of the wait by however long that
    // settling took
    let mut shortest = Duration::MAX;
    let mut longest = Duration::ZERO;

    for handle in handles {
        let slept = handle
            .join()
            .expect("every idling task finishes, rather than failing instantly");

        shortest = shortest.min(slept);
        longest = longest.max(slept);
    }

    let window = started.elapsed();
    let burnt = cpu_time() - before;

    // Against one core rather than all of them, since that is
    // what a single thread spinning would cost and spinning is
    // the thing worth catching
    let share = burnt.as_secs_f64() / window.as_secs_f64() * 100.0;

    println!(
        "{} tasks asked for {:?} and slept {:?} to {:?}, \
         burning {:?} of cpu over a {:?} window, {:.3}% of one core",
        tasks, waiting, shortest, longest, burnt, window, share,
    );

    // Late is the promise, early is a broken one. Worth
    // checking here because this is the one test that leaves
    // sleeps alone for long enough to drift
    assert!(
        shortest >= waiting,
        "a sleep asked for {:?} came back after {:?}, which is early",
        waiting,
        shortest,
    );

    // A thread spinning out the whole wait would be 100% of a
    // core. The floor here is well above what parked threads
    // and a manager ticking a hundred times actually cost, and
    // far below anything that is genuinely awake
    assert!(
        share < 5.0,
        "idling {} tasks over a {:?} window burnt {:?} of cpu, {:.3}% of a core, \
         so something was awake that should have been parked",
        tasks,
        window,
        burnt,
        share,
    );
}

/// Cpu time this process has been charged, user and system
///
/// Both halves, because a runtime that idles by hammering the
/// kernel costs just as much as one that idles by spinning in
/// userspace, and only the second of those shows up in user
/// time
fn cpu_time() -> Duration {
    let mut usage = unsafe { std::mem::zeroed::<libc::rusage>() };

    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return Duration::ZERO;
    }

    let seconds = |time: libc::timeval| {
        Duration::from_secs(time.tv_sec as u64) + Duration::from_micros(time.tv_usec as u64)
    };

    seconds(usage.ru_utime) + seconds(usage.ru_stime)
}
