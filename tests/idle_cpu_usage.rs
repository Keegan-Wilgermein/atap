//! # Idle CPU Usage
//! What the runtime costs while it has nothing to do but wait

use atap::{Runtime, Sleep};
use std::{
    thread,
    time::{Duration, Instant},
};

/// A runtime with every task parked in a long sleep uses
/// almost no cpu
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
        .map(|_| Runtime::task(Sleep::sleep(waiting, true)).spawn())
        .collect();

    thread::sleep(Duration::from_millis(200));

    let before = cpu_time();
    let started = Instant::now();

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

    // Against one core, since a single spinning thread is the
    // thing worth catching
    let share = burnt.as_secs_f64() / window.as_secs_f64() * 100.0;

    println!(
        "{} tasks asked for {:?} and slept {:?} to {:?}, \
         burning {:?} of cpu over a {:?} window, {:.3}% of one core",
        tasks, waiting, shortest, longest, burnt, window, share,
    );

    // No sleep came back early
    assert!(
        shortest >= waiting,
        "a sleep asked for {:?} came back after {:?}, which is early",
        waiting,
        shortest,
    );

    // A thread spinning out the whole wait would be 100% of a core
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
