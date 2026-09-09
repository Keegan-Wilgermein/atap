//! # Manager teardown
//! What happens to everything depending on the manager once it
//! gives up for good
//!
//! A file of its own, and it has to be: this kills the manager
//! permanently, and nothing sharing the process with it would
//! work again afterwards
//!
//! #### Note
//! Six panics are printed as they unwind. That is the test
//! working — it takes one more than the restart limit to make
//! the supervisor stop trying

use atap::{Runtime, RuntimeError, Sleep};
use std::{thread, time::Duration};

/// A manager that gives up doesn't strand what depended on it
///
/// ## What is being separated here
/// Three kinds of repeat depend on the manager to different
/// degrees, and only one of them is actually stranded when it
/// goes:
///
/// - `repeating` never needed it. It puts itself back on the
///   pool, which is still running
/// - `repeat_every` heals itself. It finds the queue closed
///   when it goes to wait out its interval and ends its own
///   series then and there
/// - `every` cannot do either. Its slot is held for the life of
///   the schedule rather than the life of a run, and the only
///   thing that would ever look at it again was a timer on the
///   queue that just closed
///
/// So the schedule is the one that needs writing off by hand,
/// and this is the test that says so
#[test]
fn a_manager_that_gives_up_strands_nothing() {
    Runtime::init();

    let quick = || Sleep::sleep(Duration::from_nanos(1), true);
    let interval = Duration::from_millis(20);

    let before = Runtime::workers();

    let schedule = Runtime::every(interval, quick());
    let timed = Runtime::repeat_every(interval, quick());
    let looping = Runtime::repeating(quick());

    // Running properly before any of this, so a settled handle
    // below is the teardown doing it rather than a schedule
    // that never got going
    thread::sleep(Duration::from_millis(100));

    assert!(
        !schedule.ready() || schedule.maybe_join().is_ok(),
        "the schedule settled before the manager was touched",
    );

    // More than the restart limit, so the supervisor runs out
    // of patience and closes the queue for good
    Runtime::inject_manager_faults(16);

    // Six deaths and their backoffs come to a little over two
    // hundred milliseconds
    thread::sleep(Duration::from_secs(1));

    // Both of the timed repeats are stranded once the queue
    // goes: the schedule always, and the timed repeat whenever
    // it was between runs rather than inside one, which is most
    // of the time. Neither has anything left anywhere that
    // would start it again
    //
    // Drained first, so anything readable afterwards is a run
    // that started after the manager had gone
    let _ = schedule.clone().take();
    let _ = timed.clone().take();

    thread::sleep(Duration::from_millis(200));

    assert_eq!(
        schedule.clone().take(),
        Err(RuntimeError::AlreadyTaken),
        "a schedule kept starting runs after the queue driving it had closed",
    );

    assert_eq!(
        timed.clone().take(),
        Err(RuntimeError::AlreadyTaken),
        "a timed repeat kept running after the queue it waits on had closed",
    );

    // Settled rather than left hanging, so a listener blocked
    // on either gets an answer instead of waiting for the life
    // of the process
    assert!(
        schedule.ready() && timed.ready(),
        "something was left unsettled with nothing able to run it, pool {:?}",
        Runtime::workers(),
    );

    // The pool outlives its manager, which is the whole reason
    // the manager is allowed to die. It stops adapting, it does
    // not stop working
    let slept = Runtime::spawn(Sleep::sleep(Duration::from_millis(10), false))
        .join()
        .expect("the pool still runs tasks with no manager at all");

    looping.cancel();
    drop(schedule);
    drop(timed);

    // Every slot back. This is the assertion that actually
    // catches it — a stranded task still reads perfectly well
    // through a handle that is holding it alive, and only says
    // so once that handle has gone and the slot doesn't follow
    assert!(
        settles(|| Runtime::workers().live <= before.live),
        "the manager gave up holding {} live tasks, up from {}",
        Runtime::workers().live,
        before.live,
    );

    println!(
        "manager gave up, schedule settled, pool still ran a task in {slept:?}, \
         live back to {}",
        Runtime::workers().live,
    );
}

/// Waits for something to become true, with a cap
///
/// Cleanup after the manager goes is spread across whichever
/// runs happen to be in flight, so none of it lands at the
/// moment the manager dies
fn settles(mut condition: impl FnMut() -> bool) -> bool {
    let waited = std::time::Instant::now();

    while waited.elapsed() < Duration::from_secs(5) {
        if condition() {
            return true;
        }

        thread::sleep(Duration::from_millis(10));
    }

    condition()
}
