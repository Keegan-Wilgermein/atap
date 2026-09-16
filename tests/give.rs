//! Give tests
//!
//! Tasks spawned with `wait_for`, and what each give does to them.
//! Each test checks only the values its own tasks come back with,
//! so they share a binary and run side by side on purpose

mod common;

use atap::{Compute, JoinPolicy, Runtime, RuntimeError, Sleep, SleepMode, TaskHandle, Waiting};
use common::settles;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Long enough to be sure nothing more is coming
const QUIET: Duration = Duration::from_millis(100);

/// A give starts one run with its value, and nothing more
#[test]
fn a_give_starts_one_run_with_its_value() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let doubler = Runtime::task(Compute::compute(move |value: i32| {
        let _ = ran.send(value);
        value * 2
    }))
    .wait_for::<i32>()
    .spawn();

    doubler.give(7).expect("the give was refused");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(7));
    assert_eq!(doubler.join_with_timeout(PATIENCE), Ok(14));
    assert!(
        runs.recv_timeout(QUIET).is_err(),
        "one give ran more than once"
    );
}

/// Nothing runs before the first give
#[test]
fn nothing_runs_before_the_first_give() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let handle = Runtime::task(Compute::compute(move |value: u8| {
        counted.fetch_add(1, Ordering::Relaxed);
        value
    }))
    .wait_for::<u8>()
    .spawn();

    thread::sleep(QUIET);

    assert_eq!(
        runs.load(Ordering::Relaxed),
        0,
        "a task waiting for a give ran without one"
    );
    assert!(
        handle.is_pending(),
        "a task never given anything read {:?}",
        handle.state()
    );
    assert!(
        handle.is_waiting(),
        "a task never given anything wasn't waiting"
    );

    assert_eq!(
        handle.join_with_timeout(Duration::from_millis(30)),
        Err(RuntimeError::NotReady),
    );
}

/// A task takes gives one after another until it is cancelled
#[test]
fn a_task_takes_gives_until_it_is_cancelled() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .spawn();

    for value in 0..200 {
        assert!(
            settles(|| handle.is_waiting()),
            "give {} found the task still busy",
            value
        );

        handle.give(value).expect("a give was refused");

        assert_eq!(
            runs.recv_timeout(PATIENCE),
            Ok(value),
            "give {} ran with another value",
            value
        );
    }

    assert!(
        !handle.is_finished(),
        "a task with no count finished on its own"
    );

    handle.clone().cancel();

    assert_eq!(handle.give(1), Err(RuntimeError::Cancelled));
}

/// A count of gives finishes the task once the last one's run is
/// over, and keeps its last output
#[test]
fn a_count_of_gives_finishes_the_task() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value * 10
    }))
    .wait_for::<u32>()
    .count(3)
    .spawn();

    for value in 1..=3 {
        assert!(
            settles(|| handle.is_waiting()),
            "give {} found the task still busy",
            value
        );

        handle.give(value).expect("a counted give was refused");

        assert_eq!(runs.recv_timeout(PATIENCE), Ok(value));
    }

    assert!(
        settles(|| handle.is_finished()),
        "the task never finished after its last give"
    );
    assert_eq!(handle.give(4), Err(RuntimeError::Finished));
    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Ok(30),
        "the last output didn't stay readable"
    );
}

/// Gives that land while a run is under way leave one more run,
/// with the newest value
#[test]
fn gives_during_a_run_leave_one_more_run_with_the_newest_value() {
    let _ = Runtime::init();

    let (started, starts) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = started.send(value);

        // The first run holds on until the test has given more
        if value == 1 {
            let _ = released.lock().unwrap().recv_timeout(PATIENCE);
        }

        value
    }))
    .wait_for::<u32>()
    .spawn();

    handle.give(1).expect("the first give was refused");

    assert_eq!(starts.recv_timeout(PATIENCE), Ok(1));

    for value in 2..=5 {
        handle.give(value).expect("a give mid run was refused");
    }

    release.send(()).expect("the first run went away");

    assert_eq!(
        starts.recv_timeout(PATIENCE),
        Ok(5),
        "the run a burst owed didn't get the newest value",
    );

    assert!(
        starts.recv_timeout(QUIET).is_err(),
        "a burst of gives owed more than one run"
    );
}

/// Gives from many threads at once never overlap runs, and the last
/// value given always runs
#[test]
fn gives_from_many_threads_never_overlap_and_the_last_one_runs() {
    let _ = Runtime::init();

    const LAST: u64 = u64::MAX;

    let running = Arc::new(AtomicUsize::new(0));
    let overlapped = Arc::new(AtomicBool::new(false));
    let last = Arc::new(Mutex::new(None));

    let handle = {
        let running = Arc::clone(&running);
        let overlapped = Arc::clone(&overlapped);
        let last = Arc::clone(&last);

        Runtime::task(Compute::compute(move |value: u64| {
            if running.fetch_add(1, Ordering::SeqCst) != 0 {
                overlapped.store(true, Ordering::SeqCst);
            }

            thread::sleep(Duration::from_micros(50));
            *last.lock().unwrap() = Some(value);

            running.fetch_sub(1, Ordering::SeqCst);
            value
        }))
        .wait_for::<u64>()
        .spawn()
    };

    let givers: Vec<_> = (0..16u64)
        .map(|giver| {
            let handle = handle.clone();

            thread::spawn(move || {
                for index in 0..200u64 {
                    handle
                        .give(giver * 1_000 + index)
                        .expect("a give from a thread was refused");

                    if index % 16 == 0 {
                        thread::yield_now();
                    }
                }
            })
        })
        .collect();

    for giver in givers {
        giver.join().expect("a giving thread went down");
    }

    handle.give(LAST).expect("the last give was refused");

    assert!(
        settles(|| *last.lock().unwrap() == Some(LAST)),
        "the last value given never ran, the latest run saw {:?}",
        last.lock().unwrap(),
    );

    assert!(
        !overlapped.load(Ordering::SeqCst),
        "two runs of one task overlapped"
    );
}

/// A give to a cancelled task is refused
#[test]
fn a_give_to_a_cancelled_task_is_refused() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();

    handle.clone().cancel();

    assert_eq!(handle.give(1), Err(RuntimeError::Cancelled));
    assert!(
        handle.is_cancelled(),
        "a cancelled waiting task read {:?}",
        handle.state()
    );
    assert!(
        handle.is_finished(),
        "a cancelled waiting task wasn't finished"
    );
}

/// A task that takes nothing can wait for a value of any type
#[test]
fn a_task_that_takes_nothing_waits_for_anything() {
    let _ = Runtime::init();

    let asked = Duration::from_millis(2);

    let handle = Runtime::task(Sleep::sleep(asked).mode(SleepMode::Relaxed))
        .wait_for::<String>()
        .spawn();

    handle
        .give(String::from("anything"))
        .expect("the give was refused");

    let slept = handle
        .join_with_timeout(PATIENCE)
        .expect("the sleep never ran");

    assert!(
        slept >= asked,
        "a sleep given a string slept {:?} of {:?}",
        slept,
        asked
    );
}

/// A compute whose closure takes nothing waits for `()`
#[test]
fn a_compute_that_takes_nothing_waits_for_unit() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| 5))
        .wait_for::<()>()
        .spawn();

    handle.give(()).expect("the give was refused");

    assert_eq!(handle.join_with_timeout(PATIENCE), Ok(5));
}

/// An output taken between gives is replaced by the next give's run
#[test]
fn taking_between_gives_leaves_room_for_the_next_output() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
        value
    }))
    .wait_for::<u32>()
    .spawn();

    for value in 1..=2 {
        assert!(
            settles(|| handle.is_waiting()),
            "give {} found the task still busy",
            value
        );

        handle.give(value).expect("a give was refused");

        assert_eq!(runs.recv_timeout(PATIENCE), Ok(value));
        assert_eq!(
            handle.take_with_timeout(PATIENCE),
            Ok(value),
            "give {} couldn't be taken",
            value
        );
    }
}

/// `after` is waited out after every give
#[test]
fn after_is_waited_out_after_every_give() {
    let _ = Runtime::init();

    let delay = Duration::from_millis(30);
    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |given: Instant| {
        let _ = ran.send(given.elapsed());
    }))
    .wait_for::<Instant>()
    .after(delay)
    .spawn();

    for give in 0..2 {
        assert!(
            settles(|| handle.is_waiting()),
            "give {} found the task still busy",
            give
        );

        handle.give(Instant::now()).expect("a give was refused");

        let waited = runs
            .recv_timeout(PATIENCE)
            .expect("a delayed give never ran");

        assert!(
            waited >= delay,
            "give {} ran {:?} after it, inside its {:?} delay",
            give,
            waited,
            delay
        );
    }
}

/// Dropping every handle to a task never given anything lets it go
#[test]
fn dropping_every_handle_to_a_waiting_task_lets_it_go() {
    let _ = Runtime::init();

    let held = Arc::new(());
    let captured = Arc::clone(&held);

    let handle = Runtime::task(Compute::compute(move |value: u8| {
        let _ = Arc::strong_count(&captured);
        value
    }))
    .wait_for::<u8>()
    .spawn();

    let copy = handle.clone();

    drop(handle);

    thread::sleep(Duration::from_millis(20));

    assert_eq!(
        Arc::strong_count(&held),
        2,
        "the task went while a handle could still give to it"
    );

    drop(copy);

    assert!(
        settles(|| Arc::strong_count(&held) == 1),
        "the task outlived every handle that could give to it",
    );
}

/// The last handle going mid run lets the task go once the run is
/// over, and not before
#[test]
fn the_last_handle_going_mid_run_lets_the_task_go_after_the_run() {
    let _ = Runtime::init();

    let held = Arc::new(());
    let captured = Arc::clone(&held);
    let (started, starts) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);

    let handle = Runtime::task(Compute::compute(move |value: u8| {
        let _ = Arc::strong_count(&captured);
        let _ = started.send(());
        let _ = released.lock().unwrap().recv_timeout(PATIENCE);
        value
    }))
    .wait_for::<u8>()
    .spawn();

    handle.give(1).expect("the give was refused");
    starts
        .recv_timeout(PATIENCE)
        .expect("the run never started");

    drop(handle);

    thread::sleep(Duration::from_millis(20));

    assert_eq!(
        Arc::strong_count(&held),
        2,
        "the task went while its run was still under way"
    );

    let _ = release.send(());

    assert!(
        settles(|| Arc::strong_count(&held) == 1),
        "the task outlived its last run and every handle that could give to it",
    );
}

/// A handle given as data keeps its task alive until the task that
/// holds it is done with it
#[test]
fn a_handle_given_as_data_lives_as_long_as_the_task_holding_it() {
    let _ = Runtime::init();

    let inner = Runtime::task(Compute::compute(|()| 11)).spawn();

    let outer = Runtime::task(Compute::compute(|inner: TaskHandle<i32>| {
        inner.join_with_timeout(PATIENCE)
    }))
    .wait_for::<TaskHandle<i32>>()
    .spawn();

    // The only handle the test had goes in as data
    outer.give(inner).expect("the handle couldn't be given");

    assert_eq!(outer.join_with_timeout(PATIENCE), Ok(Ok(11)));
}

/// What a task holds onto through its own handle
#[derive(Clone)]
struct Holding {
    /// The task's own handle, given back to it
    handle: Option<TaskHandle<u8, Waiting<Holding>>>,

    /// Counts how long what was given lives, which is all it is for
    _probe: Arc<()>,
}

/// A task given its own handle as data lets go of it once cancelled,
/// so neither keeps the other alive
#[test]
fn a_task_given_its_own_handle_lets_go_of_it_once_cancelled() {
    let _ = Runtime::init();

    let probe = Arc::new(());
    let (ran, runs) = mpsc::channel();

    let handle = Runtime::task(Compute::compute(move |holding: Holding| {
        let _ = ran.send(holding.handle.is_some());
        0u8
    }))
    .wait_for::<Holding>()
    .spawn();

    handle
        .give(Holding {
            handle: Some(handle.clone()),
            _probe: Arc::clone(&probe),
        })
        .expect("the task couldn't be given its own handle");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(true));

    handle.clone().cancel();
    drop(handle);

    assert!(
        settles(|| Arc::strong_count(&probe) == 1),
        "what the task was given outlived its cancel",
    );
}

/// A waiting task that panics takes no more gives
#[test]
fn a_waiting_task_that_panics_takes_no_more_gives() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|value: u8| -> u8 {
        if value == 0 {
            panic!("this give is meant to take the task down");
        }

        value
    }))
    .wait_for::<u8>()
    .spawn();

    handle.give(0).expect("the give was refused");

    assert_eq!(
        handle.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );
    assert_eq!(handle.give(1), Err(RuntimeError::TaskFailed));
}

/// `join_first` races handles to waiting tasks like any others
#[test]
fn join_first_races_waiting_handles() {
    let _ = Runtime::init();

    let first = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();

    let second = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();

    let given = second.id();

    second.give(9).expect("the give was refused");

    let (winner, _) = Runtime::join_first([first, second], JoinPolicy::Cancel);

    assert_eq!(
        winner.id(),
        given,
        "the task that was never given anything won"
    );
    assert_eq!(winner.join_with_timeout(PATIENCE), Ok(9));
}
