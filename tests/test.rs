use atap::{Runtime, RuntimeError, Sleep, TaskHandle, TaskState};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Prints how a blocking precise sleep compares to `thread::sleep`
#[test]
fn sleep_accuracy_vs_std_blocking() {
    Runtime::init();

    let duration = Duration::from_secs(1);

    let handle = thread::spawn(move || {
        println!("Running std ...\n");
        let start = Instant::now();
        thread::sleep(duration);
        let elapsed = start.elapsed();
        println!("std slept for: {:?}", elapsed);

        elapsed
    });

    println!("Running atap ...");
    let result = Runtime::block(Sleep::sleep(duration, true));

    println!("Result: {:?}", result);

    let std = handle.join().unwrap();

    println!("\nDiff: {:?}", std - result);
    println!(
        "std error:{:?}\natap error:{:?}\n",
        std - duration,
        result - duration
    )
}

/// Blocking sleeps on several threads at once
#[test]
fn sleep_multi_threaded_blocking() {
    Runtime::init();

    let threads = 3;

    (1..=threads).into_iter().for_each(|i| {
        thread::spawn(move || {
            let duration = Duration::from_secs(i);

            let time = Runtime::block(Sleep::sleep(duration, i % 2 == 0));

            let error = time - duration;
            println!("Thread {} slept for {:?}\n{:?} error\n", i, time, error);
        });
    });

    Runtime::block(Sleep::sleep(Duration::from_secs(threads + 2), false));
}

/// A single spawned sleep joins
#[test]
fn single_spawned_task() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(2), true)).spawn();

    if let Ok(time) = handle.join() {
        println!("atap slept for: {:?}", time);
    } else {
        println!("Failed");
    };
}

/// Two handles to one task both read the same output
#[test]
fn duplicated_handles_both_join() {
    Runtime::init();

    let duration = Duration::from_millis(200);

    let first = Runtime::task(Sleep::sleep(duration, false)).spawn();
    let second = first.clone();

    let one = first.join().expect("first listener");
    let two = second.join().expect("second listener");

    println!("first got {:?}, second got {:?}", one, two);

    assert_eq!(one, two, "both listeners read the same output");
    assert!(one >= duration, "the task actually ran");
}

/// A take leaves other handles reading `AlreadyTaken`
#[test]
fn take_invalidates_other_handles() {
    Runtime::init();

    let first = Runtime::task(Sleep::sleep(Duration::from_millis(200), false)).spawn();
    let second = first.clone();

    let taken = first.take().expect("the value moves out once");
    println!("took {:?}", taken);

    assert_eq!(
        second.join(),
        Err(RuntimeError::AlreadyTaken),
        "the second listener finds the value gone rather than blocking",
    );
}

/// A cancelled task reads `Cancelled` through every handle
#[test]
fn cancelled_task_is_unreadable() {
    Runtime::init();

    let first = Runtime::task(Sleep::sleep(Duration::from_secs(1), false)).spawn();
    let second = first.clone();

    first.cancel();

    assert_eq!(
        second.join(),
        Err(RuntimeError::Cancelled),
        "a cancelled task hands nothing out",
    );
}

/// `maybe_join` says why there is nothing to read
#[test]
fn maybe_join_says_why_rather_than_just_nothing() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(1), false)).spawn();
    let watcher = handle.clone();

    assert_eq!(
        watcher.maybe_join(),
        Err(RuntimeError::NotReady),
        "a task still running hasn't failed, it just isn't finished",
    );

    handle.cancel();

    assert_eq!(
        watcher.maybe_join(),
        Err(RuntimeError::Cancelled),
        "a cancelled task says so rather than looking unfinished",
    );
}

/// A timed out join leaves the handle usable
#[test]
fn join_with_timeout_gives_up_without_giving_up_the_handle() {
    Runtime::init();

    let duration = Duration::from_secs(1);
    let handle = Runtime::task(Sleep::sleep(duration, false)).spawn();

    assert_eq!(
        handle.join_with_timeout(Duration::from_millis(50)),
        Err(RuntimeError::NotReady),
        "nowhere near long enough, and it says so",
    );

    // Borrowed rather than consumed, so the task is still there
    let slept = handle
        .join_with_timeout(Duration::from_secs(5))
        .expect("the second wait is long enough");

    println!("gave up once, then waited and got {:?}", slept);

    assert!(
        slept >= duration,
        "the task came back with {:?} for a {:?} sleep",
        slept,
        duration,
    );
}

/// A back to back repeat keeps running until it is cancelled
#[test]
fn repeating_runs_until_cancelled() {
    Runtime::init();

    let wanted = 20;
    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(5), false)).repeat().spawn();

    // A take that succeeds is a run that happened
    for _ in 0..wanted {
        take_a_run(&handle);
    }

    handle.clone().cancel();

    assert_eq!(
        handle.clone().take(),
        Err(RuntimeError::Cancelled),
        "a cancelled series hands nothing out",
    );

    // Long enough for several more runs, had any been coming
    thread::sleep(Duration::from_millis(100));

    assert_eq!(
        handle.take(),
        Err(RuntimeError::Cancelled),
        "the series carried on after being cancelled",
    );

    println!("{} runs through one handle, then cancelled", wanted);
}

/// A back to back repeat never overlaps its runs
#[test]
fn repeating_finishes_a_run_before_the_next() {
    Runtime::init();

    let duration = Duration::from_millis(50);
    let runs: u32 = 5;

    let handle = Runtime::task(Sleep::sleep(duration, false)).repeat().spawn();

    // One on its own first, so the clock starts at the end of a run
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // Four whole runs between the first take and the last
    let floor = duration * (runs - 1);

    println!("{} runs of {:?} took {:?}, floor {:?}", runs, duration, elapsed, floor);

    assert!(
        elapsed >= floor,
        "{} runs of {:?} took {:?}, so they were overlapping",
        runs,
        duration,
        elapsed,
    );
}

/// A repeat with a gap waits out the gap between runs
#[test]
fn repeat_every_waits_between_runs() {
    Runtime::init();

    let interval = Duration::from_millis(50);
    let runs: u32 = 5;

    // A task with nothing in it, so what is measured is the gap
    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn();

    // One on its own first, so the clock starts at the end of a run
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // Four whole gaps between the first take and the last
    let floor = interval * (runs - 1);

    println!(
        "{} runs {:?} apart took {:?}, floor {:?}",
        runs, interval, elapsed, floor,
    );

    assert!(
        elapsed >= floor,
        "{} runs {:?} apart took only {:?}",
        runs,
        interval,
        elapsed,
    );
}

/// `at_rate` starts its runs no closer together than the period
#[test]
fn every_starts_runs_on_the_interval() {
    Runtime::init();

    let interval = Duration::from_millis(50);
    let runs: u32 = 5;

    // A task with nothing in it, so what is measured is the clock
    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).at_rate(interval).spawn();

    // One on its own first, so the clock starts at the end of a run
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // Four whole periods between the first take and the last
    let floor = interval * (runs - 1);

    println!(
        "{} runs on a {:?} period took {:?}, floor {:?}",
        runs, interval, elapsed, floor,
    );

    assert!(
        elapsed >= floor,
        "{} runs on a {:?} period took only {:?}",
        runs,
        interval,
        elapsed,
    );
}

/// Cancelling an `at_rate` schedule ends every run of it
#[test]
fn every_ends_the_whole_series_on_cancel() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).at_rate(Duration::from_millis(5)).spawn();

    // Several periods in, so the schedule is well established
    for _ in 0..10 {
        take_a_run(&handle);
    }

    handle.clone().cancel();

    assert_eq!(
        handle.clone().take(),
        Err(RuntimeError::Cancelled),
        "a cancelled schedule hands nothing out",
    );

    // Long enough for many more periods, and for every run in
    // flight to have tried to publish
    thread::sleep(Duration::from_millis(200));

    assert_eq!(
        handle.take(),
        Err(RuntimeError::Cancelled),
        "the schedule carried on after being cancelled",
    );
}

/// A million tasks spawned and joined one at a time
#[test]
fn many_one_by_one_tasks() {
    Runtime::init();

    let tasks = 1_000_000;

    let mut avg = 0.0;

    for _ in 0..tasks {
        let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(500), true)).spawn();

        if let Ok(time) = handle.join() {
            avg += time.as_nanos() as f32;
        }
    }

    avg /= tasks as f32;

    println!("Average time: {}", avg);
}

/// Waits for the next run of a repeating task and takes it
fn take_a_run(handle: &TaskHandle<Duration>) -> Duration {
    let mut polls = 0u64;
    let waited = Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The next run hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!(
                "a repeating task came back with {:?} after {} polls, pool {:?}",
                error,
                polls,
                Runtime::workers(),
            ),
        }

        polls += 1;

        // A series that stopped producing fails rather than hangs
        assert!(
            waited.elapsed() < Duration::from_secs(30),
            "a repeating task stopped producing runs after {} polls, pool {:?}",
            polls,
            Runtime::workers(),
        );

        thread::sleep(Duration::from_micros(100));
    }
}

/// The pool works through a backlog while the manager is down,
/// alongside every other test in this file, and the manager
/// comes back
#[test]
fn survives_losing_its_manager() {
    Runtime::init();

    let tasks = 200_000;
    let quick = || Sleep::sleep(Duration::from_nanos(1), true);

    let started = Instant::now();

    // Deep enough that the pool is still working through it
    // long after the manager has gone
    let before: Vec<_> = (0..tasks).map(|_| Runtime::task(quick()).spawn()).collect();

    Runtime::inject_manager_faults(3);

    let during: Vec<_> = (0..tasks).map(|_| Runtime::task(quick()).spawn()).collect();

    let mut finished = 0u64;

    for handle in before.into_iter().chain(during) {
        handle
            .join()
            .expect("every task finishes with no manager to help it");

        finished += 1;
    }

    report("manager back");

    // Only the manager reads this timer
    let timed = Runtime::task(quick()).repeat().every(Duration::from_millis(20)).spawn();

    for _ in 0..3 {
        take_a_run(&timed);
    }

    timed.cancel();

    println!(
        "{} tasks through a pool that lost its manager three times in {:?}, and timers after",
        finished,
        started.elapsed(),
    );
}

/// A line of what the pool is doing at this moment
fn report(at: &str) {
    let stats = Runtime::workers();

    println!(
        "  [{}] {} workers ({} busy), {} sleep threads ({} busy), \
         {} queued, {} blocking, {} backlog, {} live, {} slots",
        at,
        stats.len(),
        stats.busy(),
        stats.sleep_threads(),
        stats.sleep_busy(),
        stats.queued(),
        stats.blocking_queued(),
        stats.backlog(),
        stats.live(),
        stats.peak_slots(),
    );
}

/// `maybe_take` polls a task whose output can't be cloned
/// without committing to waiting for it
#[test]
fn maybe_take_polls_without_committing() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(200), false)).spawn();

    assert_eq!(
        handle.maybe_take(),
        Err(RuntimeError::NotReady),
        "a task still running hasn't failed, it just isn't finished",
    );

    handle.wait().expect("the task settles");

    let taken = handle.maybe_take().expect("the value moves out once");
    println!("took {taken:?}");

    assert_eq!(
        handle.maybe_take(),
        Err(RuntimeError::AlreadyTaken),
        "only one caller ever gets the output, however it is asked for",
    );
}

/// Giving up on a take leaves the output where it was
#[test]
fn take_with_timeout_costs_nothing_when_it_gives_up() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(300), false)).spawn();

    assert_eq!(
        handle.take_with_timeout(Duration::from_millis(20)),
        Err(RuntimeError::NotReady),
        "ran out of patience before the task ran out of work",
    );

    // Nothing was claimed, so the value is still there
    let taken = handle
        .take_with_timeout(Duration::from_secs(5))
        .expect("the output survived the caller giving up on it");

    println!("took {taken:?} on the second ask");
}

/// Waiting without reading, and without giving up the handle
#[test]
fn wait_settles_without_consuming_or_reading() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(50), false)).spawn();

    let state = handle.wait().expect("the task settles");

    assert_eq!(state, TaskState::Ready, "it finished, so it has an output");
    assert!(handle.is_ready(), "and the handle agrees");

    handle.join().expect("the output is still there to be had");
}

/// The state and the predicates say the same thing
#[test]
fn state_and_predicates_agree() {
    Runtime::init();

    let ready = Runtime::task(Sleep::sleep(Duration::from_millis(20), false)).spawn();
    ready.wait().expect("it settles");

    assert_eq!(ready.state(), TaskState::Ready);
    assert!(ready.is_ready() && ready.settled());

    let cancelled = Runtime::task(Sleep::sleep(Duration::from_secs(5), false)).spawn();
    cancelled.clone().cancel();

    assert_eq!(cancelled.state(), TaskState::Cancelled);
    assert!(cancelled.is_cancelled() && cancelled.settled());

    // Settled is not the same as having something to hand out
    assert!(
        !cancelled.is_ready(),
        "a cancelled task has settled and has nothing to give",
    );

    let taken = Runtime::task(Sleep::sleep(Duration::from_millis(20), false)).spawn();
    let watcher = taken.clone();
    taken.take().expect("the value moves out");

    assert_eq!(watcher.state(), TaskState::Taken);
    assert!(watcher.is_taken());

    // An empty slot is never observable through a handle
    for handle in [&ready, &cancelled, &watcher] {
        assert_ne!(handle.state(), TaskState::Free, "a live handle is never free");
    }
}

/// A repeat built at a priority is still a repeat
#[test]
fn builder_repeats_at_a_priority() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false))
        .priority(200)
        .repeat()
        .spawn();

    let first = handle.take_with_timeout(Duration::from_secs(5));

    // Retried, since a read between runs comes back `AlreadyTaken`
    // rather than waiting
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut second = Err(RuntimeError::AlreadyTaken);

    while Instant::now() < deadline {
        second = handle.take_with_timeout(Duration::from_millis(100));

        if second.is_ok() {
            break;
        }
    }

    // Before the assertions, or a panic leaves it running
    handle.cancel();

    assert!(first.is_ok(), "the first run publishes: {first:?}");
    assert!(
        second.is_ok(),
        "a later read succeeds where a one shot would stay AlreadyTaken: {second:?}",
    );
}

/// A handle can be a map key, which is what hashing one is for
#[test]
fn handles_compare_and_hash_on_the_task() {
    use std::collections::HashSet;

    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(20), false)).spawn();
    let same = handle.clone();
    let other = Runtime::task(Sleep::sleep(Duration::from_millis(20), false)).spawn();

    assert_eq!(handle, same, "a clone points at the same task");
    assert_ne!(handle.id(), other.id(), "two spawns are two tasks");

    let mut seen = HashSet::new();

    assert!(seen.insert(handle.clone()));
    assert!(!seen.insert(same), "the same task doesn't go in twice");
    assert!(seen.insert(other.clone()));

    handle.join().expect("still finishes");
    other.join().expect("still finishes");
}

/// Every task in a set, in the order they were given
#[test]
fn join_all_keeps_the_order_it_was_given() {
    Runtime::init();

    let handles: Vec<_> = (1..=5)
        .map(|step| Runtime::task(Sleep::sleep(Duration::from_millis(step * 10), false)).spawn())
        .collect();

    let results = Runtime::join_all(handles);

    assert_eq!(results.len(), 5, "one result per task");

    // Given longest last, so ascending results are the order kept
    // rather than the order finished
    for (index, result) in results.into_iter().enumerate() {
        let slept = result.expect("every task finishes");
        let asked = Duration::from_millis((index as u64 + 1) * 10);

        assert!(
            slept >= asked,
            "result {index} slept {slept:?} against {asked:?}, so the order moved",
        );
    }
}

/// A healthy runtime says so
#[test]
fn status_reports_a_live_runtime() {
    Runtime::init();

    let status = Runtime::status();
    println!("{status}");

    assert!(Runtime::initialised(), "init has finished");
    assert!(status.initialised());
    assert!(!status.shut_down(), "nothing has shut this down");

    assert!(status.reactor_alive(), "the reactor is up");
    assert!(status.manager_alive(), "the manager is up");
    assert!(status.healthy());
}

/// A delayed task waits, then runs
#[test]
fn after_waits_before_it_runs() {
    Runtime::init();

    let delay = Duration::from_millis(200);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).after(delay).spawn();

    // Sitting on a timer rather than anywhere in the pool
    assert!(!handle.settled(), "nowhere near the delay being up");
    assert_eq!(
        handle.maybe_join(),
        Err(RuntimeError::NotReady),
        "a task waiting out a delay hasn't failed, it just hasn't started",
    );

    let state = handle.wait().expect("it runs once the delay is up");
    let waited = started.elapsed();

    println!("ran after {waited:?} of a {delay:?} delay");

    assert_eq!(state, TaskState::Ready, "it ran and published");
    assert!(
        waited >= delay,
        "started after only {waited:?}, which is early",
    );
}

/// Far more delayed tasks than the pool has threads all get
/// through, since a delay costs a slot and no thread
#[test]
fn many_delayed_tasks_cost_no_threads() {
    Runtime::init();

    let delay = Duration::from_millis(300);
    let started = Instant::now();

    let handles: Vec<_> = (0..2_000)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50), false)).after(delay).spawn())
        .collect();

    for (task, handle) in handles.into_iter().enumerate() {
        handle
            .join()
            .unwrap_or_else(|error| panic!("delayed task {task} never ran: {error}"));
    }

    let total = started.elapsed();

    println!("2000 delayed tasks all ran, {total:?} against a {delay:?} delay");

    assert!(
        total >= delay,
        "they can't all have waited their delay in {total:?}",
    );
}

/// The builder reaches the same delay the method does
#[test]
fn builder_after_delays_too() {
    Runtime::init();

    let delay = Duration::from_millis(150);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false))
        .priority(200)
        .after(delay)
        .spawn();

    handle.wait().expect("it runs once the delay is up");
    let waited = started.elapsed();

    println!("the built one ran after {waited:?} of a {delay:?} delay");

    assert!(waited >= delay, "started after only {waited:?}");

    // Still a one shot
    handle.take().expect("the output is there");
}

/// A delay and a repeat compose rather than replacing each other
#[test]
fn delay_and_repeat_compose() {
    Runtime::init();

    let delay = Duration::from_millis(200);
    let gap = Duration::from_millis(20);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .after(delay)
        .repeat()
        .every(gap)
        .spawn();

    handle
        .wait()
        .expect("the first run happens once the delay is up");

    let first = started.elapsed();
    println!("first run after {first:?} of a {delay:?} delay");

    // Before the assertion, or a panic leaves it running
    handle.cancel();

    assert!(
        first >= delay,
        "the first run came after {first:?}, so the delay was lost when the kind was set",
    );
}

/// Drains a bounded series, counting what it published
fn drain(handle: &TaskHandle<Duration>, patience: Duration) -> usize {
    let deadline = Instant::now() + patience;
    let mut seen = 0;

    while Instant::now() < deadline {
        match handle.maybe_take() {
            Ok(_) => seen += 1,

            // Between runs, or one still going
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::NotReady) => {
                thread::sleep(Duration::from_millis(1))
            }

            // `Finished` and every other error are endings
            Err(_) => break,
        }
    }

    seen
}

/// A deadline already past runs once and stops
#[test]
fn until_in_the_past_runs_once() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .until(Instant::now())
        .spawn();

    let seen = drain(&handle, Duration::from_secs(5));

    println!("a deadline already past ran {seen} time(s)");

    assert!(handle.is_finished());
    assert_eq!(seen, 1, "the first run is never the one that is refused");
}

/// A schedule counts what it starts
#[test]
fn at_rate_starts_exactly_its_count() {
    Runtime::init();

    let runs = 4;

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .at_rate(Duration::from_millis(40))
        .count(runs)
        .spawn();

    let seen = drain(&handle, Duration::from_secs(10));

    println!("a schedule bounded to {runs} published {seen} times");

    assert!(handle.is_finished(), "the schedule never reported finishing");

    // Overlapping runs can publish one output between two, so
    // only an upper bound can be seen
    assert!(
        seen > 0 && seen as u32 <= runs,
        "saw {seen} outputs from {runs} runs",
    );
}

/// A schedule can be delayed too
#[test]
fn at_rate_waits_out_a_delay_before_its_first_run() {
    Runtime::init();

    let delay = Duration::from_millis(200);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .at_rate(Duration::from_millis(30))
        .after(delay)
        .spawn();

    handle
        .wait()
        .expect("the first run happens once the delay is up");

    let first = started.elapsed();
    println!("the schedule's first run landed after {first:?} of a {delay:?} delay");

    handle.cancel();

    assert!(
        first >= delay,
        "the first run came after {first:?}, so the delay was never served",
    );
}

/// Cancelling a bounded series still ends it
#[test]
fn a_cancelled_bounded_repeat_is_finished() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false))
        .repeat()
        .every(Duration::from_millis(50))
        .count(1_000)
        .spawn();

    let watcher = handle.clone();
    handle.cancel();

    assert_eq!(
        watcher.clone().join(),
        Err(RuntimeError::Cancelled),
        "a cancelled series hands nothing out",
    );

    // The kind still says it repeats, which is why the state
    // alone can't answer this
    assert!(
        watcher.is_finished(),
        "a cancelled series is not going to run again",
    );

    watcher.cancel();
}

/// A repeat with no bound is not finished between runs
#[test]
fn an_unbounded_repeat_is_never_finished() {
    Runtime::init();

    // A long gap, so the read below lands between runs rather
    // than racing the next one
    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(5), false))
        .repeat()
        .every(Duration::from_secs(60))
        .spawn();

    handle.wait().expect("a run publishes");

    let settled = handle.settled();
    let finished = handle.is_finished();

    handle.cancel();

    assert!(settled, "it published, so it has settled");
    assert!(!finished, "but there is another run coming, so it isn't over");
}
