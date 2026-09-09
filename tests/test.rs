use atap::{File, JoinPolicy, Runtime, RuntimeError, Sleep, TaskHandle, TaskState};
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

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

#[test]
fn cloned_handles_read_the_same_value_across_threads() {
    Runtime::init();

    let threads = 16;
    let rounds = 32;

    // Two events rather than one, so a handle that reads the
    // wrong task comes back with a wrong answer rather than
    // with nothing. Far enough apart that no amount of jitter
    // could make one look like the other
    let quick = Duration::from_millis(300);
    let slow = Duration::from_millis(700);

    let first = Runtime::task(Sleep::sleep(quick, false)).spawn();
    let second = Runtime::task(Sleep::sleep(slow, false)).spawn();

    let barrier = Arc::new(Barrier::new(threads));

    let readers: Vec<_> = (0..threads)
        .map(|thread| {
            // Half the threads on each event
            let handle = match thread % 2 == 0 {
                true => first.clone(),
                false => second.clone(),
            };

            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                // Held until every thread is ready, so one
                // thread's clones are being made while another
                // thread's reads are already in flight
                barrier.wait();

                let mut seen = Vec::with_capacity(rounds);
                let mut held = Vec::new();

                for round in 0..rounds {
                    let copy = handle.clone();

                    // Half read straight away and half are kept
                    // back, so cloning and reading stay mixed
                    // rather than falling into two phases
                    match round % 2 == 0 {
                        true => seen.push(copy.join().expect("every listener reads")),
                        false => held.push(copy),
                    }
                }

                for copy in held {
                    seen.push(copy.join().expect("every listener reads"));
                }

                (thread % 2 == 0, seen)
            })
        })
        .collect();

    let mut quick_seen = Vec::new();
    let mut slow_seen = Vec::new();

    for reader in readers {
        let (was_quick, seen) = reader.join().expect("every reader finishes");

        match was_quick {
            true => quick_seen.extend(seen),
            false => slow_seen.extend(seen),
        }
    }

    // The originals last, so the output was still there after
    // every clone of it had come and gone
    let quick_value = first.join().expect("the original still reads");
    let slow_value = second.join().expect("the original still reads");

    println!(
        "{} reads of {:?} and {} of {:?}",
        quick_seen.len(),
        quick_value,
        slow_seen.len(),
        slow_value,
    );

    assert_eq!(quick_seen.len(), threads / 2 * rounds, "every clone read");
    assert_eq!(slow_seen.len(), threads / 2 * rounds, "every clone read");

    // Exactly equal, not approximately. Every listener is
    // reading one stored value through one id, so anything
    // other than an identical answer means they weren't
    assert!(
        quick_seen.iter().all(|seen| *seen == quick_value),
        "listeners on the same task disagreed about its output",
    );

    assert!(
        slow_seen.iter().all(|seen| *seen == slow_value),
        "listeners on the same task disagreed about its output",
    );

    assert!(
        quick_value >= quick && slow_value >= slow && quick_value < slow_value,
        "the two events came back as {:?} and {:?}",
        quick_value,
        slow_value,
    );
}

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

    // The whole reason this returns a result: a task that will
    // never have an answer is a different thing from one that
    // doesn't have an answer yet
    assert_eq!(
        watcher.maybe_join(),
        Err(RuntimeError::Cancelled),
        "a cancelled task says so rather than looking unfinished",
    );
}

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

    // Borrowed rather than consumed, so running out of patience
    // didn't cost the task
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

#[test]
fn repeating_runs_until_cancelled() {
    Runtime::init();

    let wanted = 20;
    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(5), false)).repeat().spawn();

    // Taking a run is what counts one. The slot goes back to
    // holding nothing, and the next run fills it again, so a
    // take that succeeds is a run that happened
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

#[test]
fn repeating_finishes_a_run_before_the_next() {
    Runtime::init();

    let duration = Duration::from_millis(50);
    let runs: u32 = 5;

    let handle = Runtime::task(Sleep::sleep(duration, false)).repeat().spawn();

    // One on its own first, so the timing below starts from the
    // end of a run rather than part way through one
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // Between the first take and the last there are four whole
    // runs, and a fifth that was already under way when the
    // clock started. Four is what can be insisted on
    let floor = duration * (runs - 1);

    println!("{} runs of {:?} took {:?}, floor {:?}", runs, duration, elapsed, floor);

    // A floor rather than a ceiling, which is what makes this
    // say something on a loaded machine. Runs held up by a busy
    // pool make it slower and never wrong; runs that overlapped
    // would fit into less time than they are long, and there is
    // nothing that can make that happen except overlapping
    assert!(
        elapsed >= floor,
        "{} runs of {:?} took {:?}, so they were overlapping",
        runs,
        duration,
        elapsed,
    );
}

#[test]
fn repeat_every_waits_between_runs() {
    Runtime::init();

    let interval = Duration::from_millis(50);
    let runs: u32 = 5;

    // A task with nothing in it, so what is being measured is
    // the gap rather than the work
    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn();

    // One on its own first, so the clock starts at the end of a
    // run rather than part way through one
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // Four whole gaps between the first take and the last, and
    // a fifth that was already being waited out when the clock
    // started
    let floor = interval * (runs - 1);

    println!(
        "{} runs {:?} apart took {:?}, floor {:?}",
        runs, interval, elapsed, floor,
    );

    // A floor, so a busy pool makes this slower and never
    // wrong. Runs closer together than the interval would mean
    // the gap wasn't being waited out at all
    assert!(
        elapsed >= floor,
        "{} runs {:?} apart took only {:?}",
        runs,
        interval,
        elapsed,
    );
}

#[test]
fn every_starts_runs_on_the_interval() {
    Runtime::init();

    let interval = Duration::from_millis(50);
    let runs: u32 = 5;

    // A task with nothing in it, so nothing overlaps and what
    // is being measured is the clock rather than the work
    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).at_rate(interval).spawn();

    // The first run goes at once rather than an interval in, so
    // the clock starts from the end of it
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // Four whole periods between the first take and the last,
    // and a fifth that was already running when the clock
    // started
    let floor = interval * (runs - 1);

    println!(
        "{} runs on a {:?} period took {:?}, floor {:?}",
        runs, interval, elapsed, floor,
    );

    // A floor, so a busy pool makes this slower and never
    // wrong. Runs arriving closer together than the period
    // would mean the schedule wasn't being kept at all
    assert!(
        elapsed >= floor,
        "{} runs on a {:?} period took only {:?}",
        runs,
        interval,
        elapsed,
    );
}

#[test]
fn every_overlaps_its_runs() {
    Runtime::init();

    let interval = Duration::from_millis(50);
    let duration = Duration::from_millis(200);
    let runs: u32 = 5;

    // Four times the period, so four runs are in flight before
    // the first one has finished. This is the whole difference
    // between `every` and the other two, and the only way to
    // see it from out here is how fast the outputs arrive
    //
    // Four rather than the ten a 20ms period would give,
    // because a sleep this long is a blocking task and takes a
    // sleep thread for its whole duration. Ten of those held
    // across this test moves a number the rest of the suite
    // reads — the pool starts a new thread only when none is
    // parked, so the threads this leaves behind are threads
    // another test doesn't start — and four still tells the two
    // apart by a factor of four
    let handle = Runtime::task(Sleep::sleep(duration, false)).at_rate(interval).spawn();

    // The first output lands a whole duration in whatever
    // happens, so the clock starts after it
    take_a_run(&handle);

    let started = Instant::now();

    for _ in 0..runs {
        take_a_run(&handle);
    }

    let elapsed = started.elapsed();

    handle.clone().cancel();

    // What it would take if a run had to finish before the next
    // one could start, which is what `repeating` and
    // `repeat_every` both promise and this one doesn't
    let serial = duration * runs;
    let ceiling = serial / 2;

    println!(
        "{} runs of {:?} on a {:?} period took {:?}, one at a time would be {:?}",
        runs, duration, interval, elapsed, serial,
    );

    // A ceiling rather than a floor, which is the other way
    // round from every other timing test here — and it holds up
    // because the thing being ruled out is five times slower
    // than the thing being measured, not five percent. A pool
    // busy enough to eat that margin would have to be five
    // times over
    assert!(
        elapsed < ceiling,
        "{} runs of {:?} took {:?}, so they were running one at a time",
        runs,
        duration,
        elapsed,
    );
}

#[test]
fn every_ends_the_whole_series_on_cancel() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).at_rate(Duration::from_millis(5)).spawn();

    // Several periods in, so the schedule is well established
    // rather than being cancelled before it ever got going
    for _ in 0..10 {
        take_a_run(&handle);
    }

    handle.clone().cancel();

    assert_eq!(
        handle.clone().take(),
        Err(RuntimeError::Cancelled),
        "a cancelled schedule hands nothing out",
    );

    // Long enough for a great many more periods to have come
    // round, and for every run still in flight to have finished
    // and tried to publish
    thread::sleep(Duration::from_millis(200));

    assert_eq!(
        handle.take(),
        Err(RuntimeError::Cancelled),
        "the schedule carried on after being cancelled",
    );
}

#[test]
fn cancelling_a_spawned_task_settles_every_listener() {
    Runtime::init();

    // Long enough that it could not possibly have finished on
    // its own by the time anything below is checked
    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(30), false)).spawn();
    let watcher = handle.clone();

    // Far enough in to be sitting inside the kernel wait rather
    // than still queued, which is the case worth cancelling
    thread::sleep(Duration::from_millis(200));

    let started = Instant::now();
    handle.cancel();

    assert_eq!(
        watcher.join(),
        Err(RuntimeError::Cancelled),
        "a cancelled task hands nothing out",
    );

    let settled = started.elapsed();

    println!("a 30 second sleep cancelled and settled in {:?}", settled);

    assert!(
        settled < Duration::from_secs(1),
        "cancelling a sleep took {:?} to settle",
        settled,
    );
}

#[test]
fn concurrent_spawn_distinct_results() {
    Runtime::init();

    let threads = 8;
    let per_thread = 8;

    let spawners: Vec<_> = (0..threads)
        .map(|worker| {
            thread::spawn(move || {
                // Every task in the run gets its own duration,
                // so a result landing in the wrong slot shows
                // up as a wrong answer rather than as nothing
                (0..per_thread)
                    .map(|task| {
                        let millis = worker * per_thread + task + 1;
                        let duration = Duration::from_millis(millis);

                        (duration, Runtime::task(Sleep::sleep(duration, false)).spawn())
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    for spawner in spawners {
        for (duration, handle) in spawner.join().unwrap() {
            let slept = handle.join().expect("every task finishes");

            assert!(
                slept >= duration && slept < duration + Duration::from_millis(500),
                "expected roughly {:?}, got {:?}",
                duration,
                slept,
            );
        }
    }
}

#[test]
fn spawning_does_not_leak() {
    Runtime::init();

    let warmup = 1_000;
    let total = 200_000;
    let mut baseline = 0;

    for task in 0..total {
        let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn();
        handle.join().expect("every task finishes");

        // Taken after the table has grown to its working size
        // so that growth isn't mistaken for a leak
        if task == warmup {
            baseline = max_rss();
        }
    }

    let after = max_rss();
    let growth = after.saturating_sub(baseline);

    println!(
        "baseline {} bytes, after {} bytes, growth {} bytes",
        baseline, after, growth
    );

    // Ids are recycled, so the table never grows past its
    // first block and the mappings go straight back to the
    // kernel as each task is read. Growth should be flat
    //
    // A free list that never gives an id back would instead
    // leave a slot resident per task, which at 16 bytes each
    // and this many tasks is a few megabytes. The threshold
    // sits between the two, high enough to ride out the noise
    // in a high water mark and well under the leak it is
    // watching for
    assert!(
        growth < 2 * 1024 * 1024,
        "{} tasks grew the process by {} bytes",
        total,
        growth,
    );
}

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

#[test]
fn many_concurrent_tasks() {
    Runtime::init();

    let tasks = 1_000_000;
    let mut handle_list = Vec::with_capacity(tasks);

    let mut avg = 0.0;

    let baseline = max_rss();

    for _ in 0..tasks {
        let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(500), true)).spawn();

        handle_list.push(handle);
    }

    // Read before the handles are joined, while every task is
    // still live and holding its slot. This is the only test
    // that has more than a handful alive at once, so it is the
    // only one that says anything about what a slot costs
    let peak = max_rss();
    let slots = Runtime::workers().slots;

    for handle in handle_list {
        if let Ok(time) = handle.join() {
            avg += time.as_nanos() as f32;
        }
    }

    avg /= tasks as f32;

    let growth = peak.saturating_sub(baseline);

    println!("Average time: {}", avg);
    println!(
        "{} live tasks cost {} bytes, {} each",
        tasks,
        growth,
        growth / tasks,
    );
    println!("table handed out {} slots", slots);

    // Said outright rather than inferred from the byte count.
    // A handle keeps its task's slot alive, so a million live
    // handles means the table had to hand out a million slots.
    // Fewer means slots were recycled underneath live handles,
    // which would make every join above a read of somebody
    // else's task and the byte count meaningless
    assert!(
        slots >= tasks,
        "{} live handles but the table only handed out {} slots, \
         so slots are being recycled while handles still hold them",
        tasks,
        slots,
    );

    // Slots come out of shared blocks, so a live task costs
    // SLOT_SIZE and its share of the block holding it, which
    // is around 256MB across a million of them. The handles
    // and the boxed tasks waiting to be run add to that, so
    // the threshold sits well clear of it
    //
    // What it is really watching for is a slot going back to
    // owning its own mapping. That is a whole page each, or
    // roughly 16GB at this count on Apple silicon, so the two
    // are never going to be confused for one another
    assert!(
        growth < 512 * 1024 * 1024,
        "{} live tasks took {} bytes, which is page per slot territory",
        tasks,
        growth,
    );
}

#[test]
fn concurrent_tasks_run_in_parallel() {
    Runtime::init();

    let duration = Duration::from_millis(500);
    let tasks = cores();

    let started = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(duration, false)).spawn())
        .collect();

    // Each task reports how long it took, so adding those up
    // and comparing against the wall clock says how much of it
    // happened at the same time
    let mut slept = Duration::ZERO;

    for handle in handles {
        slept += handle.join().expect("every task finishes");
    }

    let elapsed = started.elapsed();

    println!(
        "{} tasks of {:?} took {:?}, {:?} slept between them",
        tasks, duration, elapsed, slept,
    );

    // Measured as overlap rather than against a stopwatch. A
    // machine with a million other tasks on it stretches any
    // wall clock threshold, but it can't make sleeps that ran
    // one after another add up to more than the time they ran
    // in. Sequential puts this ratio at 1 whatever else the
    // machine is doing
    assert!(
        slept > elapsed * 2,
        "{} sleeps took {:?} of wall clock but only {:?} between them, \
         so they were barely overlapping",
        tasks,
        elapsed,
        slept,
    );
}

#[test]
fn concurrent_tasks_run_in_parallel_long() {
    Runtime::init();

    let duration = Duration::from_secs(5);
    let tasks = cores();

    let started = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(duration, false)).spawn())
        .collect();

    // Each task reports how long it took, so adding those up
    // and comparing against the wall clock says how much of it
    // happened at the same time
    let mut slept = Duration::ZERO;

    for handle in handles {
        slept += handle.join().expect("every task finishes");
    }

    let elapsed = started.elapsed();

    println!(
        "{} tasks of {:?} took {:?}, {:?} slept between them",
        tasks, duration, elapsed, slept,
    );

    // Measured as overlap rather than against a stopwatch. A
    // machine with a million other tasks on it stretches any
    // wall clock threshold, but it can't make sleeps that ran
    // one after another add up to more than the time they ran
    // in. Sequential puts this ratio at 1 whatever else the
    // machine is doing
    assert!(
        slept > elapsed * 2,
        "{} sleeps took {:?} of wall clock but only {:?} between them, \
         so they were barely overlapping",
        tasks,
        elapsed,
        slept,
    );
}

#[test]
fn pool_grows_under_blocking_load() {
    Runtime::init();

    let tasks = cores() * 4;
    let duration = Duration::from_millis(400);

    let started = Instant::now();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(duration, false)).spawn())
        .collect();

    // Long enough for the offloads to have found threads, and
    // far short of the sleeps finishing
    thread::sleep(Duration::from_millis(100));

    let stats = Runtime::workers();

    println!(
        "{} blocking tasks got {} sleep threads, {} busy, {} still queued",
        tasks, stats.sleep_threads, stats.sleep_busy, stats.blocking_queued,
    );

    let mut slept = Duration::ZERO;

    for handle in handles {
        slept += handle.join().expect("every task finishes");
    }

    let elapsed = started.elapsed();

    println!(
        "{} blocking sleeps of {:?} took {:?}, {:?} slept between them",
        tasks, duration, elapsed, slept,
    );

    // Blocking tasks grow the sleep threads rather than the
    // workers, because a worker hands one over and carries
    // straight on rather than being held by it
    assert!(
        stats.sleep_threads >= tasks / 2,
        "{} blocking tasks in flight got only {} threads",
        tasks,
        stats.sleep_threads,
    );

    // Overlap rather than a stopwatch, for the same reason
    // `concurrent_tasks_run_in_parallel` measures it that way
    assert!(
        slept > elapsed * 2,
        "{} blocking sleeps took {:?} of wall clock but only {:?} between them, \
         so they were barely overlapping",
        tasks,
        elapsed,
        slept,
    );
}

#[test]
fn pool_reaps_idle_sleep_threads() {
    Runtime::init();

    let handles: Vec<_> = (0..cores() * 4)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_millis(200), false)).spawn())
        .collect();

    // Read while they are all still in flight, so the number
    // being come down from is this test's own doing
    let peak = Runtime::workers().sleep_threads;

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    // Long enough for the idle window to pass and for the
    // manager to have ticked well past it
    thread::sleep(Duration::from_millis(1500));

    let settled = Runtime::workers().sleep_threads;

    println!("{} sleep threads at peak, {} once idle", peak, settled);

    // Measured against this test's own peak rather than against
    // zero. `Runtime` is one pool for the whole process, so
    // anything else running at the same time has its own sleep
    // threads open and none of them are this test's business
    assert!(
        settled < peak,
        "sleep threads held at {} after the idle window, having peaked at {}",
        settled,
        peak,
    );
}

#[test]
fn backlog_is_visible_while_running() {
    Runtime::init();

    let tasks = 200_000;

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50), true)).spawn())
        .collect();

    // Asked while the pool is still working through them, which
    // is the whole point: a worker answers these without being
    // interrupted and without stopping what it is doing
    let mut seen_backlog = false;
    let mut seen_busy = false;

    for _ in 0..1000 {
        let stats = Runtime::workers();

        println!("{:#?}", stats);

        seen_backlog |= stats.backlog() > 0;
        seen_busy |= stats.busy() > 0;

        if seen_backlog && seen_busy {
            break;
        }
    }

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    assert!(seen_backlog, "never saw any work queued across the pool");
    assert!(seen_busy, "never saw a worker inside a task");
}

#[test]
fn high_priority_runs_first() {
    Runtime::init();

    let tasks = 50_000;
    let started = Instant::now();

    let queued: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50), true)).spawn())
        .collect();

    // Last in, and served first anyway
    let queued_at = Instant::now();
    let urgent = Runtime::task(Sleep::sleep(Duration::from_micros(50), true)).priority(255).spawn();

    while !urgent.settled() {
        thread::yield_now();
    }

    let waited = queued_at.elapsed();

    for handle in queued {
        handle.join().expect("every task finishes");
    }

    let total = started.elapsed();

    let workers = Runtime::workers().len();

    println!(
        "urgent task waited {:?}, the {} before it took {:?}",
        waited, tasks, total,
    );

    println!("There were {} workers running at the time", workers);

    // Measured against the batch rather than against a fixed
    // number, so the test says the same thing on a fast machine
    // as on a slow one. Without priority the last task spawned
    // is also the last one run, and this would be the whole
    // batch rather than a fraction of it
    assert!(
        waited * 4 < total,
        "the top priority task waited {:?} of the batch's {:?}",
        waited,
        total,
    );
}

#[test]
fn monolithic() {
    Runtime::init();

    // Block until every other task is done
    loop {
        Runtime::block(
            Sleep::sleep(
                Duration::from_secs(2),
                true,
            )
        );

        let stats = Runtime::workers();

        if !stats.has_any_task() {
            println!("Slots: {}", stats.slots);
            break;
        }
    }

    report("drained");

    println!("\n== ids are recycled forever ==");
    recycles_ids_forever();

    println!("\n== tasks never cross ==");
    never_crosses_two_tasks();

    println!("\n== every ending at once ==");
    survives_every_ending_at_once();

    println!("\n== priority under a deep queue ==");
    keeps_priority_under_a_deep_queue();

    println!("\n== cancelling hands the thread back ==");
    cancelling_hands_the_thread_back();

    println!("\n== losing the manager ==");
    survives_losing_its_manager();

    println!("\n== a peak of live tasks ==");
    holds_a_peak_of_live_tasks();

    println!("\n== a repeating task holds one slot ==");
    repeating_holds_one_slot();

    println!("\n== a schedule gives its run slots back ==");
    every_gives_its_run_slots_back();

    println!("\n== waiting costs no thread ==");
    waiting_costs_no_thread();

    println!("\n== the table gives its pages back ==");
    gives_the_table_back();

    println!("\n== outputs that own memory are dropped ==");
    file_outputs_are_dropped_not_leaked();

    println!("\n== a race picks one and settles the rest ==");
    join_first_settles_every_loser();

    println!();
    report("finished");
}

/// A race gives every slot back, whichever way the losers end
///
/// ## Why it lives here
/// The isolated tests check that each policy does what it says.
/// What they can't check is the accounting: a race holds a
/// registration on every slot in its set, and three of the
/// four ways out of one — cancelled, dropped, handed back and
/// then forgotten — end somewhere other than a read
///
/// A registration left behind on a slot that has since been
/// freed and handed to somebody else would point a later task's
/// wake at a queue nobody is waiting on. Nothing about that is
/// visible from one race, and everything about it is visible
/// from a few thousand
fn join_first_settles_every_loser() {
    let races = 512;
    let width = 8;

    let base = settled_live();

    // All three policies, in rotation, so no one way out of a
    // race is the only one exercised
    for race in 0..races {
        let quick = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn();

        let slow: Vec<_> = (0..width)
            .map(|_| Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).spawn())
            .collect();

        let policy = match race % 3 {
            0 => JoinPolicy::Cancel,
            1 => JoinPolicy::Drop,
            _ => JoinPolicy::PassBack,
        };

        let (first, rest) = Runtime::join_first(std::iter::once(quick).chain(slow), policy);

        assert!(first.settled(), "a race produced an unsettled winner");

        match rest {
            Some(losers) => {
                assert_eq!(losers.len(), width, "PassBack lost track of a loser");

                // Handed back and then let go without being
                // read, which is the case a caller who asked
                // for them and then changed their mind makes
                drop(losers);
            }
            None => assert_ne!(policy, JoinPolicy::PassBack, "PassBack handed back nothing"),
        }
    }

    report("races run");

    // Waited for, rather than sampled once it stops moving.
    // `settled_live` asks whether the count has stopped
    // changing, and a backlog that hasn't started draining
    // answers yes to that just as readily as one that has
    // finished — four thousand sleeps queued behind a pool that
    // can run sixty of them at a time look perfectly still for
    // as long as the first one takes
    //
    // Every race here ends without a read, so there is no
    // handle left to join and no way to wait on the work
    // itself. Waiting on the pool is what is left
    let waited = Instant::now();

    while waited.elapsed() < Duration::from_secs(30) {
        let now = Runtime::workers();

        if !now.has_any_task() && now.live <= base + 8 {
            break;
        }

        thread::sleep(Duration::from_millis(20));
    }

    let after = Runtime::workers().live;

    println!("  {} races of {}, live {} -> {}", races, width + 1, base, after);

    assert!(
        after <= base + 8,
        "{} live tasks after {} races against {} before them",
        after,
        races,
        base,
    );
}

/// An output that owns memory is dropped, however it ends
///
/// Every output the crate ran before file tasks existed was a
/// `Duration`, a `usize` or a `()`. All of them `Copy`, so the
/// `drop_glue` a slot keeps compiled to nothing and the three
/// places that call it had never dropped anything real
///
/// A `Vec<u8>` is the first that does, which puts all three on
/// the hook at once:
///
/// - `destroy` drops an output nobody took, when the last
///   listener leaves
/// - `recycle` drops the previous run's output when a repeat
///   comes round again
/// - `drop_glue` is how either of them knows what it is
///   dropping, after the type is gone
///
/// ## Why it lives here
/// A leak in any of them is invisible to a test that takes its
/// results and asserts on them. The slot count stays perfect,
/// every assertion passes, and the only symptom is memory that
/// never comes back — which needs volume and an accounting
/// pass either side of it to show up at all
///
/// #### Note
/// Half the handles are dropped rather than read, and the
/// repeat is never read at all. Reading them would be the
/// version of this test that can't fail
fn file_outputs_are_dropped_not_leaked() {
    let reads = 2048;
    let size = 16 * 1024;
    let runs = 64;

    let path = fixture("monolithic-outputs", size);

    // Read once nothing is moving, for the same reason every
    // other phase does it — the one before is still winding
    // down and a baseline taken now is a number on its way
    let base = settled_live();
    let before = Runtime::workers();

    let handles: Vec<_> = (0..reads)
        .map(|_| Runtime::task(File::read(&path)).spawn())
        .collect();

    let mut taken = 0;
    let mut dropped = 0;

    for (index, handle) in handles.into_iter().enumerate() {
        // Settled before either branch, so the half that goes
        // unread is dropped holding a whole output rather than
        // being dropped before there was one to hold
        let _ = handle.wait();

        if index % 2 == 0 {
            let read = handle.take().expect("take failed").expect("read failed");

            assert_eq!(read.len(), size, "a read came back the wrong length");

            taken += 1;

            continue;
        }

        drop(handle);

        dropped += 1;
    }

    report("outputs taken and dropped");

    // Every run but the last has its output dropped by the
    // recycle the next one does, and the last by the teardown.
    // Nothing reads any of them
    let repeated = Runtime::task(File::read(&path))
        .repeat()
        .every(Duration::from_millis(1))
        .count(runs)
        .spawn();

    let waited = Instant::now();

    while !repeated.is_finished() && waited.elapsed() < Duration::from_secs(30) {
        thread::sleep(Duration::from_millis(5));
    }

    assert!(repeated.is_finished(), "the unread repeat never finished");

    drop(repeated);

    let after = settled_live();
    let stats = Runtime::workers();

    report("outputs settled");

    println!(
        "  {} taken, {} dropped unread, {} recycled unread, live {} -> {}",
        taken, dropped, runs, base, after,
    );

    assert_eq!(taken + dropped, reads, "some handles went missing");

    // Slots rather than bytes, because a slot is the thing this
    // suite can count. A payload leaked without its slot would
    // pass here — the number that catches that one is the
    // process's own memory, which is what running the whole of
    // this under a watch is for
    assert!(
        after <= base + 8,
        "{} live tasks after the file phase against {} before it",
        after,
        base,
    );

    assert!(
        stats.slots >= before.slots,
        "the table lost slots it had already handed out",
    );

    let _ = fs::remove_file(&path);
}

/// Writes a file of `size` bytes and gives back its path
///
/// Under `tests/files`, which is ignored whole — git doesn't
/// track directories, so an ignored one doesn't survive a clone
/// and it has to be made rather than assumed
fn fixture(name: &str, size: usize) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    let path = root.join(format!("{}-{}.txt", name, std::process::id()));
    let body: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();

    fs::write(&path, body).expect("could not write the fixture");

    path
}

/// The table hands its pages back once it has stopped using
/// them
///
/// Run straight after the peak, where the table is at its
/// largest and every slot in it has been given back, which is
/// the case the whole thing exists for
///
/// The last part is the one that matters. Giving pages back is
/// only safe if the table still works afterwards, so this
/// spawns into the range that was just handed over and checks
/// every one of them comes back with an answer
fn gives_the_table_back() {
    let passes = 8;

    let before = Runtime::workers();

    // Impossible on its face, so it catches the count running
    // away rather than letting it quietly refuse to trim for
    // the rest of the process
    assert!(
        before.live <= before.slots,
        "{} live tasks in a table that has only ever handed out {} slots",
        before.live,
        before.slots,
    );

    let mut released = 0;
    let mut done = 0;

    // A pass gives back at most a fifth, so it takes repeating.
    // Capped rather than run to the floor, because each pass
    // walks the whole free list and forty of them would take
    // longer than the rest of this test put together
    //
    // A refusal isn't the end of it. The runtime trims itself
    // as well, and one already under way turns this one away
    // rather than fighting it for the free list, so a few extra
    // attempts are allowed to land the passes wanted
    for _ in 0..passes * 4 {
        if done >= passes {
            break;
        }

        if let Ok(bytes) = Runtime::trim() {
            released += bytes;
            done += 1;
        }
    }

    let after = Runtime::workers();

    println!(
        "{} passes gave back {} bytes, table {} -> {} slots, {} live",
        done, released, before.slots, after.slots, after.live,
    );

    report("trimmed");

    assert!(done > 0, "the table refused to give anything back");

    assert!(
        after.slots < before.slots,
        "the table stayed at {} slots",
        after.slots,
    );

    // Never below the hundred slots it always keeps
    assert!(
        after.slots >= 100,
        "the table trimmed itself down to {} slots",
        after.slots,
    );

    // Straight back into the range that was just handed over
    let handles: Vec<_> = (0..200_000)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn())
        .collect();

    for handle in handles {
        handle
            .join()
            .expect("the table still works after giving pages back");
    }

    println!("200000 tasks ran through the trimmed table");
}

/// Waits for the next run of a repeating task and takes it
///
/// A take that lands is a run that happened, and it empties the
/// slot so the run after it can be counted the same way
fn take_a_run(handle: &TaskHandle<Duration>) -> Duration {
    let mut polls = 0u64;
    let waited = Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The last run's output has already been taken and
            // the next one hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!(
                "a repeating task came back with {:?} after {} polls, pool {:?}",
                error,
                polls,
                Runtime::workers(),
            ),
        }

        polls += 1;

        // A series that quietly stopped producing should say so
        // rather than wait for the harness to give up on the
        // whole suite. Far longer than any interval in here, so
        // it only ever fires for something genuinely stuck
        assert!(
            waited.elapsed() < Duration::from_secs(30),
            "a repeating task stopped producing runs after {} polls, pool {:?}",
            polls,
            Runtime::workers(),
        );

        // Paused rather than spun. A bare loop turns the gap
        // between runs into millions of clones of the same
        // handle, which says nothing about repeating and a
        // great deal about how hard the reference count can be
        // hammered
        thread::sleep(Duration::from_micros(100));
    }
}

/// The pool works through a backlog with nobody supervising it
///
/// ## What is being separated
/// The manager is not in the way of a task reaching a thread,
/// and this is where that stops being a claim in a comment. A
/// deep queue is put down, the manager is made to come apart
/// three times, and more work is spawned into the gap — into a
/// pool that is finding its own work, reversing its own queue
/// and clearing up after its own dead with nothing supervising
/// any of it
///
/// Then the manager coming back, which needs something only it
/// can do. A timed repeat is that something: it is driven
/// entirely by a timer on the manager's own queue and read by
/// nothing else in the process
///
/// ## Why this is here and its sibling isn't
/// Three deaths is well under the restart limit, so the
/// supervisor is expected to win. The other half of that story
/// — a manager that gives up for good — closes its queue and
/// would strand every phase after this one, so it lives in a
/// file of its own where it is the only thing in the process
///
/// Everything below this point therefore runs on a manager that
/// has already been killed and rebuilt, which is worth more
/// than the phase itself
fn survives_losing_its_manager() {
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

    // Back for real. Nothing else in the process reads that
    // queue, so a timed repeat that keeps producing runs is a
    // manager loop that is genuinely reading it again
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

/// A repeating task lives in one slot however long it runs
///
/// The whole design rests on this. A series that allocated a
/// slot per run, or that gave the `Executor`'s reference back
/// at the end of each one, looks identical from the outside
/// until you count what the table is holding
///
/// Run here rather than on its own because the table is shared
/// by everything in the process
fn repeating_holds_one_slot() {
    let runs = 20_000;

    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().spawn();

    // The first one, so the series is properly under way before
    // anything is measured
    take_a_run(&handle);

    let before = Runtime::workers();

    for _ in 1..runs {
        take_a_run(&handle);
    }

    let after = Runtime::workers();

    handle.clone().cancel();

    println!(
        "{} runs through one handle: {} -> {} slots, {} -> {} live",
        runs, before.slots, after.slots, before.live, after.live,
    );

    // A slot per run would be twenty thousand of them. The
    // slack is there for the rest of the runtime, not for this
    assert!(
        after.slots <= before.slots + 100,
        "{} runs grew the table from {} slots to {}",
        runs,
        before.slots,
        after.slots,
    );

    // One task, held for the life of the series and given back
    // once. Anything else is a reference counted wrong, in one
    // direction or the other
    assert_eq!(
        after.live, before.live,
        "{} runs took the live count from {} to {}",
        runs, before.live, after.live,
    );
}

/// A schedule hands back every slot its runs used
///
/// The one thing `every` does that nothing else here does is
/// allocate. `repeating` and `repeat_every` go round in the
/// slot they started in, so their accounting is a constant;
/// a schedule takes a fresh slot for every run it starts and
/// gives it back when that run finishes, thousands of times
/// over, while the slot the handle points at stays exactly
/// where it is
///
/// A run that never gave its slot back shows up as the table
/// climbing by one per run. A run that gave one back twice
/// would have taken the series down with it long before the
/// count could be looked at
///
/// Run here rather than on its own because it reads counts
/// across the whole table, which mean nothing while the rest
/// of the suite is spawning
fn every_gives_its_run_slots_back() {
    let schedules = 32;
    let interval = Duration::from_millis(5);
    let running = Duration::from_millis(500);

    // Read once nothing is moving rather than the instant this
    // phase starts. The phase before is still winding down — a
    // repeating task gives its slot back when its next run
    // finds it cancelled, not when `cancel` returns — so a
    // baseline taken straight away is a number still on its way
    // down, and everything below is measured against it
    settled_live();

    let before = Runtime::workers();

    // Instant runs on a short period, so the pressure is on how
    // fast slots come and go rather than on how many can be
    // held at once
    let handles: Vec<_> = (0..schedules)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).at_rate(interval).spawn())
        .collect();

    // Counted rather than assumed. A schedule that quietly
    // stopped would leave every count below looking perfect
    let mut runs = 0u64;

    let started = Instant::now();

    while started.elapsed() < running {
        for handle in &handles {
            // Taken rather than read, so each one counted is a
            // run that happened. A clone read the same output
            // over and over would count the polling instead
            if handle.clone().take().is_ok() {
                runs += 1;
            }
        }

        thread::yield_now();
    }

    report("schedules running");

    let peak = Runtime::workers();

    for handle in handles {
        handle.cancel();
    }

    // Waited out rather than timed. A schedule under load can
    // have hundreds of runs outstanding at the moment it is
    // cancelled — the interval comes round whether the pool is
    // keeping up or not, which is the whole point of it — and
    // each of those holds a claim on the schedule's slot until
    // it finishes. A fixed sleep here is a bet on how loaded
    // the machine was, and the assertion below is exact
    //
    // The cap is what keeps this a test rather than a hang. A
    // schedule that genuinely leaked never reaches the
    // condition, waits the cap out and fails with the real
    // numbers
    let settling = Instant::now();

    while Runtime::workers().live > before.live && settling.elapsed() < Duration::from_secs(10) {
        thread::sleep(Duration::from_millis(10));
    }

    let after = Runtime::workers();
    report("schedules cancelled");

    println!(
        "{} schedules on a {:?} period for {:?}: {} outputs read, \
         settled in {:?}, live {} -> {} -> {}, slots {} -> {} -> {}",
        schedules,
        interval,
        running,
        runs,
        settling.elapsed(),
        before.live,
        peak.live,
        after.live,
        before.slots,
        peak.slots,
        after.slots,
    );

    assert!(
        runs > 0,
        "{} schedules produced nothing at all in {:?}",
        schedules,
        running,
    );

    // Thousands of runs went through, and only ever a handful
    // alive at a time. The slack covers the schedules
    // themselves and the runs in flight at the moment the peak
    // was read, and nothing like a slot per run
    //
    // The table's own size says nothing here. It is already at
    // its high water mark from the phase before, so a run that
    // never gave its slot back would take one off the free list
    // rather than growing anything. Live tasks is the count
    // that still moves
    assert!(
        peak.live <= before.live + schedules * 8,
        "{} schedules took the live count from {} to {} while running",
        schedules,
        before.live,
        peak.live,
    );

    // Every schedule slot and every run slot back. This is the
    // one that catches a reference held one too many times,
    // which is the shape a leak takes when a run outlives the
    // schedule that started it
    //
    // One direction only, and deliberately. Thousands of slots
    // went out and came back through a count that belongs to
    // the whole table, so a slot arriving back late from
    // somewhere else is not this phase's business — a slot that
    // never comes back is
    assert!(
        after.live <= before.live,
        "{} schedules took the live count from {} to {}",
        schedules,
        before.live,
        after.live,
    );
}

/// Waits for the table's live count to stop moving
///
/// Counts across the whole table only say anything while
/// nothing is changing them. Cleanup after a phase is
/// asynchronous — a cancelled repeating task holds its slot
/// until the run after the cancel finds it, and a schedule
/// holds its own until its next tick comes round — so the
/// moment a phase returns is not the moment it has finished
///
/// ## Returns
/// The count once two reads in a row agreed on it, or whatever
/// it was when the wait ran out
fn settled_live() -> usize {
    let waited = Instant::now();
    let mut last = Runtime::workers().live;

    while waited.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(20));

        let now = Runtime::workers().live;

        if now == last {
            return now;
        }

        last = now;
    }

    last
}

/// Waiting out an interval costs the pool nothing
///
/// The whole reason `repeat_every` waits on a timer rather than
/// on a sleep. A thread put down for the interval would be a
/// worker or a sleep thread held for it, and worse, a worker
/// that stops finishing tasks is exactly what the manager reads
/// as stuck and grows the pool to make up for — so a task doing
/// nothing at all would have talked the pool into more threads
///
/// Run here rather than on its own because it counts threads
/// across the whole pool
fn waiting_costs_no_thread() {
    let interval = Duration::from_secs(1);

    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn();

    // The first run out of the way, so what follows is the wait
    take_a_run(&handle);

    // Well into the interval, and past enough manager ticks
    // that a pool inclined to grow would have done by now
    thread::sleep(Duration::from_millis(300));

    let stats = Runtime::workers();
    report("waiting out an interval");

    handle.clone().cancel();

    println!(
        "waiting out {:?}: {} workers busy, {} sleep threads ({} busy), {} waiting anywhere",
        interval,
        stats.busy(),
        stats.sleep_threads,
        stats.sleep_busy,
        stats.backlog(),
    );

    assert_eq!(
        stats.busy(),
        0,
        "a task that was only waiting had {} workers busy",
        stats.busy(),
    );

    assert_eq!(
        stats.sleep_busy, 0,
        "a task that was only waiting had {} sleep threads busy",
        stats.sleep_busy,
    );

    assert_eq!(
        stats.backlog(),
        0,
        "a task that was only waiting left {} queued",
        stats.backlog(),
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
        stats.sleep_threads,
        stats.sleep_busy,
        stats.queued,
        stats.blocking_queued,
        stats.backlog(),
        stats.live,
        stats.slots,
    );
}

/// Everything the pool is doing, workers and all
///
/// The crate's own `Display` rather than anything written out
/// here. `report` above is the terse version, for the places
/// that want a line rather than a page
fn report_full(at: &str) {
    println!("  [{}]\n{}", at, Runtime::workers());
}

/// Ids come back and get used again, however many go through
///
/// Every wave is joined before the next one starts, so only one
/// wave is ever alive and the table should settle at one wave's
/// worth and never move again. A free list that loses ids leaks
/// them upward instead, and a tag that doesn't defeat ABA hands
/// the same id out twice and corrupts a wave's results
fn recycles_ids_forever() {
    let waves = 16;
    let per_wave = 100_000;

    let wave = || {
        let handles: Vec<_> = (0..per_wave)
            .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn())
            .collect();

        for handle in handles {
            handle.join().expect("every task finishes");
        }
    };

    // The first one is what grows the table to a wave's size,
    // so it is the baseline rather than part of the measurement
    wave();
    let settled = Runtime::workers().slots;
    report("one wave in");

    for _ in 1..waves {
        wave();
    }

    report("all waves through");

    let after = Runtime::workers().slots;

    println!(
        "{} waves of {}: table settled at {} slots, ended at {}",
        waves, per_wave, settled, after,
    );

    // Not exactly equal, because the runtime trims the table
    // by itself and a trim briefly takes the free list out of
    // circulation to walk it. A spawn landing in that window
    // has nothing to reuse and grows the table by one, which is
    // self correcting and nothing to do with recycling
    //
    // The slack is a fraction of a single wave. A free list
    // that genuinely failed to give ids back would grow the
    // table by every task that ever ran, which is two orders of
    // magnitude past this
    let slack = per_wave / 10;

    assert!(
        after <= settled + slack,
        "{} tasks through a table that only ever held {} at once grew it from {} to {}",
        waves * per_wave,
        per_wave,
        settled,
        after,
    );
}

/// A task's output only ever reaches that task's handle
///
/// Every task is given a duration of its own, so a slot handed
/// to two tasks at once, or an id that finds the wrong slot,
/// comes back as a wrong answer rather than as nothing. Spawned
/// from every thread at once, because that is when an id is
/// most likely to be handed out twice
fn never_crosses_two_tasks() {
    let threads = 32;
    let per_thread = 128;

    let barrier = Arc::new(Barrier::new(threads));

    let spawners: Vec<_> = (0..threads)
        .map(|worker| {
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                (0..per_thread)
                    .map(|task| {
                        let micros = (worker * per_thread + task + 1) as u64;
                        let duration = Duration::from_micros(micros);

                        (duration, Runtime::task(Sleep::sleep(duration, true)).spawn())
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    // Read once every thread has finished spawning and before
    // anything is joined, so the queue is at its fullest
    let mut spawned = Vec::with_capacity(threads);

    for spawner in spawners {
        spawned.push(spawner.join().expect("every spawner finishes"));
    }

    report("all spawned, none read");

    let mut checked = 0;

    for batch in spawned {
        for (duration, handle) in batch {
            let slept = handle.join().expect("every task finishes");

            assert!(
                slept >= duration,
                "a task asked for {:?} and came back with {:?}, which is somebody else's",
                duration,
                slept,
            );

            checked += 1;
        }
    }

    println!("{} tasks all came back with their own answer", checked);
}

/// Every way a task can end, all racing on the same tasks
///
/// One thread joins, one takes, one cancels and one just drops,
/// all on clones of the same handles at the same time. Whatever
/// order they land in, the one thing that must never happen is
/// a read coming back with a value that isn't this task's
fn survives_every_ending_at_once() {
    let tasks = 20_000;

    let spawned: Vec<_> = (0..tasks)
        .map(|task| {
            let duration = Duration::from_micros((task % 200 + 1) as u64);

            (duration, Runtime::task(Sleep::sleep(duration, true)).spawn())
        })
        .collect();

    let joiners: Vec<_> = spawned.iter().map(|(at, on)| (*at, on.clone())).collect();
    let takers: Vec<_> = spawned.iter().map(|(at, on)| (*at, on.clone())).collect();
    // Only a third, because cancelling is a single exchange
    // and joining has to wait for the task to actually run. A
    // canceller let loose on all of them wins nearly every race
    // and the reading paths never get exercised at all
    let cancellers: Vec<_> = spawned
        .iter()
        .step_by(3)
        .map(|(_, on)| on.clone())
        .collect();
    let droppers: Vec<_> = spawned.iter().map(|(_, on)| on.clone()).collect();

    let barrier = Arc::new(Barrier::new(4));

    let join_barrier = Arc::clone(&barrier);
    let joining = thread::spawn(move || {
        join_barrier.wait();

        joiners
            .into_iter()
            .map(|(at, on)| (at, on.join()))
            .collect::<Vec<_>>()
    });

    let take_barrier = Arc::clone(&barrier);
    let taking = thread::spawn(move || {
        take_barrier.wait();

        takers
            .into_iter()
            .map(|(at, on)| (at, on.take()))
            .collect::<Vec<_>>()
    });

    let cancel_barrier = Arc::clone(&barrier);
    let cancelling = thread::spawn(move || {
        cancel_barrier.wait();

        for on in cancellers {
            on.cancel();
        }
    });

    let drop_barrier = Arc::clone(&barrier);
    let dropping = thread::spawn(move || {
        drop_barrier.wait();
        drop(droppers);
    });

    // Taken while all four are still going at each other
    report("mid race");

    cancelling.join().expect("the canceller finishes");
    dropping.join().expect("the dropper finishes");

    let read = joining
        .join()
        .expect("the joiner finishes")
        .into_iter()
        .chain(taking.join().expect("the taker finishes"));

    let mut answered = 0;
    let mut refused = 0;

    for (at, result) in read {
        match result {
            // Whatever else the race did, a value that comes
            // back has to be this task's own
            Ok(slept) => {
                assert!(
                    slept >= at,
                    "a task asked for {:?} and came back with {:?}",
                    at,
                    slept,
                );

                answered += 1;
            }

            // The only ways a read is allowed to fail
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::Cancelled) => refused += 1,

            Err(error) => panic!("a read failed with {:?}", error),
        }
    }

    // The originals last, so nothing was freed underneath them
    for (at, on) in spawned {
        if let Ok(slept) = on.join() {
            assert!(slept >= at, "the original handle read {:?} for {:?}", slept, at);
        }
    }

    println!(
        "{} tasks with four endings racing, a third cancelled: {} read, {} refused",
        tasks, answered, refused,
    );

    // Both halves have to have happened, or the race proved
    // nothing about whichever one didn't
    assert!(
        answered > tasks / 4,
        "only {} of {} reads got through the race",
        answered,
        tasks * 2,
    );

    assert!(refused > 0, "not one read was refused, so nothing raced");
}

/// Priority still means priority when the queue is enormous
///
/// The queue is deep enough that its oldest task is starving by
/// any measure, which is exactly when the aging relief used to
/// take over and serve the whole queue oldest first. That is
/// priority inverted rather than aged, so this checks the task
/// asking to go first actually does
fn keeps_priority_under_a_deep_queue() {
    let filler = 400_000;

    let started = Instant::now();

    let queued: Vec<_> = (0..filler)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(20), true)).spawn())
        .collect();

    report("queue filled");

    let asked = Instant::now();
    let urgent = Runtime::task(Sleep::sleep(Duration::from_micros(20), true)).priority(255).spawn();

    // Blocked on rather than polled for. A spin on `ready` puts
    // this thread at the back of a run queue behind every busy
    // worker, so what it measures is its own scheduling quantum
    // rather than anything the pool did
    urgent.join().expect("the urgent task finishes");

    let waited = asked.elapsed();
    report("top priority served");

    for handle in queued {
        handle.join().expect("every task finishes");
    }

    let total = started.elapsed();

    println!(
        "top priority behind {} tasks waited {:?} of the batch's {:?}",
        filler, waited, total,
    );

    // Measured against the batch, so it says the same thing on
    // any machine. Last in and served near the front is the
    // whole promise of asking for a priority
    assert!(
        waited * 8 < total,
        "the top priority task waited {:?} of the batch's {:?}",
        waited,
        total,
    );
}

/// A cancelled sleep gives its thread straight back
///
/// Settling the handle is the easy half and would look the same
/// whether the thread came back or not, because the state moves
/// the moment `cancel` is called. What this actually watches is
/// the pool: a thread still inside a `kevent` call is a thread
/// the pool can't use, so the sleeps have to come off the
/// kernel rather than just be marked as unwanted
///
/// Run here rather than on its own because it counts threads
/// across the whole pool, and the pool is shared by everything
/// in the process
fn cancelling_hands_the_thread_back() {
    let sleeps = 16;
    let patience = Duration::from_secs(5);

    // Counted from whatever the pool was already doing rather
    // than from zero. The drain this test waits on can be
    // satisfied while another test is merely between spawns, so
    // an absolute count is one unrelated sleep away from being
    // wrong
    let before = Runtime::workers().sleep_busy;

    // Long enough that not one of them could have finished by
    // itself, so every thread that comes back was made to
    let handles: Vec<_> = (0..sleeps)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_secs(30), false)).spawn())
        .collect();

    let waiting = Instant::now();

    while Runtime::workers().sleep_busy < before + sleeps && waiting.elapsed() < patience {
        thread::sleep(Duration::from_micros(200));
    }

    let sleeping = Runtime::workers().sleep_busy.saturating_sub(before);
    report("all sleeping");

    assert_eq!(
        sleeping, sleeps,
        "only {} of {} sleeps ever reached a thread",
        sleeping, sleeps,
    );

    let started = Instant::now();

    for handle in handles.iter() {
        handle.clone().cancel();
    }

    while Runtime::workers().sleep_busy > before && started.elapsed() < patience {
        thread::sleep(Duration::from_micros(200));
    }

    let freed = started.elapsed();
    let left = Runtime::workers().sleep_busy.saturating_sub(before);

    report("all cancelled");

    for handle in handles {
        assert_eq!(
            handle.join(),
            Err(RuntimeError::Cancelled),
            "a cancelled task hands nothing out",
        );
    }

    println!(
        "{} sleeps of 30s cancelled, every thread back in {:?}",
        sleeps, freed,
    );

    assert_eq!(
        left, 0,
        "{} threads were still inside a wait after being cancelled",
        left,
    );

    // Nowhere near the thirty seconds they were asked for, so
    // they were taken off the kernel rather than left to run out
    assert!(
        freed < Duration::from_secs(1),
        "threads took {:?} to come back from a cancel",
        freed,
    );
}

/// As many tasks alive at once as will comfortably fit
///
/// Every handle is held, so every slot stays live and the table
/// has to grow to hold all of them. This is the only thing here
/// that says what a task actually costs, and the only one that
/// pushes the table into its higher blocks
fn holds_a_peak_of_live_tasks() {
    let tasks = 12_000_000;

    let baseline = max_rss();

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn())
        .collect();

    let peak = max_rss();
    let stats = Runtime::workers();

    report_full("all live, none read");

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    println!(
        "{} live tasks: {} bytes resident, {} each, was {} before, table at {} slots",
        tasks,
        peak,
        peak / tasks,
        baseline,
        stats.slots,
    );

    // A handle keeps its task's slot alive, so this many live
    // handles means this many slots had to be handed out
    assert!(
        stats.slots >= tasks,
        "{} live handles but the table only handed out {} slots",
        tasks,
        stats.slots,
    );

    // Measured against the whole process rather than against
    // what it grew by. `max_rss` is a high water mark, so a
    // baseline taken part way through a run already carries
    // every peak before it and subtracting it hides most of
    // what this phase actually cost
    //
    // #### Note
    // Resident only, and it reads well under what a task costs.
    // A slot is 64 bytes of header, 16 of `Duration` and 48
    // never written, so the pages are mostly zeros and macOS
    // compresses them. Activity Monitor counts what the
    // compressor is holding and this doesn't, so the honest
    // figure is `SLOT_SIZE` plus the handle and this number is
    // a floor under it
    //
    // The count of slots above is the assertion that means
    // something. This one is a crash guard: a slot back to
    // owning a page would be sixteen kilobytes each, and the
    // machine would go down before the threshold was reached
    assert!(
        peak < 5 * 1024 * 1024 * 1024,
        "{} live tasks put the process at {} bytes",
        tasks,
        peak,
    );
}

/// Online cores, which the pool sizes itself against
fn cores() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}

/// The high water mark of the process's resident memory
fn max_rss() -> usize {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };

    usage.ru_maxrss as usize
}

/// A timeout that isn't needed costs nothing
///
/// The regression this exists for: `join_with_timeout` used to
/// sleep out the whole duration and only then look, so a task
/// that finished immediately still held its caller for the full
/// timeout. It gave the right answer at the worst possible
/// moment, and every assertion about the *value* passed while
/// it did
#[test]
fn join_with_timeout_returns_as_soon_as_the_task_does() {
    Runtime::init();

    let timeout = Duration::from_secs(10);
    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(20), false)).spawn();

    let started = Instant::now();
    let result = handle.join_with_timeout(timeout);
    let waited = started.elapsed();

    assert!(result.is_ok(), "the task finished, so it reads: {result:?}");

    println!("waited {waited:?} of a {timeout:?} timeout");

    // Generous on purpose. The point is the difference between
    // "as long as the task took" and "as long as the timeout
    // was", which is three orders of magnitude here — not
    // whether a loaded machine took an extra millisecond
    assert!(
        waited < Duration::from_secs(1),
        "came back after {waited:?}, which is the timeout being waited out rather than the task",
    );
}

/// A timeout that is needed is roughly the timeout
#[test]
fn join_with_timeout_gives_up_near_its_deadline() {
    Runtime::init();

    let timeout = Duration::from_millis(100);
    let handle = Runtime::task(Sleep::sleep(Duration::from_secs(5), false)).spawn();

    let started = Instant::now();
    let result = handle.join_with_timeout(timeout);
    let waited = started.elapsed();

    assert_eq!(
        result,
        Err(RuntimeError::NotReady),
        "nowhere near long enough, and it says so",
    );

    println!("gave up after {waited:?} against a {timeout:?} timeout");

    assert!(waited >= timeout, "came back early, after only {waited:?}");

    // A spurious wake used to be able to restart the whole
    // timeout. The budget is recomputed each pass now, so a
    // stream of them can't push the deadline out
    assert!(
        waited < timeout * 10,
        "took {waited:?} over a {timeout:?} timeout, so something is restarting the wait",
    );

    handle.cancel();
}

/// Polling a task whose output can't be cloned
///
/// `maybe_join` needs `Clone` and `take` blocks, so before
/// `maybe_take` there was no way to look at one of these
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

    // Borrowed rather than consumed, so the handle is still
    // here to say what happened to the value
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

    // Nothing was claimed, so the value is still there for
    // whoever asks next
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

    // Read nothing and claimed nothing, so everything is still
    // available afterwards
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

    // Settled is not the same as having something to hand out,
    // which is the whole reason both questions exist
    assert!(
        !cancelled.is_ready(),
        "a cancelled task has settled and has nothing to give",
    );

    let taken = Runtime::task(Sleep::sleep(Duration::from_millis(20), false)).spawn();
    let watcher = taken.clone();
    taken.take().expect("the value moves out");

    assert_eq!(watcher.state(), TaskState::Taken);
    assert!(watcher.is_taken());

    // Never observable through a handle. The `Executor` filters
    // an empty slot out and answers `Failed` in its place
    for handle in [&ready, &cancelled, &watcher] {
        assert_ne!(handle.state(), TaskState::Free, "a live handle is never free");
    }
}

/// A bare chain is a task that runs once, now
///
/// The floor of the builder: nothing set, nothing scheduled,
/// and the same one shot the runtime has always had
#[test]
fn bare_chain_runs_once_now() {
    Runtime::init();

    let duration = Duration::from_millis(50);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(duration, false)).spawn();
    let slept = handle.join().expect("it finishes");

    println!("slept {slept:?} against {duration:?}");

    assert!(slept >= duration, "slept {slept:?}, which is short");

    // Once, and no more. A repeat would have published again by
    // now and this would read something rather than nothing
    assert!(
        started.elapsed() < duration * 10,
        "a bare chain took far longer than one run of it",
    );
}

/// Priority set through the builder reaches the injector
///
/// Measured the way `high_priority_runs_first` measures it:
/// against the batch rather than against a fixed number, so it
/// says the same thing on a fast machine as on a slow one
#[test]
fn builder_priority_reaches_the_band() {
    Runtime::init();

    // Smaller than `high_priority_runs_first`, which measures
    // the same property. Two 50,000 task batches running beside
    // each other saturate the pool and slow the whole suite for
    // no extra confidence
    let tasks = 10_000;
    let started = Instant::now();

    let queued: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50), true)).spawn())
        .collect();

    // Last in, and served first anyway
    let queued_at = Instant::now();
    let urgent = Runtime::task(Sleep::sleep(Duration::from_micros(50), true))
        .priority(255)
        .spawn();

    while !urgent.settled() {
        thread::yield_now();
    }

    let waited = queued_at.elapsed();

    for handle in queued {
        handle.join().expect("every task finishes");
    }

    let total = started.elapsed();

    println!(
        "urgent task waited {:?}, the {} before it took {:?}",
        waited, tasks, total,
    );

    assert!(
        waited * 4 < total,
        "the top priority task waited {waited:?} of the batch's {total:?}",
    );
}

/// A repeat built at a priority is still a repeat
///
/// The combination the builder exists for: there is no
/// `repeating_with_priority`, and before the builder there was
/// no way to ask for one at all
#[test]
fn builder_repeats_at_a_priority() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false))
        .priority(200)
        .repeat()
        .spawn();

    // A repeat settles between runs rather than at the end, so
    // reading it twice is how you know it went round
    let first = handle.take_with_timeout(Duration::from_secs(5));

    // Retried rather than read straight off. A repeat sits in
    // `Taken` for the window between a read and its next run
    // starting, and a read landing in that window comes back
    // `AlreadyTaken` rather than waiting — so reading once and
    // asserting on it is a coin toss
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut second = Err(RuntimeError::AlreadyTaken);

    while Instant::now() < deadline {
        second = handle.take_with_timeout(Duration::from_millis(100));

        if second.is_ok() {
            break;
        }
    }

    // Before the assertions, always. A repeating task is held
    // by the `Executor` for the life of the series, so dropping
    // the handle on a panic doesn't stop it — it would just run
    // for the rest of the process
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

    // Given longest-last, so the results coming back in
    // ascending order is the order being kept rather than the
    // order they happened to finish in
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
    assert!(status.initialised);
    assert!(!status.shut_down, "nothing has shut this down");

    // Both supervisors are independent, and this test only
    // claims what it can see: a runtime nothing has knocked
    // over has both of them
    assert!(status.reactor_alive, "the reactor is up");
    assert!(status.manager_alive, "the manager is up");
    assert!(status.healthy());
}

/// A delayed task waits, then runs
#[test]
fn after_waits_before_it_runs() {
    Runtime::init();

    let delay = Duration::from_millis(200);
    let started = Instant::now();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).after(delay).spawn();

    // Not started, and not finished either — it is sitting on
    // a timer rather than anywhere in the pool
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

/// A delay that is cancelled never runs at all
#[test]
fn after_can_be_cancelled_before_it_starts() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(10), false)).after(Duration::from_millis(300)).spawn();

    let watcher = handle.clone();
    handle.cancel();

    let started = Instant::now();
    let result = watcher.join();
    let waited = started.elapsed();

    assert_eq!(
        result,
        Err(RuntimeError::Cancelled),
        "a task cancelled before its delay was up never ran",
    );

    // The cancel lands for readers straight away, even though
    // the slot itself isn't given back until the timer would
    // have fired anyway
    assert!(
        waited < Duration::from_millis(200),
        "the reader waited {waited:?}, so the cancel didn't land until the delay did",
    );
}

/// A delay costs a slot and no thread
///
/// Far more delayed tasks than the pool could ever hold threads
/// for. If a delay tied one up, these would not all get through
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

    // Asserted against the delay rather than against a worker
    // count, because the pool is process wide and whatever else
    // is running shares it
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

    // Still a one shot, whatever else was set on it
    handle.take().expect("the output is there");
}

/// A delay and a repeat compose rather than replacing each other
///
/// Entering a state never clears anything, so a delay set
/// before the kind survives the kind being chosen. This waits
/// the delay out once and then repeats on its gap — the two
/// durations are different things and the slot holds both
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

    // Cancelled before the assertion. A repeat is held by the
    // `Executor` for the life of the series, so a panic here
    // would leave it running for the rest of the process
    handle.cancel();

    assert!(
        first >= delay,
        "the first run came after {first:?}, so the delay was lost when the kind was set",
    );
}

/// Drains a bounded series, counting what it published
///
/// Polls faster than the gap, so no run publishes and is
/// overwritten between two looks. Stops on the read that comes
/// back `Finished`, which is the whole reason that variant
/// exists — `AlreadyTaken` alone can't tell "the next run will
/// publish" from "there is no next run"
fn drain(handle: &TaskHandle<Duration>, patience: Duration) -> usize {
    let deadline = Instant::now() + patience;
    let mut seen = 0;

    while Instant::now() < deadline {
        match handle.maybe_take() {
            Ok(_) => seen += 1,

            // Between runs, or a run still going. Either way
            // there is another output coming
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::NotReady) => {
                thread::sleep(Duration::from_millis(1))
            }

            // `Finished` and every other error are endings. The
            // read that found nothing is the same read that
            // said why, so there is no window between the two
            // for the series to end in
            Err(_) => break,
        }
    }

    seen
}

/// A count runs exactly that many times
#[test]
fn count_runs_exactly_that_many_times() {
    Runtime::init();

    let runs = 5;

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(Duration::from_millis(30))
        .count(runs)
        .spawn();

    let seen = drain(&handle, Duration::from_secs(10));

    println!("saw {seen} runs against a count of {runs}");

    assert!(handle.is_finished(), "the series never reported finishing");
    assert_eq!(seen as u32, runs, "saw {seen} runs, not {runs}");

    // Ran out rather than fell over. A bounded series that
    // reaches its ending has succeeded, so nothing about it
    // reads as a failure
    assert!(!handle.is_failed(), "running out is not failing");
}

/// A run that would begin past the deadline is never begun
///
/// The case most likely to be got wrong. On a 750ms gap
/// bounded to a second there is time for a run at 0ms and one
/// at 750ms, and a third would land near 1500ms — so the answer
/// is two, not one and not three
#[test]
fn for_duration_stops_before_the_run_that_would_overrun() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(Duration::from_millis(750))
        .for_duration(Duration::from_secs(1))
        .spawn();

    let seen = drain(&handle, Duration::from_secs(10));

    println!("a 750ms gap bounded to 1s ran {seen} times");

    assert!(handle.is_finished());
    assert_eq!(seen, 2, "expected the runs at 0ms and 750ms and no more");
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

/// A count and a deadline end at whichever comes first
#[test]
fn count_and_deadline_end_at_whichever_is_first() {
    Runtime::init();

    let gap = Duration::from_millis(20);

    // The count is the tighter of the two — three runs at a
    // 20ms gap is nowhere near the ten seconds allowed
    let counted = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(gap)
        .count(3)
        .for_duration(Duration::from_secs(10))
        .spawn();

    // And here the deadline is, with a count far out of reach
    let timed = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(gap)
        .count(1_000_000)
        .for_duration(Duration::from_millis(120))
        .spawn();

    let by_count = drain(&counted, Duration::from_secs(10));
    let by_time = drain(&timed, Duration::from_secs(10));

    println!("count won at {by_count} runs, deadline won at {by_time} runs");

    assert_eq!(by_count, 3, "the count should have ended this one");

    // Bounded by the clock rather than by a number, so this
    // says what it can honestly say: it stopped, and it stopped
    // well short of the million it was allowed
    assert!(timed.is_finished(), "the deadline never ended it");
    assert!(
        by_time > 0 && by_time < 1_000,
        "{by_time} runs is not a 120ms window at a 20ms gap",
    );
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

    // Runs of a schedule overlap by design, so two finishing
    // together publish one output between them. What is
    // promised is how many *start*, and no more than that can
    // ever be seen
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

    // Over is over, however it got there. The kind still says
    // it repeats, which is exactly why the state alone can't
    // answer this
    assert!(
        watcher.is_finished(),
        "a cancelled series is not going to run again",
    );

    watcher.cancel();
}

/// A repeat with no bound is not finished between runs
///
/// The other side of `is_finished`, and the reason it can't
/// just be `settled`
#[test]
fn an_unbounded_repeat_is_never_finished() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(5), false))
        .repeat()
        .spawn();

    handle.wait().expect("a run publishes");

    let settled = handle.settled();
    let finished = handle.is_finished();

    handle.cancel();

    assert!(settled, "it published, so it has settled");
    assert!(!finished, "but there is another run coming, so it isn't over");
}

/// A drained series says it is over, in the read that fails
///
/// The distinction `AlreadyTaken` can't draw. Both of these end
/// with an output that has been moved out and a slot in the
/// same state — what separates them is whether anything is
/// coming after it
#[test]
fn finished_and_already_taken_are_different_endings() {
    Runtime::init();

    // A one shot. Somebody beat the second reader to it, and
    // that is all this means
    let once = Runtime::task(Sleep::sleep(Duration::from_millis(5), false)).spawn();
    let watcher = once.clone();

    once.take().expect("the value moves out");

    assert_eq!(
        watcher.maybe_take(),
        Err(RuntimeError::AlreadyTaken),
        "a one shot says somebody was first, not that a series ended",
    );

    // A bounded repeat, drained to the end
    let bounded = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(Duration::from_millis(10))
        .count(3)
        .spawn();

    let seen = drain(&bounded, Duration::from_secs(10));

    assert_eq!(seen, 3, "saw {seen} of 3 runs");

    assert_eq!(
        bounded.maybe_take(),
        Err(RuntimeError::Finished),
        "a series that ran out says so rather than looking like a lost race",
    );

    // Still not a failure. Running out is how a bounded series
    // succeeds, and the two must not be confused
    assert!(bounded.is_finished());
    assert!(!bounded.is_failed());
}

/// An unbounded repeat never reports `Finished`
///
/// The other side of it: this one really is a lost race, and
/// reading again really will find the next run
#[test]
fn an_unbounded_repeat_reports_a_lost_race() {
    Runtime::init();

    let handle = Runtime::task(Sleep::sleep(Duration::from_millis(1), false))
        .repeat()
        .every(Duration::from_millis(50))
        .spawn();

    handle.wait().expect("a run publishes");
    handle.maybe_take().expect("and the first reader gets it");

    let second = handle.maybe_take();

    handle.cancel();

    assert_eq!(
        second,
        Err(RuntimeError::AlreadyTaken),
        "there is another run coming, so this is a race rather than an ending",
    );
}
