use atap::{Runtime, RuntimeError, Sleep, TaskHandle};
use std::{
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

    let handle = Runtime::spawn(Sleep::sleep(Duration::from_secs(2), true));

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

    let first = Runtime::spawn(Sleep::sleep(duration, false));
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

    let first = Runtime::spawn(Sleep::sleep(quick, false));
    let second = Runtime::spawn(Sleep::sleep(slow, false));

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

    let first = Runtime::spawn(Sleep::sleep(Duration::from_millis(200), false));
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

    let first = Runtime::spawn(Sleep::sleep(Duration::from_secs(1), false));
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

    let handle = Runtime::spawn(Sleep::sleep(Duration::from_secs(1), false));
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
    let handle = Runtime::spawn(Sleep::sleep(duration, false));

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
    let handle = Runtime::repeating(Sleep::sleep(Duration::from_millis(5), false));

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

    let handle = Runtime::repeating(Sleep::sleep(duration, false));

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
    let handle = Runtime::repeat_every(interval, Sleep::sleep(Duration::from_nanos(1), true));

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
fn cancelling_a_spawned_task_settles_every_listener() {
    Runtime::init();

    // Long enough that it could not possibly have finished on
    // its own by the time anything below is checked
    let handle = Runtime::spawn(Sleep::sleep(Duration::from_secs(30), false));
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

                        (duration, Runtime::spawn(Sleep::sleep(duration, false)))
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
        let handle = Runtime::spawn(Sleep::sleep(Duration::from_nanos(1), true));
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
        let handle = Runtime::spawn(Sleep::sleep(Duration::from_nanos(500), true));

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
        let handle = Runtime::spawn(Sleep::sleep(Duration::from_nanos(500), true));

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
        .map(|_| Runtime::spawn(Sleep::sleep(duration, false)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(duration, false)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(duration, false)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_millis(200), false)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_micros(50), true)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_micros(50), true)))
        .collect();

    // Last in, and served first anyway
    let queued_at = Instant::now();
    let urgent = Runtime::spawn_with_priority(Sleep::sleep(Duration::from_micros(50), true), 255);

    while !urgent.ready() {
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

    println!("\n== a peak of live tasks ==");
    holds_a_peak_of_live_tasks();

    println!("\n== a repeating task holds one slot ==");
    repeating_holds_one_slot();

    println!("\n== waiting costs no thread ==");
    waiting_costs_no_thread();

    println!("\n== the table gives its pages back ==");
    gives_the_table_back();

    println!();
    report("finished");
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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_nanos(1), true)))
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

        // Paused rather than spun. A bare loop turns the gap
        // between runs into millions of clones of the same
        // handle, which says nothing about repeating and a
        // great deal about how hard the reference count can be
        // hammered
        thread::sleep(Duration::from_micros(100));
    }
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

    let handle = Runtime::repeating(Sleep::sleep(Duration::from_nanos(1), true));

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

    let handle = Runtime::repeat_every(interval, Sleep::sleep(Duration::from_nanos(1), true));

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

/// Every worker's own ring and count, one to a line
fn report_workers(at: &str) {
    let stats = Runtime::workers();

    println!("  [{}] {} workers:", at, stats.len());

    for (index, worker) in stats.workers.iter().enumerate() {
        println!(
            "    worker {}: {}, {} queued, {} done",
            index,
            match worker.busy {
                true => "busy",
                false => "idle",
            },
            worker.backlog,
            worker.completed,
        );
    }
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
            .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_nanos(1), true)))
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

                        (duration, Runtime::spawn(Sleep::sleep(duration, true)))
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

            (duration, Runtime::spawn(Sleep::sleep(duration, true)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_micros(20), true)))
        .collect();

    report("queue filled");

    let asked = Instant::now();
    let urgent = Runtime::spawn_with_priority(Sleep::sleep(Duration::from_micros(20), true), 255);

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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_secs(30), false)))
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
        .map(|_| Runtime::spawn(Sleep::sleep(Duration::from_nanos(1), true)))
        .collect();

    let peak = max_rss();
    let stats = Runtime::workers();

    report("all live, none read");
    report_workers("all live, none read");

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
