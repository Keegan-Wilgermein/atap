//! # Every phase in one process, in order
//!
//! Several phases read the process wide `live` count, so they
//! run one after another rather than side by side

mod common;

use atap::{File, JoinPolicy, Runtime, RuntimeError, Sleep, SleepMode};
use common::{report, take_a_run};
use std::{
    fs,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, Instant},
};

/// Runs every phase below, one after another
#[test]
fn monolithic() {
    Runtime::init();

    report("starting");

    println!("\n== tasks never cross ==");
    never_crosses_two_tasks();

    println!("\n== every ending at once ==");
    survives_every_ending_at_once();

    println!("\n== priority under a deep queue ==");
    keeps_priority_under_a_deep_queue();

    println!("\n== losing the manager ==");
    survives_losing_its_manager();

    println!("\n== a repeating task holds one slot ==");
    repeating_holds_one_slot();

    println!("\n== a schedule gives its run slots back ==");
    every_gives_its_run_slots_back();

    println!("\n== outputs that own memory are dropped ==");
    file_outputs_are_dropped_not_leaked();

    println!("\n== a race picks one and settles the rest ==");
    join_first_settles_every_loser();

    println!();
    report("finished");
}

/// Thousands of races give every slot back, whichever way the
/// losers end
fn join_first_settles_every_loser() {
    let races = 512;
    let width = 8;

    let base = settled_live();

    // All three policies, in rotation
    for race in 0..races {
        let quick = Runtime::task(Sleep::sleep(Duration::from_nanos(1))).spawn();

        let slow: Vec<_> = (0..width)
            .map(|_| Runtime::task(Sleep::sleep(Duration::from_millis(10)).mode(SleepMode::Relaxed)).spawn())
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

                // Handed back and then let go without being read
                drop(losers);
            }
            None => assert_ne!(policy, JoinPolicy::PassBack, "PassBack handed back nothing"),
        }
    }

    report("races run");

    // Waited for, since a backlog that hasn't started draining
    // looks just as still as one that has finished
    let waited = Instant::now();

    while waited.elapsed() < Duration::from_secs(30) {
        let now = Runtime::workers();

        if !now.has_any_task() && now.live() <= base + 8 {
            break;
        }

        thread::sleep(Duration::from_millis(20));
    }

    let after = Runtime::workers().live();

    println!("  {} races of {}, live {} -> {}", races, width + 1, base, after);

    assert!(
        after <= base + 8,
        "{} live tasks after {} races against {} before them",
        after,
        races,
        base,
    );
}

/// Outputs that own memory are dropped, whether they are
/// read, dropped unread, or replaced by a repeat's next run
fn file_outputs_are_dropped_not_leaked() {
    let reads = 2048;
    let size = 16 * 1024;
    let runs = 64;

    let path = fixture("monolithic-outputs", size);

    // Read once the phase before has wound down
    let base = settled_live();
    let before = Runtime::workers();

    let handles: Vec<_> = (0..reads)
        .map(|_| Runtime::task(File::read(&path)).spawn())
        .collect();

    let mut taken = 0;
    let mut dropped = 0;

    for (index, handle) in handles.into_iter().enumerate() {
        // Settled first, so the unread half is dropped holding a
        // whole output
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

    // Nothing reads any of these runs
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

    assert!(
        after <= base + 8,
        "{} live tasks after the file phase against {} before it",
        after,
        base,
    );

    assert!(
        stats.peak_slots() >= before.peak_slots(),
        "the table lost slots it had already handed out",
    );

    let _ = fs::remove_file(&path);
}

/// Writes a file of `size` bytes and gives back its path
fn fixture(name: &str, size: usize) -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    let path = root.join(format!("{}-{}.txt", name, std::process::id()));
    let body: Vec<u8> = (0..size).map(|index| (index % 251) as u8).collect();

    fs::write(&path, body).expect("could not write the fixture");

    path
}

/// The pool works through a backlog while the manager is down,
/// and the manager comes back
///
/// Every phase after this one runs on a manager that has been
/// killed and rebuilt
fn survives_losing_its_manager() {
    let tasks = 200_000;
    let quick = || Sleep::sleep(Duration::from_nanos(1));

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

/// A repeating task lives in one slot however long it runs
fn repeating_holds_one_slot() {
    let runs = 20_000;

    let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(1))).repeat().spawn();

    // The first one, so the series is under way before anything
    // is measured
    take_a_run(&handle);

    // A cancelled repeat from the phase before keeps its slot
    // until its next timer
    settled_live();

    let before = Runtime::workers();

    for _ in 1..runs {
        take_a_run(&handle);
    }

    let after = Runtime::workers();

    handle.clone().cancel();

    println!(
        "{} runs through one handle: {} -> {} slots, {} -> {} live",
        runs, before.peak_slots(), after.peak_slots(), before.live(), after.live(),
    );

    // A slot per run would be twenty thousand of them
    assert!(
        after.peak_slots() <= before.peak_slots() + 100,
        "{} runs grew the table from {} slots to {}",
        runs,
        before.peak_slots(),
        after.peak_slots(),
    );

    // Held for the life of the series and given back once
    assert_eq!(
        after.live(), before.live(),
        "{} runs took the live count from {} to {}",
        runs, before.live(), after.live(),
    );
}

/// A schedule hands back every slot its runs used
fn every_gives_its_run_slots_back() {
    let schedules = 32;
    let interval = Duration::from_millis(5);
    let running = Duration::from_millis(500);

    // Read once the phase before has wound down
    settled_live();

    let before = Runtime::workers();

    // Instant runs on a short period, so slots come and go fast
    let handles: Vec<_> = (0..schedules)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1))).at_rate(interval).spawn())
        .collect();

    // Counted, so a schedule that quietly stopped fails
    let mut runs = 0u64;

    let started = Instant::now();

    while started.elapsed() < running {
        for handle in &handles {
            // Taken rather than read, so each one counted is a run
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

    // Waited out rather than timed, since a loaded schedule can
    // have hundreds of runs outstanding when it is cancelled
    let settling = Instant::now();

    while Runtime::workers().live() > before.live() && settling.elapsed() < Duration::from_secs(10) {
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
        before.live(),
        peak.live(),
        after.live(),
        before.peak_slots(),
        peak.peak_slots(),
        after.peak_slots(),
    );

    assert!(
        runs > 0,
        "{} schedules produced nothing at all in {:?}",
        schedules,
        running,
    );

    // Only ever a handful alive at a time, nothing like a slot
    // per run
    assert!(
        peak.live() <= before.live() + schedules * 8,
        "{} schedules took the live count from {} to {} while running",
        schedules,
        before.live(),
        peak.live(),
    );

    // Every schedule slot and every run slot back
    assert!(
        after.live() <= before.live(),
        "{} schedules took the live count from {} to {}",
        schedules,
        before.live(),
        after.live(),
    );
}

/// Waits for the table's live count to stop moving
///
/// ## Returns
/// The count once two reads in a row agreed on it, or whatever
/// it was when the wait ran out
///
/// #### Note
/// The reads are further apart than the longest interval any
/// phase here leaves a cancelled repeat on
fn settled_live() -> usize {
    let waited = Instant::now();
    let mut last = Runtime::workers().live();

    while waited.elapsed() < Duration::from_secs(5) {
        thread::sleep(Duration::from_millis(100));

        let now = Runtime::workers().live();

        if now == last {
            return now;
        }

        last = now;
    }

    last
}

/// Tasks spawned from every thread at once only ever read
/// their own output
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

                        (duration, Runtime::task(Sleep::sleep(duration)).spawn())
                    })
                    .collect::<Vec<_>>()
            })
        })
        .collect();

    // Read once every thread has finished spawning and before
    // anything is joined
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

/// Joins, takes, cancels and drops all racing on the same
/// tasks never read another task's value
fn survives_every_ending_at_once() {
    let tasks = 20_000;

    let spawned: Vec<_> = (0..tasks)
        .map(|task| {
            let duration = Duration::from_micros((task % 200 + 1) as u64);

            (duration, Runtime::task(Sleep::sleep(duration)).spawn())
        })
        .collect();

    let joiners: Vec<_> = spawned.iter().map(|(at, on)| (*at, on.clone())).collect();
    let takers: Vec<_> = spawned.iter().map(|(at, on)| (*at, on.clone())).collect();
    // Only a third, or the canceller wins nearly every race
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
            // A value that comes back has to be this task's own
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

    // Both reads and refusals have to have happened
    assert!(
        answered > tasks / 4,
        "only {} of {} reads got through the race",
        answered,
        tasks * 2,
    );

    assert!(refused > 0, "not one read was refused, so nothing raced");
}

/// A high priority task is served ahead of a queue deep enough
/// to be starving
fn keeps_priority_under_a_deep_queue() {
    let filler = 400_000;

    let started = Instant::now();

    let queued: Vec<_> = (0..filler)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(20))).spawn())
        .collect();

    report("queue filled");

    let asked = Instant::now();
    let urgent = Runtime::task(Sleep::sleep(Duration::from_micros(20))).priority(255).spawn();

    // Blocked on rather than polled for, so this thread's own
    // scheduling isn't what gets measured
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

    assert!(
        waited * 8 < total,
        "the top priority task waited {:?} of the batch's {:?}",
        waited,
        total,
    );
}
