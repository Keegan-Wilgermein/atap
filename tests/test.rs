use std::{
    thread,
    time::{Duration, Instant},
};
use atap::{Runtime, RuntimeError, Sleep};

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
    let result = Runtime::block(
        Sleep::sleep(duration, true),
    );

    println!("Result: {:?}", result);

    let std = handle.join().unwrap();

    println!("\nDiff: {:?}", std - result);
    println!("std error:{:?}\natap error:{:?}\n", std - duration, result - duration)
}

#[test]
fn sleep_multi_threaded_blocking() {
    Runtime::init();

    let threads = 10;

    (1..=threads).into_iter()
    .for_each(|i| {
        thread::spawn(move || {
            let duration = Duration::from_secs(i);
    
            let time = Runtime::block(
                Sleep::sleep(
                    duration ,i % 2 == 0,
                )
            );
    
            let error = time - duration;
            println!("Thread {} slept for {:?}\n{:?} error\n", i, time, error);
        });
    });

    Runtime::block(
        Sleep::sleep(
            Duration::from_secs(threads + 2), false,
        )
    );
}

#[test]
fn single_spawned_task() {
    Runtime::init();

    let handle = Runtime::spawn(
        Sleep::sleep(Duration::from_secs(2), true),
    );

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

    println!("baseline {} bytes, after {} bytes, growth {} bytes", baseline, after, growth);

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

/// The high water mark of the process's resident memory
fn max_rss() -> usize {
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) };

    usage.ru_maxrss as usize
}
