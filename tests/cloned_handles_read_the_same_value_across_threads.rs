use atap::{Runtime, Sleep, SleepMode};
use std::thread;
use std::time::Duration;
use std::sync::{Arc, Barrier};

/// Clones of a handle read the same value from any thread
#[test]
fn cloned_handles_read_the_same_value_across_threads() {
    Runtime::init();

    let threads = 16;
    let rounds = 32;

    // Two tasks far enough apart that reading the wrong one gives
    // a wrong answer
    let quick = Duration::from_millis(300);
    let slow = Duration::from_millis(700);

    let first = Runtime::task(Sleep::sleep(quick).mode(SleepMode::Relaxed)).spawn();
    let second = Runtime::task(Sleep::sleep(slow).mode(SleepMode::Relaxed)).spawn();

    let barrier = Arc::new(Barrier::new(threads));

    let readers: Vec<_> = (0..threads)
        .map(|thread| {
            // Half the threads on each task
            let handle = match thread % 2 == 0 {
                true => first.clone(),
                false => second.clone(),
            };

            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                // Every thread starts together, so clones and reads overlap
                barrier.wait();

                let mut seen = Vec::with_capacity(rounds);
                let mut held = Vec::new();

                for round in 0..rounds {
                    let copy = handle.clone();

                    // Half read straight away and half are kept back
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

    // The originals last, so the output outlived every clone
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
