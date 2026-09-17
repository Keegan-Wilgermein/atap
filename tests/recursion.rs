//! Recursion tests
//!
//! Computes that spawn, join, give to and block on other tasks from
//! inside themselves. Each test checks only the values its own tasks
//! come back with, so they share a binary and run side by side on
//! purpose

mod common;

use atap::{JoinPolicy, Runtime, RuntimeError, TaskHandle, compute::Compute};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    time::Duration,
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(60);

/// Fibonacci the slow way, every call its own task joined by the call
/// above it
fn fibonacci(n: u64) -> u64 {
    if n < 2 {
        return n;
    }

    let left = Runtime::task(Compute::compute(move |()| fibonacci(n - 1))).spawn();
    let right = Runtime::task(Compute::compute(move |()| fibonacci(n - 2))).spawn();

    left.join().expect("a left branch failed") + right.join().expect("a right branch failed")
}

/// The same answer the plain way
fn fibonacci_plain(n: u64) -> u64 {
    let (mut a, mut b) = (0u64, 1u64);

    for _ in 0..n {
        (a, b) = (b, a + b);
    }

    a
}

/// A compute that spawns and joins a child hands back the child's
/// answer
#[test]
fn a_compute_joins_a_child_it_spawned() {
    let _ = Runtime::init();

    let parent = Runtime::task(Compute::compute(|()| {
        let child = Runtime::task(Compute::compute(|()| 20)).spawn();

        child.join().expect("the child failed") + 1
    }))
    .spawn();

    assert_eq!(parent.join_with_timeout(PATIENCE), Ok(21));
}

/// A task tree thousands of tasks wide and twenty levels deep comes
/// back with the right answer
#[test]
fn a_recursive_split_comes_back_right() {
    let _ = Runtime::init();

    for n in [1, 5, 12, 20] {
        let root = Runtime::task(Compute::compute(move |()| fibonacci(n))).spawn();

        assert_eq!(
            root.join_with_timeout(PATIENCE),
            Ok(fibonacci_plain(n)),
            "fibonacci {} split into tasks came back wrong",
            n,
        );
    }
}

/// A chain far deeper than a worker helps comes back with every
/// level counted
#[test]
fn a_chain_deeper_than_help_goes_comes_back() {
    let _ = Runtime::init();

    fn chain(depth: usize) -> usize {
        if depth == 0 {
            return 0;
        }

        Runtime::task(Compute::compute(move |()| chain(depth - 1)))
            .spawn()
            .join()
            .expect("a link failed")
            + 1
    }

    let root = Runtime::task(Compute::compute(|()| chain(300))).spawn();

    assert_eq!(root.join_with_timeout(PATIENCE), Ok(300));
}

/// Loops of spawns inside a compute all land
#[test]
fn a_loop_of_spawns_inside_a_compute_all_land() {
    let _ = Runtime::init();

    let parent = Runtime::task(Compute::compute(|()| {
        let children: Vec<_> = (0..2_000u64)
            .map(|index| Runtime::task(Compute::compute(move |()| index * 3)).spawn())
            .collect();

        Runtime::join_all(children)
            .into_iter()
            .map(|result| result.expect("a child failed"))
            .sum::<u64>()
    }))
    .spawn();

    assert_eq!(
        parent.join_with_timeout(PATIENCE),
        Ok((0..2_000u64).map(|index| index * 3).sum())
    );
}

/// Many parents each spawning their own loop of children, all at once
#[test]
fn many_parents_spawning_loops_at_once() {
    let _ = Runtime::init();

    let parents: Vec<_> = (0..64u64)
        .map(|parent| {
            Runtime::task(Compute::compute(move |()| {
                let children: Vec<_> = (0..64u64)
                    .map(|child| {
                        Runtime::task(Compute::compute(move |()| parent * 1_000 + child)).spawn()
                    })
                    .collect();

                Runtime::join_all(children)
                    .into_iter()
                    .map(|result| result.expect("a child failed"))
                    .sum::<u64>()
            }))
            .spawn()
        })
        .collect();

    for (parent, handle) in parents.into_iter().enumerate() {
        let parent = parent as u64;
        let wanted: u64 = (0..64u64).map(|child| parent * 1_000 + child).sum();

        assert_eq!(
            handle.join_with_timeout(PATIENCE),
            Ok(wanted),
            "parent {} came back with somebody else's children",
            parent,
        );
    }
}

/// Blocking inside a compute runs on the worker itself
#[test]
fn blocking_inside_a_compute_runs_there() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| {
        let here = std::thread::current().id();
        let there = Runtime::block(Compute::compute(|()| std::thread::current().id()));

        (here == there, Runtime::block(Compute::compute(|()| 5)) + 1)
    }))
    .spawn();

    assert_eq!(handle.join_with_timeout(PATIENCE), Ok((true, 6)));
}

/// Blocking on a compute that spawns and joins, inside a compute
#[test]
fn a_block_that_spawns_inside_a_compute() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| {
        Runtime::block(Compute::compute(|()| {
            Runtime::task(Compute::compute(|()| 40))
                .spawn()
                .join()
                .expect("the inner child failed")
        })) + 2
    }))
    .spawn();

    assert_eq!(handle.join_with_timeout(PATIENCE), Ok(42));
}

/// A race run from inside a compute picks a winner and settles
#[test]
fn a_race_inside_a_compute_picks_a_winner() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| {
        let racers: Vec<_> = (0..8u64)
            .map(|index| {
                Runtime::task(Compute::compute(move |()| {
                    std::thread::sleep(Duration::from_millis(index * 5));
                    index
                }))
                .spawn()
            })
            .collect();

        let (first, _) = Runtime::join_first(racers, JoinPolicy::Cancel);

        first.join().expect("the winner failed")
    }))
    .spawn();

    let winner = handle
        .join_with_timeout(PATIENCE)
        .expect("the racing compute failed");

    assert!(winner < 8, "the race picked {} out of nowhere", winner);
}

/// A compute holding a handle it was given keeps that task alive after
/// every outside handle is dropped
#[test]
fn a_captured_handle_outlives_the_outside_ones() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|()| {
        std::thread::sleep(Duration::from_millis(30));
        String::from("still here")
    }))
    .spawn();

    let captured = source.clone();

    let reader = Runtime::task(Compute::compute(move |()| {
        captured.join_with_timeout(PATIENCE)
    }))
    .spawn();

    drop(source);

    assert_eq!(
        reader.join_with_timeout(PATIENCE),
        Ok(Ok(String::from("still here")))
    );
}

/// Handles given into a waiting compute are held until it finishes
#[test]
fn handles_given_as_values_are_held_while_waiting() {
    let _ = Runtime::init();

    let summer = Runtime::task(Compute::compute(|handles: Vec<TaskHandle<u32>>| {
        Runtime::join_all(handles)
            .into_iter()
            .map(|result| result.expect("a given handle failed"))
            .sum::<u32>()
    }))
    .wait_for::<Vec<TaskHandle<u32>>>()
    .count(1)
    .spawn();

    let handles: Vec<_> = (1..=10u32)
        .map(|value| Runtime::task(Compute::compute(move |()| value)).spawn())
        .collect();

    summer.give(handles.clone()).expect("the give was refused");

    // Every outside copy gone before the compute has run
    drop(handles);

    assert_eq!(summer.join_with_timeout(PATIENCE), Ok(55));
}

/// A compute gives to a waiting task from inside itself, and reads
/// what it made
#[test]
fn a_compute_gives_to_a_waiting_task() {
    let _ = Runtime::init();

    let squarer = Runtime::task(Compute::compute(|value: u64| value * value))
        .wait_for::<u64>()
        .spawn();

    let driver = {
        let squarer = squarer.clone();

        Runtime::task(Compute::compute(move |()| {
            let mut seen = Vec::new();

            for value in 1..=5u64 {
                while !squarer.is_waiting() {
                    std::thread::yield_now();
                }

                squarer
                    .give(value)
                    .expect("a give from a compute was refused");

                loop {
                    match squarer.take_with_timeout(PATIENCE) {
                        Ok(squared) => {
                            seen.push(squared);
                            break;
                        }
                        Err(RuntimeError::AlreadyTaken) => std::thread::yield_now(),
                        Err(error) => panic!("reading a square failed with {:?}", error),
                    }
                }
            }

            seen
        }))
        .spawn()
    };

    assert_eq!(
        driver.join_with_timeout(PATIENCE),
        Ok(vec![1, 4, 9, 16, 25])
    );

    squarer.cancel();
}

/// A receive spawned from inside a compute runs with outputs that
/// already exist
#[test]
fn a_receive_spawned_inside_a_compute() {
    let _ = Runtime::init();

    let handle = Runtime::task(Compute::compute(|()| {
        let a = Runtime::task(Compute::compute(|()| 2u32)).spawn();
        let b = Runtime::task(Compute::compute(|()| 3u32)).spawn();

        Runtime::task(Compute::compute(|(a, b): (u32, u32)| a * b))
            .receive((a, b))
            .count(1)
            .spawn()
            .join()
            .expect("the receive failed")
    }))
    .spawn();

    assert_eq!(handle.join_with_timeout(PATIENCE), Ok(6));
}

/// Recursion that both splits and chains, from many threads at once,
/// with every answer its own
#[test]
fn recursion_from_many_threads_never_crosses() {
    let _ = Runtime::init();

    let (sent, answers) = mpsc::channel();

    let started = Arc::new(AtomicUsize::new(0));

    let threads: Vec<_> = (0..8u64)
        .map(|thread| {
            let sent = sent.clone();
            let started = Arc::clone(&started);

            std::thread::spawn(move || {
                started.fetch_add(1, Ordering::Relaxed);

                for n in 8..14u64 {
                    let offset = thread * 1_000_000;

                    let handle =
                        Runtime::task(Compute::compute(move |()| fibonacci(n) + offset)).spawn();

                    let _ = sent.send((n, offset, handle.join_with_timeout(PATIENCE)));
                }
            })
        })
        .collect();

    drop(sent);

    for thread in threads {
        thread.join().expect("a spawning thread went down");
    }

    let mut checked = 0;

    for (n, offset, answer) in answers {
        assert_eq!(
            answer,
            Ok(fibonacci_plain(n) + offset),
            "fibonacci {} for offset {} crossed with another thread's",
            n,
            offset,
        );

        checked += 1;
    }

    assert_eq!(checked, 48);
}
