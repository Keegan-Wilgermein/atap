//! Handle set tests
//!
//! Receives from sets of handles of any types and shapes, gathered
//! with `receive` or merged with `receive_any`. Each test checks only
//! its own tasks' values, so they share a binary and run side by side
//! on purpose

mod common;

use atap::{
    Runtime, RuntimeError, TaskHandle,
    compute::Compute,
    sleep::{Sleep, SleepMode},
};
use common::settles;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
        mpsc,
    },
    thread,
    time::Duration,
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Long enough to be sure nothing more is coming
const QUIET: Duration = Duration::from_millis(100);

/// A task that publishes `value`
fn value<T>(value: T) -> TaskHandle<T>
where
    T: Clone + Send + 'static,
{
    Runtime::task(Compute::compute(move |()| value.clone())).spawn()
}

/// A tuple of handles of different types arrives as that tuple
#[test]
fn a_tuple_of_mixed_types_arrives_as_that_tuple() {
    let _ = Runtime::init();

    let joined = Runtime::task(Compute::compute(|(a, b, c): (u8, String, f64)| {
        format!("{a} {b} {c}")
    }))
    .receive((value(1u8), value(String::from("two")), value(3.5f64)))
    .count(1)
    .spawn();

    assert_eq!(
        joined.join_with_timeout(PATIENCE),
        Ok(String::from("1 two 3.5"))
    );
}

/// Sets inside sets arrive in the same shape
#[test]
fn nested_sets_arrive_nested() {
    let _ = Runtime::init();

    let total = Runtime::task(Compute::compute(
        |(a, (b, [c, d])): (u8, (u16, [u32; 2]))| a as u64 + b as u64 + c as u64 + d as u64,
    ))
    .receive((value(1u8), (value(2u16), [value(3u32), value(4u32)])))
    .count(1)
    .spawn();

    assert_eq!(total.join_with_timeout(PATIENCE), Ok(10));
}

/// A set bigger than twelve is built by nesting tuples
#[test]
fn a_set_past_twelve_is_built_by_nesting() {
    let _ = Runtime::init();

    type Twelve = (u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64, u64);

    let total = Runtime::task(Compute::compute(|(first, second): (Twelve, (u64, u64))| {
        let (a, b, c, d, e, f, g, h, i, j, k, l) = first;

        a + b + c + d + e + f + g + h + i + j + k + l + second.0 + second.1
    }))
    .receive((
        (
            value(0u64),
            value(1u64),
            value(2u64),
            value(3u64),
            value(4u64),
            value(5u64),
            value(6u64),
            value(7u64),
            value(8u64),
            value(9u64),
            value(10u64),
            value(11u64),
        ),
        (value(12u64), value(13u64)),
    ))
    .count(1)
    .spawn();

    assert_eq!(total.join_with_timeout(PATIENCE), Ok(91));
}

/// A `Vec` of a thousand handles is gathered whole
#[test]
fn a_vec_of_a_thousand_handles_is_gathered_whole() {
    let _ = Runtime::init();

    let handles: Vec<_> = (0..1_000u64).map(value).collect();

    let total = Runtime::task(Compute::compute(|all: Vec<u64>| all.iter().sum::<u64>()))
        .receive(handles)
        .count(1)
        .spawn();

    assert_eq!(total.join_with_timeout(PATIENCE), Ok(499_500));
}

/// A `Vec` of tuples arrives as a `Vec` of tuples, in order
#[test]
fn a_vec_of_tuples_arrives_as_a_vec_of_tuples() {
    let _ = Runtime::init();

    let pairs = Runtime::task(Compute::compute(|pairs: Vec<(u8, char)>| pairs))
        .receive(vec![(value(1u8), value('a')), (value(2u8), value('b'))])
        .count(1)
        .spawn();

    assert_eq!(
        pairs.join_with_timeout(PATIENCE),
        Ok(vec![(1, 'a'), (2, 'b')])
    );
}

/// An array of handles arrives as an array, in order
#[test]
fn an_array_of_handles_arrives_as_an_array() {
    let _ = Runtime::init();

    let all = Runtime::task(Compute::compute(|all: [u8; 4]| all))
        .receive([value(1u8), value(2u8), value(3u8), value(4u8)])
        .count(1)
        .spawn();

    assert_eq!(all.join_with_timeout(PATIENCE), Ok([1, 2, 3, 4]));
}

/// An empty set is complete before anything arrives, so the task runs
/// once with nothing and finishes
#[test]
fn an_empty_set_runs_once_with_nothing() {
    let _ = Runtime::init();

    let empty = Runtime::task(Compute::compute(|all: Vec<u8>| all.len()))
        .receive(Vec::<TaskHandle<u8>>::new())
        .spawn();

    assert_eq!(empty.join_with_timeout(PATIENCE), Ok(0));
    assert!(
        settles(|| empty.is_finished()),
        "a task receiving nothing never finished"
    );
}

/// The same handle twice in a set fills both places
#[test]
fn the_same_handle_twice_fills_both_places() {
    let _ = Runtime::init();

    let five = value(5u8);

    let both = Runtime::task(Compute::compute(|(a, b): (u8, u8)| (a, b)))
        .receive((five.clone(), five))
        .count(1)
        .spawn();

    assert_eq!(both.join_with_timeout(PATIENCE), Ok((5, 5)));
}

/// A task that takes nothing waits for the whole set before it runs
#[test]
fn a_task_that_takes_nothing_waits_for_the_whole_set() {
    let _ = Runtime::init();

    let first = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();
    let second = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();

    let sleeper = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .receive((first.clone(), second.clone()))
        .count(1)
        .spawn();

    first.give(1).expect("the first give was refused");

    thread::sleep(QUIET);

    assert!(
        sleeper.is_pending(),
        "a set ran before every task in it had published"
    );

    second.give(2).expect("the second give was refused");

    assert!(
        sleeper.join_with_timeout(PATIENCE).is_ok(),
        "the set never ran once whole"
    );
}

/// Every round from repeats at different speeds holds newer values
/// than the one before, place by place
#[test]
fn rounds_from_repeats_never_go_backwards() {
    let _ = Runtime::init();

    let fast_count = Arc::new(AtomicUsize::new(0));
    let slow_count = Arc::clone(&Arc::new(AtomicUsize::new(0)));
    let fast_counting = Arc::clone(&fast_count);
    let slow_counting = Arc::clone(&slow_count);

    let fast = Runtime::task(Compute::compute(move |()| {
        fast_counting.fetch_add(1, Ordering::SeqCst) + 1
    }))
    .repeat()
    .every(Duration::from_millis(3))
    .count(30)
    .spawn();

    let slow = Runtime::task(Compute::compute(move |()| {
        slow_counting.fetch_add(1, Ordering::SeqCst) + 1
    }))
    .repeat()
    .every(Duration::from_millis(11))
    .count(10)
    .spawn();

    let (ran, runs) = mpsc::channel();

    let gathered = Runtime::task(Compute::compute(move |(fast, slow): (usize, usize)| {
        let _ = ran.send((fast, slow));
    }))
    .receive((fast, slow))
    .spawn();

    assert!(
        settles(|| gathered.is_finished()),
        "the set outlived the repeats it gathered from"
    );

    let rounds: Vec<(usize, usize)> = runs.try_iter().collect();

    assert!(!rounds.is_empty(), "no round was ever gathered");

    assert!(
        rounds
            .windows(2)
            .all(|pair| pair[0].0 < pair[1].0 && pair[0].1 < pair[1].1),
        "a place held an older value than the round before: {:?}",
        rounds,
    );
}

/// A task in the set that fails without publishing writes the
/// receiver off
#[test]
fn a_failing_task_in_a_set_writes_the_receiver_off() {
    let _ = Runtime::init();

    let good = value(1u8);

    let bad = Runtime::task(Compute::compute(|()| -> u8 {
        panic!("this task is meant to go down")
    }))
    .spawn();

    let receiver = Runtime::task(Compute::compute(|(a, b): (u8, u8)| a + b))
        .receive((good, bad))
        .spawn();

    assert_eq!(
        receiver.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );
}

/// `receive_any` turns outputs of different types into what the task
/// takes, one at a time
#[test]
fn receive_any_turns_mixed_types_into_the_input() {
    let _ = Runtime::init();

    let small = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();
    let medium = Runtime::task(Compute::compute(|value: u16| value))
        .wait_for::<u16>()
        .spawn();
    let large = Runtime::task(Compute::compute(|value: u32| value))
        .wait_for::<u32>()
        .spawn();

    let (ran, runs) = mpsc::channel();

    let merged = Runtime::task(Compute::compute(move |value: u64| {
        let _ = ran.send(value);
    }))
    .receive_any((small.clone(), medium.clone(), large.clone()))
    .spawn();

    small.give(1).expect("a give was refused");
    assert_eq!(runs.recv_timeout(PATIENCE), Ok(1));

    medium.give(300).expect("a give was refused");
    assert_eq!(runs.recv_timeout(PATIENCE), Ok(300));

    large.give(70_000).expect("a give was refused");
    assert_eq!(runs.recv_timeout(PATIENCE), Ok(70_000));

    drop(merged);
}

/// `receive_any` starts a task that takes nothing, on any output
#[test]
fn receive_any_starts_a_task_that_takes_nothing() {
    let _ = Runtime::init();

    let first = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();
    let second = Runtime::task(Compute::compute(|text: String| text))
        .wait_for::<String>()
        .spawn();

    let sleeper = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .receive_any((first.clone(), second.clone()))
        .count(2)
        .spawn();

    second
        .give(String::from("go"))
        .expect("the give was refused");

    assert!(
        sleeper.join_with_timeout(PATIENCE).is_ok(),
        "an output didn't start the task"
    );
}

/// `receive_any` takes a `Vec` of handles
#[test]
fn receive_any_takes_a_vec() {
    let _ = Runtime::init();

    let sources: Vec<_> = (0..4)
        .map(|_| {
            Runtime::task(Compute::compute(|value: u32| value))
                .wait_for::<u32>()
                .spawn()
        })
        .collect();

    let (ran, runs) = mpsc::channel();

    let merged = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
    }))
    .receive_any(sources.clone())
    .spawn();

    for (index, source) in sources.iter().enumerate() {
        let given = index as u32 * 10;

        source.give(given).expect("a give was refused");

        assert_eq!(runs.recv_timeout(PATIENCE), Ok(given));
    }

    drop(merged);
}
