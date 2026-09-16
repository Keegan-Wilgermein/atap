//! Receive tests
//!
//! Tasks that run with other tasks' outputs, through `receive` and
//! `give_to`. Each test checks only the values its own tasks come
//! back with, so they share a binary and run side by side on purpose

mod common;

use atap::{Compute, Runtime, RuntimeError};
use common::settles;
use std::{
    sync::{
        Arc, Mutex,
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

/// A receive from a task that has already finished runs with its
/// output
#[test]
fn receiving_from_a_finished_task_runs_with_its_output() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|()| 21)).spawn();

    assert_eq!(source.join_with_timeout(PATIENCE), Ok(21));

    let doubled = Runtime::task(Compute::compute(|value: i32| value * 2))
        .receive(source.clone())
        .count(1)
        .spawn();

    assert_eq!(doubled.join_with_timeout(PATIENCE), Ok(42));
    assert!(
        settles(|| doubled.is_finished()),
        "a receive with a count of one never finished"
    );
}

/// A receive from a task that hasn't published yet runs once it does
#[test]
fn receiving_from_a_waiting_task_runs_once_it_is_given() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|value: i32| value + 1))
        .wait_for::<i32>()
        .spawn();

    let (ran, runs) = mpsc::channel();

    let receiver = Runtime::task(Compute::compute(move |value: i32| {
        let _ = ran.send(value);
    }))
    .receive(source.clone())
    .spawn();

    thread::sleep(QUIET);

    assert!(
        runs.try_recv().is_err(),
        "the receiver ran before its source published"
    );

    source.give(9).expect("the give was refused");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(10));

    drop(receiver);
}

/// A receive from a repeat follows its outputs in order, and
/// finishes once the repeat does
#[test]
fn receiving_from_a_repeat_follows_it_and_finishes_with_it() {
    let _ = Runtime::init();

    let counter = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&counter);

    let source = Runtime::task(Compute::compute(move |()| {
        counting.fetch_add(1, Ordering::SeqCst) + 1
    }))
    .repeat()
    .every(Duration::from_millis(15))
    .count(5)
    .spawn();

    let (ran, runs) = mpsc::channel();

    let receiver = Runtime::task(Compute::compute(move |value: usize| {
        let _ = ran.send(value);
    }))
    .receive(source.clone())
    .spawn();

    assert!(
        settles(|| receiver.is_finished()),
        "the receiver outlived the repeat it received from"
    );

    let seen: Vec<usize> = runs.try_iter().collect();

    assert!(!seen.is_empty(), "the receiver never ran");
    assert!(
        seen.windows(2).all(|pair| pair[0] < pair[1]),
        "outputs arrived out of order: {:?}",
        seen
    );
    assert_eq!(
        seen.last(),
        Some(&5),
        "the repeat's last output never arrived: {:?}",
        seen
    );
}

/// A receive with a count of one runs once, however often its source
/// publishes
#[test]
fn a_receive_with_a_count_of_one_runs_once() {
    let _ = Runtime::init();

    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);

    let source = Runtime::task(Compute::compute(|()| 7u32))
        .repeat()
        .every(Duration::from_millis(5))
        .spawn();

    let receiver = Runtime::task(Compute::compute(move |value: u32| {
        counted.fetch_add(1, Ordering::SeqCst);
        value
    }))
    .receive(source.clone())
    .count(1)
    .spawn();

    assert_eq!(receiver.join_with_timeout(PATIENCE), Ok(7));
    assert!(
        settles(|| receiver.is_finished()),
        "a count of one didn't finish the receiver"
    );

    thread::sleep(QUIET);

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "a receive with a count of one ran more than once"
    );

    source.cancel();
}

/// A source that panics without publishing writes its receiver off
#[test]
fn a_source_that_panics_writes_its_receiver_off() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|()| -> u8 {
        panic!("this source is meant to go down")
    }))
    .spawn();

    let receiver = Runtime::task(Compute::compute(|value: u8| value))
        .receive(source)
        .spawn();

    assert_eq!(
        receiver.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );
}

/// A source cancelled before it published writes its receiver off
#[test]
fn a_cancelled_source_writes_its_receiver_off() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|value: u8| value))
        .wait_for::<u8>()
        .spawn();

    let receiver = Runtime::task(Compute::compute(|value: u8| value))
        .receive(source.clone())
        .spawn();

    source.clone().cancel();

    assert_eq!(
        receiver.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );
}

/// An output taken before the receive isn't handed over, and the
/// receiver waits for the next
#[test]
fn an_output_taken_before_the_receive_waits_for_the_next() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|value: u32| value))
        .wait_for::<u32>()
        .spawn();

    source.give(1).expect("the first give was refused");

    assert_eq!(source.take_with_timeout(PATIENCE), Ok(1));

    let (ran, runs) = mpsc::channel();

    let receiver = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
    }))
    .receive(source.clone())
    .spawn();

    assert!(
        runs.recv_timeout(QUIET).is_err(),
        "an output that was taken was handed over"
    );
    assert!(
        settles(|| source.is_waiting()),
        "the source never went back to waiting"
    );

    source.give(2).expect("the second give was refused");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(2));

    drop(receiver);
}

/// `give_to` hands an output to every task it was chained to
#[test]
fn give_to_fans_out_to_every_waiter() {
    let _ = Runtime::init();

    let (ran, runs) = mpsc::channel();

    let waiters: Vec<_> = (0..100)
        .map(|index| {
            let ran = ran.clone();

            Runtime::task(Compute::compute(move |value: u64| {
                let _ = ran.send((index, value));
            }))
            .wait_for::<u64>()
            .spawn()
        })
        .collect();

    let mut builder = Runtime::task(Compute::compute(|()| 7u64));

    for waiter in &waiters {
        builder = builder.give_to(waiter);
    }

    let _source = builder.spawn();

    let mut seen: Vec<_> = (0..100)
        .map(|_| {
            runs.recv_timeout(PATIENCE)
                .expect("a waiter never got its give")
        })
        .collect();

    seen.sort();

    assert_eq!(seen, (0..100).map(|index| (index, 7)).collect::<Vec<_>>());
}

/// `give_to` gives every output of a repeat, in order
#[test]
fn give_to_follows_every_output_of_a_repeat() {
    let _ = Runtime::init();

    let counter = Arc::new(AtomicUsize::new(0));
    let counting = Arc::clone(&counter);
    let (ran, runs) = mpsc::channel();

    let waiter = Runtime::task(Compute::compute(move |value: usize| {
        let _ = ran.send(value);
    }))
    .wait_for::<usize>()
    .spawn();

    let _source = Runtime::task(Compute::compute(move |()| {
        counting.fetch_add(1, Ordering::SeqCst) + 1
    }))
    .give_to(&waiter)
    .repeat()
    .every(Duration::from_millis(20))
    .count(3)
    .spawn();

    let mut seen = Vec::new();

    while seen.last() != Some(&3) {
        seen.push(
            runs.recv_timeout(PATIENCE)
                .expect("the repeat's outputs stopped arriving"),
        );
    }

    assert!(
        seen.windows(2).all(|pair| pair[0] < pair[1]),
        "outputs arrived out of order: {:?}",
        seen
    );
}

/// A value carried through a chain of receives comes out the far end
#[test]
fn a_chain_of_receives_carries_a_value_through() {
    let _ = Runtime::init();

    let start = Runtime::task(Compute::compute(|()| 20)).spawn();

    let plus = Runtime::task(Compute::compute(|value: i32| value + 1))
        .receive(start)
        .count(1)
        .spawn();

    let doubled = Runtime::task(Compute::compute(|value: i32| value * 2))
        .receive(plus)
        .count(1)
        .spawn();

    assert_eq!(doubled.join_with_timeout(PATIENCE), Ok(42));
}

/// A pipeline of a thousand stages, built in a loop, counts every
/// stage
#[test]
fn a_pipeline_of_a_thousand_stages_counts_every_stage() {
    let _ = Runtime::init();

    let mut last = Runtime::task(Compute::compute(|()| 0u64)).spawn();

    for _ in 0..1_000 {
        last = Runtime::task(Compute::compute(|value: u64| value + 1))
            .receive(last)
            .count(1)
            .spawn();
    }

    assert_eq!(last.join_with_timeout(PATIENCE), Ok(1_000));
}

/// A receive registered while its source is publishing gets the output
/// exactly once
#[test]
fn a_receive_racing_a_publish_gets_the_output_exactly_once() {
    let _ = Runtime::init();

    for round in 0..500u64 {
        let runs = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&runs);

        let source = Runtime::task(Compute::compute(move |()| round)).spawn();

        let receiver = Runtime::task(Compute::compute(move |value: u64| {
            counted.fetch_add(1, Ordering::SeqCst);
            value
        }))
        .receive(source)
        .spawn();

        assert_eq!(
            receiver.join_with_timeout(PATIENCE),
            Ok(round),
            "round {} came back wrong",
            round
        );
        assert!(
            settles(|| receiver.is_finished()),
            "round {} never finished",
            round
        );

        assert_eq!(
            runs.load(Ordering::SeqCst),
            1,
            "round {} handed its output over {} times",
            round,
            runs.load(Ordering::SeqCst),
        );
    }
}

/// A waiting source given before the receive hands that output over
#[test]
fn a_waiting_source_given_before_the_receive_hands_its_output_over() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|value: u32| value * 3))
        .wait_for::<u32>()
        .spawn();

    source.give(5).expect("the give was refused");

    assert_eq!(source.join_with_timeout(PATIENCE), Ok(15));

    let receiver = Runtime::task(Compute::compute(|value: u32| value))
        .receive(source.clone())
        .count(1)
        .spawn();

    assert_eq!(receiver.join_with_timeout(PATIENCE), Ok(15));
}

/// A waiting source still running when the receive is made hands its
/// output over once it is done
#[test]
fn a_waiting_source_mid_run_hands_its_output_over_when_done() {
    let _ = Runtime::init();

    let (started, starts) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    let released = Mutex::new(released);

    let source = Runtime::task(Compute::compute(move |value: u32| {
        let _ = started.send(());
        let _ = released.lock().unwrap().recv_timeout(PATIENCE);
        value
    }))
    .wait_for::<u32>()
    .spawn();

    source.give(8).expect("the give was refused");
    starts
        .recv_timeout(PATIENCE)
        .expect("the source never started");

    let receiver = Runtime::task(Compute::compute(|value: u32| value))
        .receive(source.clone())
        .count(1)
        .spawn();

    thread::sleep(Duration::from_millis(20));

    assert!(
        receiver.is_pending(),
        "the receiver ran before its source finished"
    );

    let _ = release.send(());

    assert_eq!(receiver.join_with_timeout(PATIENCE), Ok(8));
}

/// A waiting source with an old output hands that over, then its
/// next one
#[test]
fn a_waiting_source_with_an_old_output_hands_that_over_then_the_next() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|value: u32| value))
        .wait_for::<u32>()
        .spawn();

    source.give(1).expect("the first give was refused");

    assert_eq!(source.join_with_timeout(PATIENCE), Ok(1));

    let (ran, runs) = mpsc::channel();

    let receiver = Runtime::task(Compute::compute(move |value: u32| {
        let _ = ran.send(value);
    }))
    .receive(source.clone())
    .spawn();

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(1));
    assert!(
        settles(|| source.is_waiting()),
        "the source never went back to waiting"
    );

    source.give(2).expect("the second give was refused");

    assert_eq!(runs.recv_timeout(PATIENCE), Ok(2));

    drop(receiver);
}

/// A waiting source out of gives lets its receiver finish
#[test]
fn a_waiting_source_out_of_gives_lets_its_receiver_finish() {
    let _ = Runtime::init();

    let source = Runtime::task(Compute::compute(|value: u32| value))
        .wait_for::<u32>()
        .count(1)
        .spawn();

    let receiver = Runtime::task(Compute::compute(|value: u32| value))
        .receive(source.clone())
        .spawn();

    source.give(4).expect("the give was refused");

    assert_eq!(receiver.join_with_timeout(PATIENCE), Ok(4));
    assert!(
        settles(|| receiver.is_finished()),
        "the receiver outlived a source that takes no more gives"
    );
}

/// A waiting source whose only handle went into a receive lets the
/// receiver go, and then goes itself
#[test]
fn a_waiting_source_held_only_by_a_receive_lets_both_go() {
    let _ = Runtime::init();

    let probe = Arc::new(());
    let captured = Arc::clone(&probe);

    let source = Runtime::task(Compute::compute(move |value: u8| {
        let _ = Arc::strong_count(&captured);
        value
    }))
    .wait_for::<u8>()
    .spawn();

    // The only handle to the source goes in, and nothing can give to it now
    let receiver = Runtime::task(Compute::compute(|value: u8| value))
        .receive(source)
        .spawn();

    assert_eq!(
        receiver.join_with_timeout(PATIENCE),
        Err(RuntimeError::TaskFailed)
    );
    assert!(
        settles(|| Arc::strong_count(&probe) == 1),
        "the source outlived everything that could reach it"
    );
}
