//! Channel tests
//!
//! Each test checks only the values its own channels carry, so they
//! share a binary and run side by side on purpose

mod common;

use atap::{Runtime, RuntimeError, channel::Channel, compute::Compute};
use common::until_started;
use std::{
    collections::HashSet,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Values come out in the order they went in
#[test]
fn values_arrive_in_order() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u32>().open().unwrap();

    for value in 0..100 {
        tx.send(value).unwrap();
    }

    assert_eq!(tx.len(), 100);

    for value in 0..100 {
        assert_eq!(Runtime::block(rx.recv()), Ok(value));
    }

    assert!(rx.is_empty());
}

/// A spawned receive waits for a value, then takes it
#[test]
fn a_spawned_receive_waits_for_a_value() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<String>().open().unwrap();

    let waiting = Runtime::task(rx.recv()).spawn();
    until_started(&waiting, PATIENCE);

    assert!(
        waiting.is_running(),
        "a receive with nothing to take finished"
    );

    tx.send(String::from("late")).unwrap();

    assert_eq!(
        waiting.join_with_timeout(PATIENCE),
        Ok(Ok(String::from("late")))
    );
}

/// `try_recv` says there is nothing yet, rather than waiting
#[test]
fn try_recv_does_not_wait() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().open().unwrap();

    assert_eq!(rx.try_recv(), Err(RuntimeError::NotReady));

    tx.send(9).unwrap();

    assert_eq!(rx.try_recv(), Ok(9));
}

/// Every value from many sending threads reaches one receiver, once
#[test]
fn many_senders_lose_nothing() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u64>().open().unwrap();

    let threads: Vec<_> = (0..8u64)
        .map(|sender| {
            let tx = tx.clone();

            thread::spawn(move || {
                for at in 0..500 {
                    tx.send(sender * 1000 + at).unwrap();
                }
            })
        })
        .collect();

    drop(tx);

    let mut seen = HashSet::new();

    loop {
        match Runtime::task(rx.recv()).spawn().join_with_timeout(PATIENCE) {
            Ok(Ok(value)) => assert!(seen.insert(value), "{value} came out twice"),
            Ok(Err(RuntimeError::Closed)) => break,
            other => panic!("a receive gave {other:?}"),
        }
    }

    for thread in threads {
        thread.join().unwrap();
    }

    assert_eq!(seen.len(), 4000);
}

/// Several receivers share the values, each value going to one
#[test]
fn many_receivers_share_the_values() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u64>().open().unwrap();

    let takers: Vec<_> = (0..6)
        .map(|_| {
            let rx = rx.clone();

            thread::spawn(move || {
                let mut got = Vec::new();

                while let Ok(value) = Runtime::block(rx.recv()) {
                    got.push(value);
                }

                got
            })
        })
        .collect();

    drop(rx);

    for value in 0..3000 {
        tx.send(value).unwrap();
    }

    drop(tx);

    let mut all: Vec<u64> = takers
        .into_iter()
        .flat_map(|taker| taker.join().unwrap())
        .collect();

    all.sort_unstable();

    assert_eq!(all, (0..3000).collect::<Vec<_>>());
}

/// Once every sender is gone, what is left still comes out, then
/// the channel reads closed
#[test]
fn a_closed_channel_drains_then_closes() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().open().unwrap();

    tx.send(1).unwrap();
    tx.send(2).unwrap();
    drop(tx);

    assert_eq!(Runtime::block(rx.recv()), Ok(1));
    assert_eq!(Runtime::block(rx.recv()), Ok(2));
    assert_eq!(Runtime::block(rx.recv()), Err(RuntimeError::Closed));
    assert_eq!(rx.try_recv(), Err(RuntimeError::Closed));
}

/// A receive waiting when the last sender goes is woken and told
#[test]
fn a_waiting_receive_hears_the_close() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().open().unwrap();

    let waiting = Runtime::task(rx.recv()).spawn();
    until_started(&waiting, PATIENCE);

    drop(tx);

    assert_eq!(
        waiting.join_with_timeout(PATIENCE),
        Ok(Err(RuntimeError::Closed))
    );
}

/// With no receiver left, a send gives up
#[test]
fn sending_to_nobody_is_closed() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().open().unwrap();

    drop(rx);

    assert_eq!(tx.send(1), Err(RuntimeError::Closed));

    let (tx, rx) = Channel::new::<u8>().bounded(4).open().unwrap();

    drop(rx);

    assert_eq!(Runtime::block(tx.send(1)), Err(RuntimeError::Closed));
    assert_eq!(tx.try_send(1), Err(RuntimeError::Closed));
}

/// A bounded send waits for room, and gets it once a value is taken
#[test]
fn a_full_channel_makes_a_send_wait() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().bounded(2).open().unwrap();

    Runtime::block(tx.send(1)).unwrap();
    Runtime::block(tx.send(2)).unwrap();

    assert_eq!(tx.try_send(3), Ok(Err(3)));

    let waiting = Runtime::task(tx.send(3)).spawn();
    until_started(&waiting, PATIENCE);

    assert!(waiting.is_running(), "a send into a full channel finished");

    assert_eq!(Runtime::block(rx.recv()), Ok(1));
    assert_eq!(waiting.join_with_timeout(PATIENCE), Ok(Ok(())));

    assert_eq!(Runtime::block(rx.recv()), Ok(2));
    assert_eq!(Runtime::block(rx.recv()), Ok(3));
}

/// A send waiting for room when the last receiver goes is woken
/// and told
#[test]
fn a_waiting_send_hears_the_close() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().bounded(1).open().unwrap();

    Runtime::block(tx.send(1)).unwrap();

    let waiting = Runtime::task(tx.send(2)).spawn();
    until_started(&waiting, PATIENCE);

    drop(rx);

    assert_eq!(
        waiting.join_with_timeout(PATIENCE),
        Ok(Err(RuntimeError::Closed))
    );
}

/// A capacity of zero still holds one value
#[test]
fn a_zero_capacity_holds_one() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().bounded(0).open().unwrap();

    assert_eq!(tx.try_send(1), Ok(Ok(())));
    assert_eq!(tx.try_send(2), Ok(Err(2)));
    assert_eq!(rx.try_recv(), Ok(1));
}

/// A bounded send puts its value in once, however often it runs
#[test]
fn a_send_task_sends_once() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().bounded(4).open().unwrap();

    let send = tx.send(5);

    assert_eq!(Runtime::block(send.clone()), Ok(()));
    assert_eq!(Runtime::block(send), Err(RuntimeError::Finished));
    assert_eq!(rx.len(), 1);
}

/// A cancelled receive takes nothing, so the value waits for the
/// next one
#[test]
fn a_cancelled_receive_takes_nothing() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().open().unwrap();

    let waiting = Runtime::task(rx.recv()).spawn();
    until_started(&waiting, PATIENCE);

    waiting.clone().cancel();

    assert_eq!(
        waiting.join_with_timeout(PATIENCE),
        Err(RuntimeError::Cancelled)
    );

    tx.send(4).unwrap();

    assert_eq!(Runtime::block(rx.recv()), Ok(4));
}

/// A receive with a timeout gives up, and the channel carries on
#[test]
fn a_receive_can_time_out() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u8>().open().unwrap();

    let started = Instant::now();
    let waiting = Runtime::task(rx.recv())
        .timeout(Duration::from_millis(50))
        .spawn();

    assert_eq!(
        waiting.join_with_timeout(PATIENCE),
        Err(RuntimeError::TimedOut)
    );
    assert!(started.elapsed() >= Duration::from_millis(50));

    tx.send(8).unwrap();

    assert_eq!(Runtime::block(rx.recv()), Ok(8));
}

/// A repeating receive takes a value on each run, and passes each
/// one on
#[test]
fn a_repeating_receive_passes_every_value_on() {
    let _ = Runtime::init();

    let (tx, rx) = Channel::new::<u32>().open().unwrap();
    let (seen, heard) = mpsc::channel();

    let collector = Runtime::task(Compute::compute(move |got: Result<u32, RuntimeError>| {
        let _ = seen.send(got);
    }))
    .wait_for::<Result<u32, RuntimeError>>()
    .spawn();

    let _reader = Runtime::task(rx.recv())
        .repeat()
        .count(50)
        .give_to(&collector)
        .spawn();

    // One at a time, since a give replaces one not yet taken
    for value in 0..50 {
        tx.send(value).unwrap();

        assert_eq!(heard.recv_timeout(PATIENCE), Ok(Ok(value)));
    }
}
