mod common;

use atap::{JoinPolicy, Runtime};
use common::sleeping;
use std::time::Duration;
use std::time::Instant;

/// `join_first` returns the quickest task without waiting for
/// the rest
#[test]
fn the_quickest_one_wins() {
    Runtime::init();

    let quick = sleeping(5);
    let quick_id = quick.id();

    let slow: Vec<_> = (0..4).map(|_| sleeping(4000)).collect();

    let started = Instant::now();

    let (first, rest) = Runtime::join_first(std::iter::once(quick).chain(slow), JoinPolicy::Cancel);

    let waited = started.elapsed();

    println!("the race took {:?}", waited);

    assert_eq!(first.id(), quick_id, "the wrong task won");
    assert!(rest.is_none(), "Cancel should not hand the losers back");

    assert!(
        waited < Duration::from_secs(2),
        "the race took {:?}",
        waited
    );

    assert!(first.settled(), "the winner should be settled");
}
