use atap::{JoinPolicy, Runtime, Sleep, TaskHandle};
use std::time::Duration;

/// A sleep of a given length, spawned
fn sleeping(millis: u64) -> TaskHandle<Duration> {
    Runtime::task(Sleep::sleep(Duration::from_millis(millis), false)).spawn()
}

/// `PassBack` hands the losing handles back in the order they
/// were given
#[test]
fn pass_back_hands_the_losers_over_in_order() {
    Runtime::init();

    let quick = sleeping(5);
    let quick_id = quick.id();

    let slow: Vec<_> = (0..4).map(|_| sleeping(300)).collect();
    let order: Vec<_> = slow.iter().map(|handle| handle.id()).collect();

    let (first, rest) = Runtime::join_first(
        std::iter::once(quick).chain(slow),
        JoinPolicy::PassBack,
    );

    assert_eq!(first.id(), quick_id, "the wrong task won");

    let rest = rest.expect("PassBack should hand the losers back");

    assert_eq!(rest.len(), 4, "expected four losers, got {}", rest.len());

    let handed: Vec<_> = rest.iter().map(|handle| handle.id()).collect();

    assert_eq!(handed, order, "the losers came back in a different order");

    // Every loser still finishes with an output to give
    for handle in rest {
        handle.join().expect("a loser should still finish");
    }
}
