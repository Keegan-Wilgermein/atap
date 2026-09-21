//! # Sleeping until a moment
//! The time left is worked out when the task starts, not when it
//! was made

use atap::{Runtime, sleep::Sleep};
use std::{
    thread,
    time::{Duration, Instant},
};

/// A sleep made early only sleeps what is left once it starts
#[test]
fn sleeping_until_counts_from_the_start() {
    let _ = Runtime::init();

    let when = Instant::now() + Duration::from_millis(200);
    let task = Sleep::until(when);

    thread::sleep(Duration::from_millis(120));

    let slept = Runtime::block(task);

    println!("slept {slept:?} of the 80ms left");

    assert!(Instant::now() >= when, "woke before the moment");
    assert!(
        slept < Duration::from_millis(180),
        "slept {slept:?}, as if it started when it was made"
    );
}
