//! A spawn before `Runtime::init`
//!
//! The whole binary has to reach the first spawn with no runtime
//! behind it, so it holds one test and inits nothing until the end

use atap::{
    Runtime, RuntimeError,
    compute::Compute,
    sleep::{Sleep, SleepMode},
};
use std::time::Duration;

/// How long a test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Every kind of spawn before an init settles with
/// `NotInitialised`, and the same spawns run once there is a
/// runtime
#[test]
fn spawning_before_init_says_so() {
    let now = Runtime::task(Compute::compute(|()| 6 * 7)).spawn();

    assert_eq!(
        now.join(),
        Err(RuntimeError::NotInitialised),
        "a plain spawn with no runtime should say so",
    );

    let delayed = Runtime::task(Compute::compute(|()| 1))
        .after(Duration::from_millis(1))
        .spawn();

    assert_eq!(
        delayed.join(),
        Err(RuntimeError::NotInitialised),
        "a delayed spawn with no runtime should say so",
    );

    let repeat = Runtime::task(Compute::compute(|()| 1))
        .repeat()
        .every(Duration::from_millis(1))
        .spawn();

    assert_eq!(
        repeat.join(),
        Err(RuntimeError::NotInitialised),
        "a repeat with no runtime should say so",
    );

    let waiting = Runtime::task(Compute::compute(|value: i32| value * 2))
        .wait_for::<i32>()
        .spawn();

    assert_eq!(
        waiting.give(7),
        Err(RuntimeError::NotInitialised),
        "a give with no runtime should say so",
    );

    assert_eq!(
        waiting.try_join(),
        Err(RuntimeError::NotInitialised),
        "a waiting task with no runtime should say so",
    );

    let blocking = Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
        .spawn();

    assert_eq!(
        blocking.join(),
        Err(RuntimeError::NotInitialised),
        "a blocking spawn with no runtime should say so",
    );

    Runtime::init().expect("the runtime starts");

    assert_eq!(
        Runtime::task(Compute::compute(|()| 6 * 7))
            .spawn()
            .join_with_timeout(PATIENCE),
        Ok(42),
        "the same spawn should run once there is a runtime",
    );
}
