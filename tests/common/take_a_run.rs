//! # Take a run

use atap::{Runtime, RuntimeError, TaskHandle};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Waits for the next run of a repeating task and takes it
pub fn take_a_run(handle: &TaskHandle<Duration>) -> Duration {
    let mut polls = 0u64;
    let waited = Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The next run hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!(
                "a repeating task came back with {:?} after {} polls, pool {:?}",
                error,
                polls,
                Runtime::pool(),
            ),
        }

        polls += 1;

        // A series that stopped producing fails rather than hangs
        assert!(
            waited.elapsed() < Duration::from_secs(30),
            "a repeating task stopped producing runs after {} polls, pool {:?}",
            polls,
            Runtime::pool(),
        );

        thread::sleep(Duration::from_micros(100));
    }
}
