//! # Until started

use atap::TaskHandle;
use std::{
    thread,
    time::{Duration, Instant},
};

/// Waits until a spawned task is parked or running, so what the
/// test does next lands while it waits
pub fn until_started<T>(handle: &TaskHandle<T>, patience: Duration) {
    let deadline = Instant::now() + patience;

    while handle.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    // Long enough for it to reach its park
    thread::sleep(Duration::from_millis(20));
}
