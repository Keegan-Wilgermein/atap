//! # Settles

use std::{
    thread,
    time::{Duration, Instant},
};

/// Waits for something to become true, with a cap
///
/// ## Returns
/// Whether it came true within five seconds
pub fn settles(mut condition: impl FnMut() -> bool) -> bool {
    let waited = Instant::now();

    while waited.elapsed() < Duration::from_secs(5) {
        if condition() {
            return true;
        }

        thread::sleep(Duration::from_millis(10));
    }

    condition()
}
