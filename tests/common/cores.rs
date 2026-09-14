//! # Cores

use std::thread;

/// Online cores, which the pool sizes itself against
pub fn cores() -> usize {
    thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
}
