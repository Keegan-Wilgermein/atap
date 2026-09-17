//! # Full report

use atap::Runtime;

/// Everything the pool is doing, workers and all
pub fn report_full(at: &str) {
    println!("  [{}]\n{}", at, Runtime::pool());
}
