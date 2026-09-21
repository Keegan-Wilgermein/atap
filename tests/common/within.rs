//! # Within
//! Running a task that answers with a `Result` to a time limit

use atap::{Runtime, RuntimeError, Task, builder::Standalone};
use std::time::Duration;

/// Spawns `task` limited to `limit` and waits for its answer
///
/// A task that runs out of time answers `Err(TimedOut)`, the same as
/// any other error it gives
pub fn within<F, T>(task: F, limit: Duration) -> Result<T, RuntimeError>
where
    F: Task<Output = Result<T, RuntimeError>>,
    F::Input: Standalone,
    T: Send + 'static,
{
    Runtime::task(task)
        .timeout(limit)
        .spawn()
        .take()
        .and_then(|answer| answer)
}
