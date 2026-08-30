//! Constants
//! Constants used throughout the crate

use std::time::Duration;

/// Defines the crossover between pausing the thread
/// when sleeping or starting a syscall
pub(crate) const SLEEP_TOLERANCE: Duration = Duration::from_micros(500);

/// Amount of kevents that can be processed
/// by a single `kevent`
pub(crate) const KEVENT_COUNT: usize = 16;

/// Delay before the first restart of a dead `Reactor`,
/// multiplied by the number of consecutive failures
pub(crate) const RESTART_BACKOFF: Duration = Duration::from_millis(10);

/// How long a `Reactor` has to survive before its
/// restart is treated as a one off rather than a cycle
pub(crate) const RESTART_WINDOW: Duration = Duration::from_secs(5);

/// Consecutive restarts inside `RESTART_WINDOW`
/// before the supervisor stops trying
pub(crate) const RESTART_LIMIT: u32 = 5;

/// Stored in place of the kqueue id once no
/// `Reactor` is listening on one
pub(crate) const DEAD_KQUEUE_ID: i32 = -1;
