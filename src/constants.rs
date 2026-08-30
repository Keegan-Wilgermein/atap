//! Constants
//! Constants used throughout the crate

use std::time::Duration;

/// Defines the crossover between pausing the thread
/// when sleeping or starting a syscall
pub(crate) const SLEEP_TOLERANCE: Duration = Duration::from_millis(10);

pub(crate) const KEVENT_COUNT: usize = 16;
