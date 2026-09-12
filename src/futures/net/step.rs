//! # Step
//! What every socket task keeps between the steps of one run

use crate::{
    RuntimeError,
    futures::task::sealed::{Park, Step},
};
use std::{
    fmt,
    time::{Duration, Instant},
};

/// A task's timeout, and the deadline one run of it works to
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Clock {
    /// How long a run may take, if it has a limit
    timeout: Option<Duration>,

    /// When the current run has to be done by
    deadline: Option<Instant>,
}

impl Clock {
    /// Sets how long a run may take
    #[inline(always)]
    pub(crate) fn limit(&mut self, timeout: Duration) {
        self.timeout = Some(timeout);
    }

    /// Starts the clock on a run
    pub(crate) fn start(&mut self) {
        self.deadline = self
            .timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
    }

    /// Whether the run is out of time
    pub(crate) fn expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// When the run has to be done by, if it has a limit
    ///
    /// For a task that parks on its own terms rather than through
    /// `wait`
    #[inline(always)]
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Parks on `ident` until it is ready for `filter`, or the
    /// deadline comes
    ///
    /// Out of time already is a timeout instead
    pub(crate) fn wait<T>(&self, ident: libc::c_int, filter: i16) -> Result<Step<T>, RuntimeError> {
        if self.expired() {
            return Err(RuntimeError::TimedOut);
        }

        Ok(Step::Park(Park {
            ident,
            filter,
            deadline: self.deadline,
        }))
    }
}

/// Where one run of a task has got to
///
/// A clone starts from the beginning, since a clone is always a
/// fresh run: the copy a blocking call drives, or the next run
/// of a schedule
#[derive(Default)]
pub(crate) struct Progress<T: Default>(pub(crate) T);

impl<T: Default> Clone for Progress<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T: Default> fmt::Debug for Progress<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("..")
    }
}

/// Turns a step that failed into the step that reports it
#[inline(always)]
pub(crate) fn settle<T>(
    step: Result<Step<Result<T, RuntimeError>>, RuntimeError>,
) -> Step<Result<T, RuntimeError>> {
    step.unwrap_or_else(|error| Step::Done(Err(error)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A clone is always a fresh run, whatever the original had
    /// got through
    #[test]
    fn a_clone_starts_from_the_beginning() {
        let progress = Progress((vec![1, 2, 3], true));
        let fresh = progress.clone();

        assert!(fresh.0.0.is_empty());
        assert!(!fresh.0.1);
    }

    /// A clock with no timeout never runs out, and one that has
    /// run out turns a wait into a timeout
    #[test]
    fn a_wait_past_the_deadline_is_a_timeout() {
        let mut open = Clock::default();
        open.start();

        assert!(!open.expired());
        assert!(matches!(open.wait::<()>(0, libc::EVFILT_READ), Ok(Step::Park(_))));

        let mut spent = Clock::default();
        spent.limit(Duration::ZERO);
        spent.start();

        assert!(spent.expired());
        assert!(matches!(
            spent.wait::<()>(0, libc::EVFILT_READ),
            Err(RuntimeError::TimedOut),
        ));
    }
}
