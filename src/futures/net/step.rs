//! # Step
//! What every socket task keeps between the steps of one run

use crate::{
    RuntimeError,
    futures::task::sealed::{Park, Step},
};
use std::fmt;

/// Parks on `ident` until it is ready for `filter`
#[inline(always)]
pub(crate) fn wait_on<T>(ident: libc::c_int, filter: i16) -> Result<Step<T>, RuntimeError> {
    Ok(Step::Park(Park {
        ident,
        filter,
        notes: 0,
        deadline: None,
    }))
}

/// Where one run of a task has got to
///
/// A clone starts from the beginning
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

    /// A wait always parks on what it was given
    #[test]
    fn a_wait_parks() {
        assert!(matches!(
            wait_on::<()>(3, libc::EVFILT_READ),
            Ok(Step::Park(Park {
                ident: 3,
                deadline: None,
                ..
            }))
        ));
    }
}
