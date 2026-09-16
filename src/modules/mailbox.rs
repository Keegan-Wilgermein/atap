//! # Mailbox
//! Where a give leaves its value for a waiting task's next run

use crate::modules::gate::Gate;
use std::sync::{Arc, Mutex, PoisonError};

/// The value a waiting task's runs are handed, and the gate that
/// decides what each give does
///
/// Public only so a handle's kind can name it. Nothing outside the
/// crate can reach it or do anything with it
#[doc(hidden)]
pub struct Mailbox<T> {
    /// Decides whether a give starts anything
    gate: Arc<Gate>,

    /// The latest value given
    ///
    /// Replaced by a give and never taken by a run
    value: Mutex<Option<T>>,
}

impl<T> Mailbox<T> {
    /// An empty mailbox for a task started through `gate`
    pub(crate) fn new(gate: Arc<Gate>) -> Self {
        Self {
            gate,
            value: Mutex::new(None),
        }
    }

    /// A mailbox for a handle to no task, which takes nothing
    pub(crate) fn detached() -> Self {
        Self::new(Arc::new(Gate::detached()))
    }

    /// The gate every give goes through
    #[inline(always)]
    pub(crate) fn gate(&self) -> &Gate {
        &self.gate
    }

    /// Leaves `value` for the next run
    ///
    /// ## Returns
    /// The value it replaced, which the caller drops once the lock
    /// is let go
    pub(crate) fn replace(&self, value: T) -> Option<T> {
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .replace(value)
    }

    /// A copy of the latest value, if anything has been given
    ///
    /// A copy whose `Clone` panicked leaves the lock poisoned, which
    /// is looked past rather than wedging every later give
    pub(crate) fn latest(&self) -> Option<T>
    where
        T: Clone,
    {
        self.value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Drops the value, once no run will ever want it
    ///
    /// Breaks any cycle a handle given as data made through this task
    pub(crate) fn clear(&self) {
        let value = self
            .value
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();

        drop(value);
    }
}
