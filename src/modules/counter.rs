//! # Counter
//! The entire purpose of this is to
//! keep count of a number across threads with
//! as much accuracy as possible
//! 
//! This still doesn't guarantee definite accuracy

use std::sync::RwLock;

use crate::RuntimeError;

/// `u32` so any out of order adjustments
/// are caught with `saturating_add()`
/// or `saturating_sub()`
/// 
/// Defined as an alias so it can be
/// changed easily
type CountType = u32;

/// Keeps track of a single number across threads
pub(crate) struct Counter {
    count: RwLock<CountType>,
}

impl Counter {
    /// Creates a new `Counter` with value 0
    pub(crate) const fn new() -> Self {
        Self { count: RwLock::new(0) }
    }

    /// Gets the current value
    pub(crate) fn query(&self) -> Result<CountType, RuntimeError> {
        let lock = self.count.try_read()?;
        return Ok(*lock);
    }

    /// Adds a value to the current value
    pub(crate) fn try_increment(&self, by: CountType) -> Option<RuntimeError> {
        let mut lock = self.count.try_write().ok()?;

        let data = *lock;
        *lock = data.saturating_add(by);

        None
    }

    /// Subtracts a value from the current value
    pub(crate) fn try_decrement(&self, by: CountType) -> Option<RuntimeError> {
        let mut lock = self.count.try_write().ok()?;

        let data = *lock;
        *lock = data.saturating_sub(by);

        None
    }
}
