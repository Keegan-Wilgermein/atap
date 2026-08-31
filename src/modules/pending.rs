//! Pending
//! Represents a future value

/// A pending value
pub struct Pending<T>(Option<T>)
where
    T: Clone;

impl<T> Pending<T>
where
    T: Clone,
{
    /// Creates a new pending value
    pub(crate) fn new() -> Self {
        Self(None)
    }

    /// Returns whether a value is ready or not
    pub fn ready(&self) -> bool {
        false
    }

    /// Returns the inner value wrapped
    /// inside `Option<T>`
    pub fn maybe_get(&self) -> Option<T> {
        self.0.clone()
    }

    /// Gets the inner value of pending,
    /// unwrapping the result
    pub fn get(&self) -> T {
        self.0.clone().unwrap()
    }

    /// Waits until the inner value is ready
    pub fn wait_until(&self) -> T {
        todo!()
    }
}
