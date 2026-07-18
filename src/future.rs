//! # Future
//! 
//! Futures give you the value when it exists, or tell you to wait longer

pub struct Future<T> {
    data: Option<T>,
}

impl<T> Future<T> {
    /// Creates a new `Future`
    pub(crate) fn new() -> Self {
        Self {
            data: None,
        }
    }

    /// Returns `true` if the data is ready
    pub fn ready(&self) -> bool {
        self.data.is_some()
    }

    /// Returns `T`, unwrapping the inner data and consuming `Self`
    /// 
    /// Only use after checking with `Self.ready()`
    pub fn get(self) -> T {
        self.data.unwrap()
    }
}

impl<T> Future<T>
where
    T: Clone,
{
    /// Returns `None` if there is no data, or `Some(T)` if there is
    /// 
    /// This method is only avaliable if `T` implements `Clone`
    /// 
    /// Use `Self.ready()` and `Self.get()` to check if it exists and unwrap the value,
    /// consuming `Self`
    pub fn maybe_get(&self) -> Option<T> {
        self.data.clone()
    }
}
