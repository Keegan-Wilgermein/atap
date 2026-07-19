//! # Pending
//! 
//! `Pending` data gives you the value if it exists,
//! when you check for it

pub struct Pending<T> {
    data: Option<T>,
}

impl<T> Pending<T> {
    /// Creates a new `Pending`
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
    /// Only use after checking `Self.ready()` is `true`
    pub fn get(self) -> T {
        self.data.unwrap()
    }
}

impl<T> Pending<T>
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
