//! # Task
//! 
//! A `Task` defines a specific job

pub struct Task<T, F>
where
    F: Fn() -> T,
{
    function: F,
}

impl<T, F> Task<T, F>
where
    F: Fn() -> T,
{
    pub(crate) fn new(function: F) -> Self {
        Self {
            function,
        }
    }
}
