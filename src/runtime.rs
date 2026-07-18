//! # Runtime
//! 
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use crate::{Task, future::Future};

pub struct Runtime {}

impl Runtime {
    /// Creates a new runtime
    pub const fn new() -> Self {
        Self {}
    }

    pub fn whenever<T, F>(
        &self,
        function: F,
    ) -> Future<T>
    where
        F: Fn() -> T,
    { 
        let task = Task::new(function);
        Future::new()
    }
}
