//! # Task Handle
//! A handle that allows operations
//! on unfinished tasks across threads

/// A task handle
#[derive(Debug, Clone, Copy, Hash)]
pub struct TaskHandle {
    /// The `Task` id used when
    /// registering it to `kqueue`
    /// 
    /// Actually in index
    handle: usize,
    /// The thread shard
    /// 
    /// Actually an index
    thread: usize,
}

impl TaskHandle {
    /// Creates a new task handle
    pub(crate) fn new() -> Self {
        todo!()
    }

    /// Returns whether a value is ready or not
    pub fn ready(&self) -> bool {
        todo!()
    }

    /// Returns the inner value wrapped
    /// inside `Option<T>`
    pub fn maybe_get(&self) {
        todo!()
    }

    /// Waits until the inner value is ready
    pub fn wait_until(&self) {
        todo!()
    }
}
