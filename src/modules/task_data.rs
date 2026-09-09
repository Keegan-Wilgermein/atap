//! # Task Data
//! Task data is details about the current task
//! with it's actual value stored as bytes

use std::{mem};

/// Representation of a tasks data
/// and a ready value for blocking
#[repr(align(8))]
pub(crate) struct TaskData {
    data: Vec<u8>,
}

impl TaskData {
    /// Creates a new `TaskData`
    /// 
    /// `T` is used to find the size of the data field
    pub(crate) fn new<S>() -> Self {
        let size = mem::size_of::<S>();

        Self {
            data: Vec::with_capacity(size),
        }
    }

    /// Gets the data as an owned value by cloning it
    pub(crate) fn get_data<'a>(&self) -> Vec<u8> {
        self.data.clone()
    }

    /// Sets the inner data
    pub(crate) fn set_data(&mut self, set: Vec<u8>) {
        self.data = set;
    }
}
