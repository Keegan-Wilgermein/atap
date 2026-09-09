//! # Task Handle
//! A handle that allows operations
//! on unfinished tasks across threads

use std::{marker::PhantomData};
use crate::{RuntimeError, executor::Executor};

/// A task handle
/// 
/// Task handles can be infinitely duplicated,
/// passed around threads, and
/// access data from any thread
#[derive(Hash)]
pub struct TaskHandle<T>
where
    T: Sized,
{
    id: usize,
    _pd: PhantomData<T>,
}

impl<T> TaskHandle<T>
where
    T: Sized,
{
    /// Creates a new task handle
    pub(crate) fn new(
        id: usize,
    ) -> Self {
        Self {
            id,
            _pd: PhantomData,
        }
    }

    /// Returns whether a value is ready or not
    pub fn ready(&self) -> bool {
        todo!()
    }

    /// Returns the data wrapped
    /// inside `Option<T>`
    pub fn maybe_join(&self) -> Option<T> {
        todo!()
    }

    /// Waits until the data is ready
    /// and returns it when it is
    pub fn join(&self) -> Result<T, RuntimeError> {
        // let status = unsafe {
        //     // Switch this for kqueue EVFILT_USER
        //     libc::os_sync_wait_on_address(
        //         self.ready_address,
        //         1,                                      // Truthy value so the read can be confirmed
        //         mem::size_of::<T>(),
        //         libc::OS_SYNC_WAIT_ON_ADDRESS_NONE,     // Single process waiting
        //     )
        // };

        // if status < 0 {
        //     return Err(RuntimeError::AddressLock);
        // }

        Ok(
            Executor::get_task_result(self.id)
        )
    }

    /// Duplicates the `TaskHandle`
    pub fn clone(&self) -> Self {
        Executor::add_listener();

        Self {
            id: self.id,
            _pd: PhantomData,
        }
    }
}
