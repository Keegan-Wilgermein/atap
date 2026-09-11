//! # Kernel wait
//! The part of a task that hands itself to the kernel and
//! waits for an answer

use crate::futures::task::Task;

/// A task that waits on a kqueue for something the kernel will
/// tell it about
pub(crate) trait KernelWait: Task {
    /// Gets the type specific data to be passed into the event
    fn get_intptr_t_data(&self) -> libc::intptr_t;

    /// Waits for the kernel and turns what comes back into the
    /// output
    ///
    /// `queue` is this thread's own kqueue, or `None` if it
    /// couldn't get one
    fn offload(&self, queue: Option<i32>, reactor_id: i32, task_id: usize) -> Self::Output;
}
