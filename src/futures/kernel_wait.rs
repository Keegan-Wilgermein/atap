//! # Kernel wait
//! The half of a task that hands itself to the kernel and
//! waits for an answer
//!
//! Split off `Task` because most tasks have no answer to
//! wait for. A file read holds its thread inside `pread` and
//! comes back with the whole result; only a task whose work
//! *is* a kevent has anything to say here

use crate::futures::task::Task;

/// A task that waits on a kqueue for something
/// the kernel will tell it about
///
/// ## Behaviour
/// Two questions, asked in order. `get_intptr_t_data` is what
/// goes in the event's `data` field, which is the only place a
/// filter takes a parameter. `offload` is the wait itself and
/// whatever has to happen to the result on the way back out
///
/// #### Note
/// Crate private, and deliberately not defaulted onto `Task`.
/// `offload` returns `Self::Output`, so a default body would
/// have to invent one — and the only honest way to do that is
/// to call `execute`, which is circular for every task that
/// reaches `offload` from inside `execute` in the first place
pub(crate) trait KernelWait: Task {
    /// Gets the type specific data to be passed into the event
    fn get_intptr_t_data(&self) -> libc::intptr_t;

    /// Prepares data and handles what comes back
    /// from the kernel
    ///
    /// Not required to make any syscalls
    ///
    /// `queue` is this thread's own kqueue when it has one,
    /// which is the cheap path, and `None` when it doesn't
    fn offload(&self, queue: Option<i32>, reactor_id: i32, task_id: usize) -> Self::Output;
}
