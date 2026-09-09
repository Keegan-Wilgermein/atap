//! # Erased Task
//! Hides a `Task`'s output type so that tasks of every
//! shape can sit in the same table
//!
//! `Task` carries an associated `Output`, which means
//! `Box<dyn Task>` isn't a type that can be written down.
//! Erasing the output behind a raw pointer is what lets one
//! slot hold any task at all

use crate::futures::task::Task;

/// A `Task` with its output type erased
pub(crate) trait ErasedTask: Send {
    /// Runs the task and writes its output into `payload`
    ///
    /// ## Safety
    /// `payload` must point at least `size_of::<Output>()`
    /// writable, uninitialised bytes, aligned for the output
    /// type. The slot's payload offset guarantees both
    unsafe fn run(self: Box<Self>, reactor_id: i32, task_id: usize, payload: *mut u8);
}

impl<F> ErasedTask for F
where
    F: Task,
{
    #[inline(always)]
    unsafe fn run(mut self: Box<Self>, reactor_id: i32, task_id: usize, payload: *mut u8) {
        // Blocking calls prepare on the calling thread, so
        // spawned ones prepare here, on the thread that is
        // actually about to run them
        self.prepare();

        let out = self.execute(reactor_id, task_id);

        // Moved once, straight into its final home, rather
        // than copied out through a buffer on the way
        unsafe { payload.cast::<F::Output>().write(out) };
    }
}
