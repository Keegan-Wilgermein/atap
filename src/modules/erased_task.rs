//! # Erased Task
//! Hides a `Task`'s output type, so tasks of every output
//! type can share one table

use crate::futures::task::Task;

/// A `Task` with its output type erased
pub(crate) trait ErasedTask: Send {
    /// Runs the task and writes its output into `payload`
    ///
    /// ## Safety
    /// `payload` must point at enough writable, aligned bytes for
    /// the output, and anything already there must have been
    /// dropped
    unsafe fn run(&mut self, reactor_id: i32, task_id: usize, payload: *mut u8);
}

impl<F> ErasedTask for F
where
    F: Task,
{
    #[inline(always)]
    unsafe fn run(&mut self, reactor_id: i32, task_id: usize, payload: *mut u8) {
        // Spawned tasks prepare here, on the thread about to run them
        self.prepare();

        let out = self.execute(reactor_id, task_id);

        unsafe { payload.cast::<F::Output>().write(out) };
    }
}
