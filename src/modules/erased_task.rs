//! # Erased Task
//! Hides a `Task`'s output type, so tasks of every output
//! type can share one table

use crate::futures::task::{
    Task,
    sealed::{Park, Step},
};

/// A `Task` with its output type erased
pub(crate) trait ErasedTask: Send {
    /// Runs one step of the task, writing its output into
    /// `payload` if the run finished
    ///
    /// `resumed` is a run coming back from a park, which carries
    /// on where it was rather than preparing afresh
    ///
    /// ## Returns
    /// What the task is waiting on, if it parked. Nothing is
    /// written in that case
    ///
    /// ## Safety
    /// `payload` must point at enough writable, aligned bytes for
    /// the output, and anything already there must have been
    /// dropped
    unsafe fn run(
        &mut self,
        reactor_id: i32,
        task_id: usize,
        payload: *mut u8,
        resumed: bool,
    ) -> Option<Park>;
}

impl<F> ErasedTask for F
where
    F: Task,
{
    #[inline(always)]
    unsafe fn run(
        &mut self,
        reactor_id: i32,
        task_id: usize,
        payload: *mut u8,
        resumed: bool,
    ) -> Option<Park> {
        // Spawned tasks prepare here, on the thread about to run them
        if !resumed {
            self.prepare();
        }

        match self.step(reactor_id, task_id) {
            Step::Done(out) => {
                unsafe { payload.cast::<F::Output>().write(out) };

                None
            }

            Step::Park(park) => Some(park),
        }
    }
}
