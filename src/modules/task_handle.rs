//! # Task Handle
//! A handle that allows operations
//! on unfinished tasks across threads

use crate::{Runtime, RuntimeError, Sleep, executor::Executor};
use std::{marker::PhantomData, time::Duration};

/// A task handle
///
/// Task handles can be infinitely duplicated,
/// passed around threads, and
/// access data from any thread
///
/// #### Note
/// A handle is an id and nothing else. Every operation goes
/// back through the `Executor`, which is the only thing that
/// knows whether the task behind that id is still there
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
    pub(crate) fn new(id: usize) -> Self {
        Self {
            id,
            _pd: PhantomData,
        }
    }

    /// Returns whether a value is ready or not
    ///
    /// #### Note
    /// True means the task has settled and a read won't
    /// block, not that there is definitely something to
    /// read. A cancelled or already taken task has settled
    /// too, and reading one gives back an error
    pub fn ready(&self) -> bool {
        Executor::state(self.id).terminal()
    }

    /// Reads the output if it is there, without waiting
    ///
    /// ## Returns
    /// The output, or `NotReady` if the task hasn't settled.
    /// A task that settled without an output to give says so
    /// with the reason, rather than being folded in with one
    /// that simply isn't finished
    ///
    /// #### Note
    /// Borrows rather than consuming. A poll that comes back
    /// with nothing has to leave the handle behind for the
    /// caller to poll again, and dropping the handle on the
    /// way past would end the task instead
    pub fn maybe_join(&self) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        if !self.ready() {
            return Err(RuntimeError::NotReady);
        }

        Executor::clone_result(self.id)
    }

    /// Waits a while for the output, and gives up if it
    /// doesn't arrive
    ///
    /// ## Returns
    /// The output, or `NotReady` if the task still hasn't
    /// settled when the wait is up. Any other error is the
    /// task's own, and waiting longer wouldn't have helped
    ///
    /// ## Behaviour
    /// The wait is an ordinary blocking sleep on the calling
    /// thread, so it is as accurate as `Sleep` is and costs
    /// the thread nothing while it waits
    ///
    /// A task that has already settled is read straight away
    /// rather than waited for
    ///
    /// #### Note
    /// Borrows, like `maybe_join` and for the same reason.
    /// Running out of patience isn't an answer, and a handle
    /// that was thrown away because the caller got bored
    /// couldn't be waited on again
    pub fn join_with_timeout(&self, timeout: Duration) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        if !self.ready() {
            Runtime::block(Sleep::sleep(timeout, false));
        }

        self.maybe_join()
    }

    /// Waits until the data is ready
    /// and returns it when it is
    ///
    /// The output stays where it is and a clone comes back,
    /// so every listener on a task can have one
    pub fn join(self) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Executor::clone_result(self.id)
    }

    /// Waits until the data is ready and
    /// moves it out of the task
    ///
    /// ## Returns
    /// The output, or `AlreadyTaken` if another listener
    /// moved it out first
    ///
    /// #### Note
    /// Only one caller can ever succeed. Use this for an
    /// output that can't be cloned, or when the clone is
    /// worth avoiding, and `join` when more than one
    /// listener needs the value
    pub fn take(self) -> Result<T, RuntimeError> {
        Executor::take_result(self.id)
    }

    /// Abandons the task
    ///
    /// Every other listener's read comes back `Cancelled`
    /// from here on, and the output is dropped rather than
    /// handed out
    ///
    /// #### Note
    /// This abandons a task, it doesn't interrupt one. A task
    /// already in flight has no way to be taken back off the
    /// kernel, so it runs to the end either way, its result
    /// simply goes nowhere
    pub fn cancel(self) {
        Executor::cancel(self.id);
    }
}

impl<T> Clone for TaskHandle<T>
where
    T: Sized,
{
    /// Duplicates the `TaskHandle`
    ///
    /// #### Note
    /// Written out rather than derived on purpose. A derived
    /// clone would copy the id without telling the `Executor`,
    /// so the first of the two handles to be dropped would
    /// take the listener count to zero and free the task out
    /// from under the other one
    fn clone(&self) -> Self {
        Executor::add_listener(self.id);

        Self {
            id: self.id,
            _pd: PhantomData,
        }
    }
}

impl<T> Drop for TaskHandle<T>
where
    T: Sized,
{
    /// Gives up this handle's claim on the task
    ///
    /// Never frees anything itself. The count going to zero
    /// is what frees a task, and only the `Executor` is in a
    /// position to notice that and act on it
    fn drop(&mut self) {
        Executor::drop_listener(self.id);
    }
}
