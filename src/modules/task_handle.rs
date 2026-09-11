//! # Task Handle
//! A handle that allows operations
//! on unfinished tasks across threads

use crate::{RuntimeError, TaskState, executor::Executor};
use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
    time::{Duration, Instant},
};

/// A task handle
///
/// Can be cloned freely, sent between threads, and read from
/// any of them
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

    /// The id of the task behind this handle
    ///
    /// #### Note
    /// Only meaningful while the task is alive. Once every handle
    /// to it is gone, the same id can belong to another task
    pub fn id(&self) -> usize {
        self.id
    }

    /// Whether the task has settled, so a read won't block
    ///
    /// #### Note
    /// Doesn't mean there is an output. Use `is_ready` for that
    pub fn settled(&self) -> bool {
        self.state().terminal()
    }

    /// What the task is doing right now
    ///
    /// #### Note
    /// A snapshot. A repeating task moves on between runs whether
    /// anything is looking or not
    pub fn state(&self) -> TaskState {
        Executor::state(self.id)
    }

    /// Whether the task is queued and waiting to start
    pub fn is_pending(&self) -> bool {
        self.state() == TaskState::Pending
    }

    /// Whether the task is running right now
    pub fn is_running(&self) -> bool {
        self.state() == TaskState::Running
    }

    /// Whether an output is written and waiting to be read
    pub fn is_ready(&self) -> bool {
        self.state() == TaskState::Ready
    }

    /// Whether the output has already been moved out by `take`
    pub fn is_taken(&self) -> bool {
        self.state() == TaskState::Taken
    }

    /// Whether a listener abandoned the task
    pub fn is_cancelled(&self) -> bool {
        self.state() == TaskState::Cancelled
    }

    /// Whether nothing is ever going to produce an output
    pub fn is_failed(&self) -> bool {
        self.state() == TaskState::Failed
    }

    /// Whether the task has settled and won't run again
    ///
    /// ## Behaviour
    /// For a repeat, `settled` is also true between runs. This is
    /// only true once a bounded series has run out, or the series
    /// was cancelled or failed. A settled one shot reads true too
    ///
    /// A series that ran out keeps its last output, so `join` and
    /// `take` still return it
    pub fn is_finished(&self) -> bool {
        Executor::finished(self.id)
    }

    /// Waits until the task settles, without reading it
    ///
    /// ## Returns
    /// How it settled
    ///
    /// #### Note
    /// A repeat settles after each run, so this returns once a run
    /// has published rather than when the series ends
    pub fn wait(&self) -> Result<TaskState, RuntimeError> {
        Executor::wait(self.id)
    }

    /// Reads the output if it is there, without waiting
    ///
    /// ## Returns
    /// The output, `NotReady` if the task hasn't settled, or why
    /// it settled without one
    pub fn maybe_join(&self) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Executor::poll_result(self.id)
    }

    /// Waits up to `timeout` for the output
    ///
    /// ## Returns
    /// The output, or `NotReady` if the task still hasn't settled
    /// when the time is up. Any other error is the task's own
    ///
    /// Returns as soon as the task settles
    pub fn join_with_timeout(&self, timeout: Duration) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Executor::clone_result_until(self.id, deadline(timeout))
    }

    /// Waits until the data is ready and returns it when it is
    ///
    /// A clone comes back, so every listener can have one
    pub fn join(self) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Executor::clone_result(self.id)
    }

    /// Moves the output out if it is there, without waiting
    ///
    /// ## Returns
    /// The output, `NotReady` if the task hasn't settled, or
    /// `AlreadyTaken` if another listener moved it out first
    pub fn maybe_take(&self) -> Result<T, RuntimeError> {
        Executor::poll_take(self.id)
    }

    /// Waits up to `timeout` to move the output out
    ///
    /// ## Returns
    /// The output, or `NotReady` if the task still hasn't settled
    /// when the time is up
    ///
    /// Giving up leaves the output where it was, for a later read
    pub fn take_with_timeout(&self, timeout: Duration) -> Result<T, RuntimeError> {
        Executor::take_result_until(self.id, deadline(timeout))
    }

    /// Waits until the data is ready and moves it out of the task
    ///
    /// ## Returns
    /// The output, or `AlreadyTaken` if another listener moved it
    /// out first
    ///
    /// Only one caller can ever get it. Use `join` when more than
    /// one listener needs the value
    pub fn take(self) -> Result<T, RuntimeError> {
        Executor::take_result(self.id)
    }

    /// Abandons the task
    ///
    /// Every other listener's read returns `Cancelled` from here on
    ///
    /// #### Note
    /// A task already running may take a moment to stop, but its
    /// output is thrown away either way
    pub fn cancel(self) {
        Executor::cancel(self.id);
    }
}

/// Turns a timeout into the moment it runs out, or `None` if
/// the clock can't hold it
#[inline(always)]
fn deadline(timeout: Duration) -> Option<Instant> {
    Instant::now().checked_add(timeout)
}

impl<T> Clone for TaskHandle<T>
where
    T: Sized,
{
    /// Duplicates the `TaskHandle`
    ///
    /// Written out rather than derived, because every handle holds
    /// its own claim on the task
    fn clone(&self) -> Self {
        Executor::add_listener(self.id);

        Self {
            id: self.id,
            _pd: PhantomData,
        }
    }
}

/// Two handles are equal when they point at the same task
impl<T> PartialEq for TaskHandle<T>
where
    T: Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for TaskHandle<T> where T: Sized {}

/// Hashed on the task, the same as equality
impl<T> Hash for TaskHandle<T>
where
    T: Sized,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T> std::fmt::Debug for TaskHandle<T>
where
    T: Sized,
{
    /// The id and the task's current state
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskHandle")
            .field("id", &self.id)
            .field("state", &self.state())
            .finish()
    }
}

impl<T> Drop for TaskHandle<T>
where
    T: Sized,
{
    /// Gives up this handle's claim on the task
    /// 
    /// Dropping all handles clears the task entirely
    fn drop(&mut self) {
        Executor::drop_listener(self.id);
    }
}
