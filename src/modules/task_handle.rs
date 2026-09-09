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
/// Task handles can be infinitely duplicated,
/// passed around threads, and
/// access data from any thread
///
/// #### Note
/// A handle is an id and nothing else. Every operation goes
/// back through the `Executor`, which is the only thing that
/// knows whether the task behind that id is still there
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
    /// For telling one task from another in a log, and nothing
    /// more. An id is only meaningful while the task holding it
    /// is alive — once the last listener goes the slot is
    /// reused, and the same number then belongs to a task with
    /// no relation to this one
    pub fn id(&self) -> usize {
        self.id
    }

    /// Returns whether the task has settled
    ///
    /// #### Note
    /// True means a read won't block, not that there is
    /// something to read. A cancelled, failed or already taken
    /// task has settled too, and reading one gives back an
    /// error. Use `is_ready` for the narrower question of
    /// whether an output is actually waiting
    pub fn settled(&self) -> bool {
        self.state().terminal()
    }

    /// What the task is doing right now
    ///
    /// #### Note
    /// A snapshot rather than a promise. A task that isn't
    /// settled can have moved on by the time this is looked at,
    /// and a repeating one moves on regardless — the states it
    /// settles in are what it looks like *between* runs rather
    /// than the end of it
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

    /// Waits until the task settles, without reading it
    ///
    /// ## Returns
    /// How it settled, so a caller can decide what to do about
    /// it before committing to a read
    ///
    /// ## Behaviour
    /// Borrows and reads nothing, so it costs no clone and
    /// claims no output. Useful for an output that isn't
    /// `Clone`, where finding out whether there is anything
    /// worth taking would otherwise mean taking it
    ///
    /// #### Note
    /// A repeating task settles between runs rather than at the
    /// end, so this comes back once a run has published rather
    /// than once the series is over. Waiting for that would be
    /// waiting forever
    pub fn wait(&self) -> Result<TaskState, RuntimeError> {
        Executor::wait(self.id)
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
    /// way past would give up this listener's claim on the task
    pub fn maybe_join(&self) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Executor::poll_result(self.id)
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
    /// Comes back the moment the task settles, or the moment
    /// the timeout is up, whichever happens first. The wait
    /// costs the thread nothing while it lasts and the clock is
    /// monotonic, so setting the system clock part way through
    /// can't lengthen or shorten it
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
        Executor::clone_result_until(self.id, deadline(timeout))
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

    /// Moves the output out if it is there, without waiting
    ///
    /// ## Returns
    /// The output, or `NotReady` if the task hasn't settled.
    /// `AlreadyTaken` if another listener moved it out first
    ///
    /// ## Behaviour
    /// The `take` counterpart of `maybe_join`, and the only way
    /// to poll a task whose output isn't `Clone`
    ///
    /// #### Note
    /// Borrows where `take` consumes. Only one caller can ever
    /// succeed either way — the claim is what decides that, not
    /// the ownership — so borrowing costs nothing and lets a
    /// poll that came back with nothing be tried again
    pub fn maybe_take(&self) -> Result<T, RuntimeError> {
        Executor::poll_take(self.id)
    }

    /// Waits a while to move the output out, and gives up if
    /// it doesn't arrive
    ///
    /// ## Returns
    /// The output, or `NotReady` if the task still hasn't
    /// settled when the wait is up
    ///
    /// ## Behaviour
    /// The same wait `join_with_timeout` does, ending on
    /// whichever of the task and the timeout comes first
    ///
    /// #### Note
    /// A timeout leaves the output exactly where it was.
    /// Nothing is claimed unless the value actually comes back,
    /// so giving up costs the caller the wait and nothing else,
    /// and a later read still finds it
    pub fn take_with_timeout(&self, timeout: Duration) -> Result<T, RuntimeError> {
        Executor::take_result_until(self.id, deadline(timeout))
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

/// Turns a timeout into the moment it runs out
///
/// `None` for a timeout so far out that the clock can't hold
/// the answer, which is a caller asking to wait for
/// approximately forever and is served by waiting for exactly
/// that
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

/// Two handles are equal when they point at the same task
///
/// #### Note
/// Written out rather than derived, the same way `Clone` is,
/// though for a different reason. A derive would put a
/// `T: PartialEq` bound on the impl, so a handle to an output
/// that can't be compared couldn't be compared either — and
/// this compares ids, which have nothing to do with `T`
impl<T> PartialEq for TaskHandle<T>
where
    T: Sized,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T> Eq for TaskHandle<T> where T: Sized {}

/// Hashed on the id, for the same reason equality is
///
/// #### Note
/// Paired with `Eq` deliberately. A `Hash` without one is a
/// type that can't be a `HashMap` key at all, which is the only
/// thing hashing a handle is for
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
    /// The id and how the task is doing
    ///
    /// #### Note
    /// Reads the state rather than printing the id alone,
    /// because an id on its own says nothing a caller couldn't
    /// already see, and the reason to look at a handle in a
    /// debugger is almost always to find out what it is doing
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
    /// Never frees anything itself. The count going to zero
    /// is what frees a task, and only the `Executor` is in a
    /// position to notice that and act on it
    fn drop(&mut self) {
        Executor::drop_listener(self.id);
    }
}
