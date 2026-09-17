//! # Task Handle
//! A handle that allows operations
//! on unfinished tasks across threads

use crate::{
    RuntimeError, TaskState,
    constants::MAX_TASK_ID,
    executor::Executor,
    modules::handle_kind::{HandleKind, Plain, Waiting},
};
use std::{
    hash::{Hash, Hasher},
    marker::PhantomData,
    mem::ManuallyDrop,
    ptr,
    time::{Duration, Instant},
};

/// A task handle
///
/// Can be cloned freely, sent between threads, and read from
/// any of them
///
/// `W` says what else the handle can do. A handle to a task spawned
/// with `wait_for` is a `TaskHandle<T, Waiting<I>>`, which can
/// [`give`](TaskHandle::give) it input
pub struct TaskHandle<T, W = Plain>
where
    T: Sized,
    W: HandleKind,
{
    id: usize,

    /// What this kind of handle carries beside the id
    extra: W::Extra,

    _pd: PhantomData<T>,
}

impl<T> TaskHandle<T, Plain>
where
    T: Sized,
{
    /// Creates a new task handle
    pub(crate) fn new(id: usize) -> Self {
        Self {
            id,
            extra: (),
            _pd: PhantomData,
        }
    }

    /// Turns this handle into one of another kind, keeping its claim
    /// on the task
    pub(crate) fn into_kind<W>(self, extra: W::Extra) -> TaskHandle<T, W>
    where
        W: HandleKind,
    {
        // Its claim moves to the new handle, so its drop never runs
        let plain = ManuallyDrop::new(self);

        TaskHandle {
            id: plain.id,
            extra,
            _pd: PhantomData,
        }
    }
}

impl<T, W> TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
{
    /// A handle to no task, which reads `NoSuchTask`
    pub(crate) fn detached() -> Self {
        Self {
            id: MAX_TASK_ID,
            extra: W::detached(),
            _pd: PhantomData,
        }
    }

    /// Another handle of the same kind on the same task, naming a
    /// different output
    pub(crate) fn retyped<O>(&self) -> TaskHandle<O, W> {
        Executor::add_listener(self.id);

        TaskHandle {
            id: self.id,
            extra: W::cloned(&self.extra),
            _pd: PhantomData,
        }
    }

    /// A plain handle on the same task, keeping this handle's claim
    /// on it but not its way to give
    pub(crate) fn into_plain(self) -> TaskHandle<T, Plain> {
        let handle = ManuallyDrop::new(self);

        W::dropped(handle.id, &handle.extra);

        // Moved out whole, since the handle's own drop never runs
        drop(unsafe { ptr::read(&handle.extra) });

        TaskHandle {
            id: handle.id,
            extra: (),
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
    ///
    /// A task spawned with `wait_for` that has never been given
    /// anything reads this too
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
    /// A task spawned with `wait_for` is only finished once it
    /// takes no more gives
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
    pub fn try_join(&self) -> Result<T, RuntimeError>
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
    pub fn try_take(&self) -> Result<T, RuntimeError> {
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

impl<T, I> TaskHandle<T, Waiting<I>>
where
    T: Sized,
    I: Send + 'static,
{
    /// Gives the task the value its next run is handed
    ///
    /// ## Behaviour
    /// A task waiting for a give starts its next run, or its next
    /// series, with it. Gives don't queue: one while a run is waiting
    /// to start or under way leaves only the newest value. A give
    /// during a single run owes the task one more run, and a give
    /// during a series only changes what the rest of the series is
    /// handed
    ///
    /// Only a give that starts something counts against the task's
    /// `count`
    ///
    /// ## Returns
    /// `Finished` once the task takes no more gives, and `Cancelled`
    /// or `TaskFailed` once it has ended another way. The value is
    /// dropped in those cases
    ///
    /// A handle from a plain `spawn` can't give:
    ///
    /// ```compile_fail,E0599
    /// use atap::{Runtime, compute::Compute};
    ///
    /// let handle = Runtime::task(Compute::compute(|()| 1)).spawn();
    /// let _ = handle.give(());
    /// ```
    pub fn give(&self, value: I) -> Result<(), RuntimeError> {
        Executor::give(self.id, &self.extra, value)
    }

    /// Whether the task is waiting for a give, with nothing owed
    ///
    /// False while a run or series a give started is queued or under
    /// way, and once the task takes no more gives. A give while this
    /// reads true starts something
    ///
    /// #### Note
    /// A snapshot. A give from another thread can change it at once
    pub fn is_waiting(&self) -> bool {
        self.extra.gate().waiting()
    }

    /// Whether the task still takes gives
    #[inline(always)]
    pub(crate) fn takes_gives(&self) -> bool {
        !self.extra.gate().finished()
    }
}

/// Turns a timeout into the moment it runs out, or `None` if
/// the clock can't hold it
#[inline(always)]
fn deadline(timeout: Duration) -> Option<Instant> {
    Instant::now().checked_add(timeout)
}

impl<T, W> Clone for TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
{
    /// Duplicates the `TaskHandle`
    fn clone(&self) -> Self {
        Executor::add_listener(self.id);

        Self {
            id: self.id,
            extra: W::cloned(&self.extra),
            _pd: PhantomData,
        }
    }
}

/// Two handles are equal when they point at the same task
impl<T, W> PartialEq for TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
{
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<T, W> Eq for TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
{
}

/// Hashed on the task, the same as equality
impl<T, W> Hash for TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
{
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.id.hash(state);
    }
}

impl<T, W> std::fmt::Debug for TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
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

impl<T, W> Drop for TaskHandle<T, W>
where
    T: Sized,
    W: HandleKind,
{
    /// Gives up this handle's claim on the task
    ///
    /// Dropping all handles clears the task entirely
    fn drop(&mut self) {
        W::dropped(self.id, &self.extra);
        Executor::drop_listener(self.id);
    }
}
