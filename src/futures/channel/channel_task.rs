//! # Channel tasks
//! Waiting to receive from a channel, and waiting for room to send

use crate::{
    RuntimeError,
    futures::{
        channel::{
            core::Refused,
            ends::{BoundedSender, Receiver},
        },
        net::step::wait_on,
        task::{
            Nothing, Task,
            sealed::{self, Step},
        },
    },
    modules::{input::Token, park},
};
use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError},
};

/// Takes the oldest value from a channel
///
/// ## Returns
/// The value, or [`RuntimeError::Closed`] once nothing is left and
/// nothing can be sent
///
/// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ChannelRecvTask<T> {
    /// The end it receives through
    from: Receiver<T>,
}

impl<T> ChannelRecvTask<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(from: Receiver<T>) -> Self {
        Self { from }
    }

    fn advance(&mut self) -> Step<Result<T, RuntimeError>> {
        match self.from.core().pop() {
            Err(RuntimeError::NotReady) => {
                match wait_on(self.from.core().items_bell(), libc::EVFILT_READ) {
                    Ok(step) => step,
                    Err(error) => Step::Done(Err(error)),
                }
            }

            done => Step::Done(done),
        }
    }
}

/// Adds a value to a bounded channel once there is room
///
/// ## Returns
/// Nothing once the value is in, or [`RuntimeError::Closed`] if no
/// receiver is left
///
/// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ChannelSendTask<T> {
    /// The end it sends through
    to: BoundedSender<T>,

    /// The value, until it is in. Shared by every copy, so it only
    /// goes in once
    value: Arc<Mutex<Option<T>>>,
}

impl<T> ChannelSendTask<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(to: BoundedSender<T>, value: T) -> Self {
        Self {
            to,
            value: Arc::new(Mutex::new(Some(value))),
        }
    }

    fn advance(&mut self) -> Step<Result<(), RuntimeError>> {
        let mut slot = self.value.lock().unwrap_or_else(PoisonError::into_inner);

        let Some(value) = slot.take() else {
            return Step::Done(Err(RuntimeError::Finished));
        };

        match self.to.core().push(value) {
            Ok(()) => Step::Done(Ok(())),
            Err(Refused::Closed) => Step::Done(Err(RuntimeError::Closed)),

            Err(Refused::Full(value)) => {
                *slot = Some(value);

                match wait_on(self.to.core().room_bell(), libc::EVFILT_READ) {
                    Ok(step) => step,
                    Err(error) => Step::Done(Err(error)),
                }
            }
        }
    }
}

impl<T> Clone for ChannelRecvTask<T> {
    fn clone(&self) -> Self {
        Self {
            from: self.from.clone(),
        }
    }
}

impl<T> Clone for ChannelSendTask<T> {
    fn clone(&self) -> Self {
        Self {
            to: self.to.clone(),
            value: Arc::clone(&self.value),
        }
    }
}

impl<T> fmt::Debug for ChannelRecvTask<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelRecvTask")
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for ChannelSendTask<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChannelSendTask")
            .finish_non_exhaustive()
    }
}

impl<T> sealed::Sealed for ChannelRecvTask<T> {}
impl<T> sealed::Sealed for ChannelSendTask<T> {}

impl<T> Task for ChannelRecvTask<T>
where
    T: Send + 'static,
{
    type Output = Result<T, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        self.advance()
    }
}

impl<T> Task for ChannelSendTask<T>
where
    T: Send + 'static,
{
    type Output = Result<(), RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        self.advance()
    }
}
