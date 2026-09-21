//! # Ends
//! The sending and receiving ends of a channel

use crate::{
    RuntimeError,
    futures::channel::{
        channel_task::{ChannelRecvTask, ChannelSendTask},
        core::{Core, Refused},
    },
};
use std::{fmt, sync::Arc};

/// Sends on an unbounded channel
///
/// Cloning it makes another sender. The channel closes for its
/// receivers once every sender is gone
pub struct Sender<T> {
    core: Arc<Core<T>>,
}

impl<T> Sender<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(core: Arc<Core<T>>) -> Self {
        Self { core }
    }

    /// Adds `value` to the channel
    ///
    /// ## Returns
    /// [`RuntimeError::Closed`] if no receiver is left, and the
    /// value is dropped
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn send(&self, value: T) -> Result<(), RuntimeError> {
        match self.core.push(value) {
            Ok(()) => Ok(()),
            Err(_) => Err(RuntimeError::Closed),
        }
    }

    /// Values sent and not yet received
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// Whether nothing is waiting to be received
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Sends on a bounded channel
///
/// Cloning it makes another sender. The channel closes for its
/// receivers once every sender is gone
pub struct BoundedSender<T> {
    core: Arc<Core<T>>,
}

impl<T> BoundedSender<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(core: Arc<Core<T>>) -> Self {
        Self { core }
    }

    /// A task that adds `value` once there is room
    ///
    /// ## Returns
    /// Nothing once the value is in. [`RuntimeError::Closed`] if no
    /// receiver is left, and the value is dropped
    ///
    /// #### Note
    /// The task sends its value once. A rerun has nothing left to
    /// send and gives [`RuntimeError::Finished`]
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    /// [`RuntimeError::Finished`]: crate::RuntimeError::Finished
    pub fn send(&self, value: T) -> ChannelSendTask<T> {
        ChannelSendTask::new(self.clone(), value)
    }

    /// Adds `value` if there is room right now
    ///
    /// ## Returns
    /// The value back if the channel is full, or
    /// [`RuntimeError::Closed`] if no receiver is left
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn try_send(&self, value: T) -> Result<Result<(), T>, RuntimeError> {
        match self.core.push(value) {
            Ok(()) => Ok(Ok(())),
            Err(Refused::Full(value)) => Ok(Err(value)),
            Err(Refused::Closed) => Err(RuntimeError::Closed),
        }
    }

    /// Values sent and not yet received
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// Whether nothing is waiting to be received
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn core(&self) -> &Core<T> {
        &self.core
    }
}

/// Receives from a channel
///
/// Cloning it makes another receiver. Each value goes to one of
/// them
pub struct Receiver<T> {
    core: Arc<Core<T>>,
}

impl<T> Receiver<T>
where
    T: Send + 'static,
{
    pub(crate) fn new(core: Arc<Core<T>>) -> Self {
        Self { core }
    }

    /// A task that takes the oldest value, waiting for one if there
    /// is none yet
    ///
    /// ## Returns
    /// The value, or [`RuntimeError::Closed`] once every sender is
    /// gone and nothing is left
    ///
    /// A repeat takes a value on every run, and a run's output is
    /// replaced when the next one starts. When every value matters,
    /// take each with a receive of its own
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv(&self) -> ChannelRecvTask<T> {
        ChannelRecvTask::new(self.clone())
    }

    /// Takes the oldest value if there is one right now
    ///
    /// ## Returns
    /// [`RuntimeError::NotReady`] if nothing is waiting, or
    /// [`RuntimeError::Closed`] once nothing ever will be
    ///
    /// [`RuntimeError::NotReady`]: crate::RuntimeError::NotReady
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn try_recv(&self) -> Result<T, RuntimeError> {
        self.core.pop()
    }

    /// Values waiting to be received
    pub fn len(&self) -> usize {
        self.core.len()
    }

    /// Whether nothing is waiting to be received
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn core(&self) -> &Core<T> {
        &self.core
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.core.add_sender();

        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.core.add_sender();

        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.core.add_receiver();

        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.core.drop_sender();
    }
}

impl<T> Drop for BoundedSender<T> {
    fn drop(&mut self) {
        self.core.drop_sender();
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.core.drop_receiver();
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Sender").finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for BoundedSender<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedSender")
            .finish_non_exhaustive()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Receiver").finish_non_exhaustive()
    }
}
