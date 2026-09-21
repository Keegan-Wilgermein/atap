//! # Channel
//! The constructor every channel is opened from

use crate::{
    RuntimeError,
    futures::channel::{
        core::Core,
        ends::{BoundedSender, Receiver, Sender},
    },
};
use std::marker::PhantomData;

/// Hands values from any number of senders to any number of
/// receivers, first in first out
///
/// It doesn't implement `Task`. [`Channel::new`] starts the
/// setup, and `open` makes the two ends
///
/// ## Behaviour
/// Each value goes to exactly one receiver. A receive is a task
/// that waits for a value, and a spawned one holds no thread while
/// it waits. Sending on an unbounded channel never waits, so it is
/// a plain call. A bounded one waits for room, so its send is a
/// task too
///
/// ```no_run
/// # use atap::{Runtime, channel::Channel};
/// # fn main() -> Result<(), atap::RuntimeError> {
/// let (tx, rx) = Channel::new::<u64>().open()?;
///
/// let next = Runtime::task(rx.recv()).spawn();
/// tx.send(7)?;
///
/// assert_eq!(next.join()??, 7);
///
/// let (tx, rx) = Channel::new::<u64>().bounded(1).open()?;
///
/// Runtime::block(tx.send(1))?;
/// let waiting = Runtime::task(tx.send(2)).spawn();
///
/// assert_eq!(Runtime::block(rx.recv())?, 1);
/// waiting.join()??;
/// # Ok(())
/// # }
/// ```
///
/// ## Closing
/// Once every sender is gone, a receive takes what is left and then
/// gives [`RuntimeError::Closed`]. Once every receiver is gone, a
/// send gives [`RuntimeError::Closed`] and its value is dropped.
/// A task waiting to send or receive counts as the end it came from
///
/// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
pub struct Channel;

impl Channel {
    /// Starts setting up a channel of `T`, unbounded until told
    /// otherwise
    #[allow(clippy::new_ret_no_self)]
    pub fn new<T>() -> Unbounded<T>
    where
        T: Send + 'static,
    {
        Unbounded(PhantomData)
    }
}

/// A channel that holds as many values as it is sent
#[must_use = "a channel is only made by `open`"]
pub struct Unbounded<T>(PhantomData<fn() -> T>);

impl<T> Unbounded<T>
where
    T: Send + 'static,
{
    /// Holds at most `capacity` values, so a send waits for room
    ///
    /// A capacity of zero holds one
    pub fn bounded(self, capacity: usize) -> Bounded<T> {
        Bounded {
            capacity: capacity.max(1),
            _values: PhantomData,
        }
    }

    /// Makes the channel
    ///
    /// ## Returns
    /// Its sending and receiving ends. Fails only if the kernel
    /// won't hand out the descriptors a channel waits on
    pub fn open(self) -> Result<(Sender<T>, Receiver<T>), RuntimeError> {
        let core = Core::open(None)?;

        Ok((Sender::new(core.clone()), Receiver::new(core)))
    }
}

/// A channel that holds a fixed number of values
#[must_use = "a channel is only made by `open`"]
pub struct Bounded<T> {
    /// Values it holds before a send waits
    capacity: usize,

    _values: PhantomData<fn() -> T>,
}

impl<T> Bounded<T>
where
    T: Send + 'static,
{
    /// Makes the channel
    ///
    /// ## Returns
    /// Its sending and receiving ends. Fails only if the kernel
    /// won't hand out the descriptors a channel waits on
    pub fn open(self) -> Result<(BoundedSender<T>, Receiver<T>), RuntimeError> {
        let core = Core::open(Some(self.capacity))?;

        Ok((BoundedSender::new(core.clone()), Receiver::new(core)))
    }
}
