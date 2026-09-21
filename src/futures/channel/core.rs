//! # Core
//! What both ends of a channel share: the values, and the
//! doorbells a waiting end parks on

use crate::{
    RuntimeError,
    futures::net::socket::configure,
    modules::{fd::Fd, int_check::IntCheck},
};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

/// A channel, shared by every end of it
///
/// Two doorbells, one each way through a socket pair. A byte sits
/// in `items` exactly while a receive has something to do, and in
/// `room` exactly while a send has
pub(crate) struct Core<T> {
    /// The values, and who is still holding an end
    state: Mutex<State<T>>,

    /// Rung by writing here, and read from `items_bell`
    items_ring: Fd,

    /// Where a receive parks
    items_bell: Fd,

    /// Rung by writing here, and read from `room_bell`
    room_ring: Fd,

    /// Where a bounded send parks
    room_bell: Fd,
}

/// What the lock guards
pub(crate) struct State<T> {
    /// The values waiting, oldest first
    queue: VecDeque<T>,

    /// How many values fit, or `None` for no limit
    capacity: Option<usize>,

    /// Sending ends still open
    senders: usize,

    /// Receiving ends still open
    receivers: usize,
}

/// Why a value couldn't go in
pub(crate) enum Refused<T> {
    /// Nobody can receive it
    Closed,

    /// There is no room yet, and the value is handed back
    Full(T),
}

impl<T> Core<T> {
    /// A channel with one end of each kind, and room for
    /// `capacity` values
    pub(crate) fn open(capacity: Option<usize>) -> Result<Arc<Self>, RuntimeError> {
        let mut pair = [0; 2];

        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, pair.as_mut_ptr()) }
            .check()?;

        let (first, second) = (Fd::new(pair[0]), Fd::new(pair[1]));

        configure(first.raw())?;
        configure(second.raw())?;

        // One pair carries both doorbells, one in each direction
        let items_ring = Fd::new(unsafe { libc::dup(first.raw()) }.check()?);
        let room_ring = Fd::new(unsafe { libc::dup(second.raw()) }.check()?);

        let core = Self {
            state: Mutex::new(State {
                queue: VecDeque::new(),
                capacity,
                senders: 1,
                receivers: 1,
            }),
            items_ring,
            items_bell: second,
            room_ring,
            room_bell: first,
        };

        // An empty bounded channel has room from the start
        if capacity.is_some() {
            ring(&core.room_ring);
        }

        Ok(Arc::new(core))
    }

    /// The descriptor a receive parks on
    #[inline(always)]
    pub(crate) fn items_bell(&self) -> libc::c_int {
        self.items_bell.raw()
    }

    /// The descriptor a bounded send parks on
    #[inline(always)]
    pub(crate) fn room_bell(&self) -> libc::c_int {
        self.room_bell.raw()
    }

    fn lock(&self) -> MutexGuard<'_, State<T>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Adds a value, if there is room and anyone to take it
    pub(crate) fn push(&self, value: T) -> Result<(), Refused<T>> {
        let mut state = self.lock();

        if state.receivers == 0 {
            return Err(Refused::Closed);
        }

        if state
            .capacity
            .is_some_and(|capacity| state.queue.len() >= capacity)
        {
            return Err(Refused::Full(value));
        }

        let was_empty = state.queue.is_empty();

        state.queue.push_back(value);

        if was_empty && state.senders > 0 {
            ring(&self.items_ring);
        }

        if state.capacity == Some(state.queue.len()) {
            drain(&self.room_bell);
        }

        Ok(())
    }

    /// Takes the oldest value
    ///
    /// ## Returns
    /// The value, `NotReady` if there is none yet, or `Closed` if
    /// there will never be one
    pub(crate) fn pop(&self) -> Result<T, RuntimeError> {
        let mut state = self.lock();

        let was_full = state.capacity == Some(state.queue.len());

        let Some(value) = state.queue.pop_front() else {
            return match state.senders {
                0 => Err(RuntimeError::Closed),
                _ => Err(RuntimeError::NotReady),
            };
        };

        // Once nothing can send, the bell stays rung so no receive
        // waits for what can't come
        if state.queue.is_empty() && state.senders > 0 {
            drain(&self.items_bell);
        }

        if was_full {
            ring(&self.room_ring);
        }

        Ok(value)
    }

    /// Counts another sending end
    pub(crate) fn add_sender(&self) {
        self.lock().senders += 1;
    }

    /// Counts a sending end gone, waking every receive once none
    /// are left
    pub(crate) fn drop_sender(&self) {
        let mut state = self.lock();

        state.senders -= 1;

        if state.senders == 0 && state.queue.is_empty() {
            ring(&self.items_ring);
        }
    }

    /// Counts another receiving end
    pub(crate) fn add_receiver(&self) {
        self.lock().receivers += 1;
    }

    /// Counts a receiving end gone, waking every send once none are
    /// left
    pub(crate) fn drop_receiver(&self) {
        let mut state = self.lock();

        state.receivers -= 1;

        if state.receivers == 0 && state.capacity == Some(state.queue.len()) {
            ring(&self.room_ring);
        }
    }

    /// Values waiting right now
    pub(crate) fn len(&self) -> usize {
        self.lock().queue.len()
    }
}

/// Leaves a byte for a bell to read
fn ring(fd: &Fd) {
    let byte = 1u8;

    let _ = unsafe { libc::write(fd.raw(), (&byte as *const u8).cast(), 1) };
}

/// Takes every byte a bell holds
fn drain(fd: &Fd) {
    let mut bytes = [0u8; 16];

    while unsafe { libc::read(fd.raw(), bytes.as_mut_ptr().cast(), bytes.len()) } > 0 {}
}
