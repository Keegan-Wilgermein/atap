//! # KQueue
//! The per thread kqueue that sleeps register their timers on
//!
//! Creating and closing a kqueue for every sleep costs two
//! syscalls, and the close lands after the spin has already
//! hit its deadline so the caller pays for it in full. One
//! queue per thread costs those syscalls once instead
//!
//! Only use this for `Sleep` calls, the microseconds
//! don't matter for other tasks so they use the global
//! kqueue

use crate::{
    RuntimeError,
    constants::WAKE_IDENT,
    modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
    },
};
use std::{cell::Cell, time::Duration};

/// Owns a thread's kqueue descriptor
///
/// Wrapped rather than held as a bare `Cell` so the descriptor
/// is closed when the thread ends instead of leaking for the
/// lifetime of the process
struct KQueue(Cell<i32>);

impl Drop for KQueue {
    fn drop(&mut self) {
        let id = self.0.get();

        if id >= 0 {
            unsafe { libc::close(id) };
        }
    }
}

thread_local! {
    /// This thread's kqueue, or -1 before it has been created
    ///
    /// Thread local because a kqueue is only safe to wait on
    /// from one thread at a time. Giving every thread its own
    /// means no sharing and no synchronisation
    static QUEUE: KQueue = const { KQueue(Cell::new(-1)) };
}

/// This thread's kqueue, creating it on first use
///
/// After the first call this is a thread local read and a
/// branch, so it stays out of the way on the hot path
#[inline(always)]
pub(crate) fn id() -> Result<i32, RuntimeError> {
    QUEUE.with(|queue| {
        let existing = queue.0.get();

        if existing >= 0 {
            return Ok(existing);
        }

        let created = unsafe { libc::kqueue() }.check()?;
        queue.0.set(created);

        Ok(created)
    })
}

/// Blocks on a queue until anything lands on it, or the time
/// runs out
///
/// ## Behaviour
/// Unlike `wait_for`, nothing here is looking for a particular
/// event. Every caller of this re-reads what it was waiting on
/// afterwards, so one wake is as good as another and a wake
/// that turns out to be somebody else's costs a second look
///
/// The ceiling is what makes that safe. A notification that
/// never arrives — because the task settled before the caller
/// registered, or because somebody else was already registered
/// on it — turns into a wait of at most `timeout` rather than a
/// wait of forever
///
/// #### Note
/// `EINTR` goes round again with the full timeout rather than
/// the remainder. This is a backstop on a path that has a real
/// notification for the ordinary case, so the arithmetic to be
/// exact about it would buy nothing
pub(crate) fn wait_any(queue: i32, timeout: Duration) {
    let mut events = eventlist();

    loop {
        match unsafe { KEvent::listen_for(queue, &mut events, timeout) }.check() {
            // Something landed, or the time ran out. Either way
            // the caller wants to look again
            Ok(_) => return,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => return,
        }
    }
}

/// Blocks on a queue until a particular event lands on it
///
/// ## Behaviour
/// A `kevent` call comes back for whatever arrives on the
/// queue, not only for the thing being waited on, so anything
/// else sends the caller round again rather than being taken
/// for an answer it isn't
///
/// Returns early if the queue itself fails, since a queue that
/// can't be listened on is never going to deliver anything and
/// waiting on it forever helps nobody
pub(crate) fn wait_for(queue: i32, ident: usize, filter: i16) {
    let mut events = eventlist();

    loop {
        let count = match unsafe { KEvent::listen(queue, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => return,
        };

        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
                continue;
            }

            if event.ident == ident && event.filter == filter {
                return;
            }

            // Somebody wants this thread back before the thing
            // it asked for arrived, which is what a cancelled
            // sleep looks like from in here. The caller is told
            // apart from a real answer by what it finds in the
            // slot afterwards
            if event.ident == WAKE_IDENT && event.filter == libc::EVFILT_USER {
                return;
            }
        }
    }
}
