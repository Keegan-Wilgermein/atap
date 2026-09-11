//! # KQueue
//! A kqueue per thread, reused by every task that waits on one

use crate::{
    RuntimeError, executor,
    constants::WAKE_IDENT,
    modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
    },
};
use std::{cell::Cell, time::Duration};

/// How a wait for a particular event ended
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Waited {
    /// The event that was asked for landed
    Arrived,

    /// Somebody cancelled the task while it waited
    Cancelled,

    /// The queue itself failed, so nothing is ever arriving
    Failed,
}

/// Owns a thread's kqueue, closing it when the thread ends
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
    static QUEUE: KQueue = const { KQueue(Cell::new(-1)) };
}

/// This thread's kqueue, creating it on first use
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

/// Blocks on a queue until anything lands on it, or `timeout`
/// passes
///
/// Callers re-read what they were waiting on afterwards, so any
/// wake will do
pub(crate) fn wait_any(queue: i32, timeout: Duration) {
    let mut events = eventlist();

    loop {
        match unsafe { KEvent::listen_for(queue, &mut events, timeout) }.check() {
            Ok(_) => return,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => return,
        }
    }
}

/// Blocks on a queue until a particular event lands on it
///
/// ## Returns
/// `Failed` if the queue itself fails
///
/// #### Note
/// A queue outlives every task that runs on its thread, so a
/// `WAKE_IDENT` can be left over from an earlier task's cancel.
/// It only counts as `Cancelled` if the current task is
pub(crate) fn wait_for(queue: i32, ident: usize, filter: i16) -> Waited {
    let mut events = eventlist();

    loop {
        let count = match unsafe { KEvent::listen(queue, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => return Waited::Failed,
        };

        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
                continue;
            }

            if event.ident == ident && event.filter == filter {
                return Waited::Arrived;
            }

            // Possibly left over from an earlier task, so the task's own
            // state decides
            if event.ident == WAKE_IDENT && event.filter == libc::EVFILT_USER {
                if executor::cancelled() {
                    return Waited::Cancelled;
                }

                continue;
            }
        }
    }
}
