//! Queue
//! The per thread kqueue that sleeps register their timers on
//!
//! Creating and closing a kqueue for every sleep costs two
//! syscalls, and the close lands after the spin has already
//! hit its deadline so the caller pays for it in full. One
//! queue per thread costs those syscalls once instead

use std::cell::Cell;
use crate::modules::int_check::IntCheck;

/// Owns a thread's kqueue descriptor
///
/// Wrapped rather than held as a bare `Cell` so the descriptor
/// is closed when the thread ends instead of leaking for the
/// lifetime of the process
struct Queue(Cell<i32>);

impl Drop for Queue {
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
    static QUEUE: Queue = Queue(Cell::new(-1));
}

/// This thread's kqueue, creating it on first use
///
/// After the first call this is a thread local read and a
/// branch, so it stays out of the way on the hot path
///
/// ## Panics
/// If `kqueue` can't be created
#[inline(always)]
pub(crate) fn id() -> i32 {
    return QUEUE.with(|queue| {
        let existing = queue.0.get();

        if existing >= 0 {
            return existing;
        }

        let created = unsafe { libc::kqueue() }.check();
        queue.0.set(created);

        return created;
    });
}
