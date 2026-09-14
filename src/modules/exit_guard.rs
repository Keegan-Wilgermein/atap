//! # Exit Guard
//! Marking a pool thread's slot on the way out of its loop,
//! however the thread leaves it

use crate::modules::{thread_slot::PoolThread, worker_pool::POOL};

/// Marks the slot on the way out of the loop, including when a
/// panic unwinds through it
pub(crate) struct ExitGuard<T: PoolThread> {
    /// The thread being left
    owner: &'static T,

    /// Whether the loop broke rather than unwound
    clean: bool,
}

impl<T: PoolThread> ExitGuard<T> {
    /// A guard that reads as a death until told otherwise
    #[inline(always)]
    pub(crate) fn new(owner: &'static T) -> Self {
        Self {
            owner,
            clean: false,
        }
    }

    /// Says the loop broke rather than unwound
    #[inline(always)]
    pub(crate) fn mark_clean(&mut self) {
        self.clean = true;
    }
}

impl<T: PoolThread> Drop for ExitGuard<T> {
    fn drop(&mut self) {
        if self.clean {
            self.owner.left();

            return;
        }

        // Counted before the slot reads dead, so no recovery ever finds a
        // death it wasn't told of
        POOL.note_death();

        self.owner.slot().mark_dead();

        // Nobody else may be left to notice, so it sends for help itself
        POOL.send_for_help();
    }
}
