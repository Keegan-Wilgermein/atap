//! # Injector
//! The queue every spawned task lands in, and the one workers
//! fall back to when their own is empty
//!
//! Unbounded, and allocates nothing, since the link lives in
//! each task's slot. Each band is two stacks: reversing the
//! pushed side when the served side runs dry gives first in,
//! first out order

use crate::{
    constants::{INDEX_MASK, PRIORITY_BANDS, STARVE_RELIEF, TAG_SHIFT},
    executor,
};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

/// Every task waiting for a worker to pick it up
pub(crate) struct Injector {
    /// Where new tasks are pushed, newest first
    ///
    /// Untagged, since these are only ever taken whole by `flip`
    incoming: [AtomicUsize; PRIORITY_BANDS],

    /// Reversed and ready to serve, oldest first
    ///
    /// Tagged against ABA, since every worker pops from it one
    /// node at a time
    ready: [AtomicUsize; PRIORITY_BANDS],

    /// Whether a reversal is under way on a band
    ///
    /// One at a time per band, so a reversal can store its list
    /// rather than merge it
    flipping: [AtomicBool; PRIORITY_BANDS],

    /// Tasks pushed but not yet taken
    len: AtomicUsize,

    /// Oldest first pops still owed, topped up by the manager when
    /// the queue is starving
    relief: AtomicU32,
}

impl Injector {
    /// An empty injector
    pub(crate) const fn new() -> Self {
        Self {
            incoming: [const { AtomicUsize::new(0) }; PRIORITY_BANDS],
            ready: [const { AtomicUsize::new(0) }; PRIORITY_BANDS],
            flipping: [const { AtomicBool::new(false) }; PRIORITY_BANDS],
            len: AtomicUsize::new(0),
            relief: AtomicU32::new(0),
        }
    }

    /// Queues a task in the band its priority picks
    ///
    /// ## Returns
    /// Whether it was queued. `false` means the id has no live
    /// task behind it
    pub(crate) fn push(&self, id: usize) -> bool {
        let Some(data) = executor::slot(id) else {
            return false;
        };

        let band = data.band().min(PRIORITY_BANDS - 1);
        let index = id + 1;

        // Counted before it is published, so a pop can never take the
        // count below zero. `SeqCst` because a parking worker reads it
        // against its own announcement
        self.len.fetch_add(1, Ordering::SeqCst);

        loop {
            let head = self.incoming[band].load(Ordering::Acquire);
            data.set_queue_next(head);

            if self.incoming[band]
                .compare_exchange_weak(head, index, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Takes the task that should be served next
    ///
    /// Highest band first, unless the queue is starving, when the
    /// oldest task can go first
    pub(crate) fn pop(&self) -> Option<usize> {
        self.pop_banded().map(|(id, _)| id)
    }

    /// The same pop, saying which band it came out of
    pub(crate) fn pop_banded(&self) -> Option<(usize, usize)> {
        // Relief prefers the band holding the oldest task, and falls
        // back to the normal order if that band is empty
        if self.relief.load(Ordering::Relaxed) != 0 {
            if let Some(band) = self.oldest_band() {
                if let Some(id) = self.take(band) {
                    self.spend_relief();

                    return Some((id, band));
                }
            }
        }

        for band in (0..PRIORITY_BANDS).rev() {
            if let Some(id) = self.take(band) {
                return Some((id, band));
            }
        }

        None
    }

    /// Takes the next task out of one band and no other
    #[inline(always)]
    pub(crate) fn pop_from(&self, band: usize) -> Option<usize> {
        self.take(band)
    }

    /// Tasks queued and not yet taken
    ///
    /// Approximate, and only ever high, never low
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Whether anything is waiting at all
    ///
    /// `SeqCst`, as a worker's park handshake needs
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len.load(Ordering::SeqCst) == 0
    }

    /// Says whether the oldest queued task is starving, granting or
    /// clearing a budget of oldest first pops
    #[inline(always)]
    pub(crate) fn set_starving(&self, starving: bool) {
        let budget = match starving {
            true => STARVE_RELIEF,
            false => 0,
        };

        self.relief.store(budget, Ordering::Relaxed);
    }

    /// Spends one unit of the starvation budget, once a relief pop
    /// has come back with a task
    #[inline(always)]
    fn spend_relief(&self) {
        let _ = self
            .relief
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| match left {
                0 => None,
                _ => Some(left - 1),
            });
    }

    /// The oldest task waiting in any band, and the band it is in
    ///
    /// Reads the served side only, so it wants calling after a
    /// flip
    pub(crate) fn oldest(&self) -> Option<(usize, usize)> {
        let mut oldest: Option<(usize, usize, u64)> = None;

        for band in 0..PRIORITY_BANDS {
            let index = self.ready[band].load(Ordering::Acquire) & INDEX_MASK;

            if index == 0 {
                continue;
            }

            let id = index - 1;

            let Some(data) = executor::slot(id) else {
                continue;
            };

            let stamp = data.priority_sequence();

            if oldest.is_none_or(|(_, _, best)| stamp < best) {
                oldest = Some((band, id, stamp));
            }
        }

        oldest.map(|(band, id, _)| (band, id))
    }

    /// The band holding the oldest queued task
    #[inline(always)]
    fn oldest_band(&self) -> Option<usize> {
        self.oldest().map(|(band, _)| band)
    }

    /// Moves the oldest task in a band onto the pushed side of the
    /// band above
    ///
    /// The task keeps its own priority class
    pub(crate) fn promote(&self, band: usize) {
        if band + 1 >= PRIORITY_BANDS {
            return;
        }

        let Some(id) = self.pop_ready(band) else {
            return;
        };

        // `pop_ready` only returns live tasks, and this one is only
        // changing band, so the count stays as it is
        let Some(data) = executor::slot(id) else {
            self.len.fetch_sub(1, Ordering::Relaxed);
            return;
        };

        let index = id + 1;

        loop {
            let head = self.incoming[band + 1].load(Ordering::Acquire);
            data.set_queue_next(head);

            if self.incoming[band + 1]
                .compare_exchange_weak(head, index, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Reverses every band's pushed side onto its served side, so
    /// `oldest` can see everything queued
    pub(crate) fn refill(&self) {
        for band in 0..PRIORITY_BANDS {
            self.flip(band);
        }
    }

    /// Empties every band into a list of ids, for tearing the pool
    /// down
    pub(crate) fn drain(&self) -> Vec<usize> {
        let mut drained = Vec::new();

        while let Some(id) = self.pop() {
            drained.push(id);
        }

        drained
    }

    /// Takes from one band, reversing its pushed side if the
    /// served side has run dry
    ///
    /// A band another thread is already reversing is skipped, not
    /// waited for
    fn take(&self, band: usize) -> Option<usize> {
        if let Some(id) = self.pop_ready(band) {
            self.len.fetch_sub(1, Ordering::Relaxed);
            return Some(id);
        }

        if !self.flip(band) {
            return None;
        }

        let id = self.pop_ready(band)?;
        self.len.fetch_sub(1, Ordering::Relaxed);

        Some(id)
    }

    /// Pops one task off a band's served side
    ///
    /// The tag is bumped on the way out, against ABA
    fn pop_ready(&self, band: usize) -> Option<usize> {
        loop {
            let head = self.ready[band].load(Ordering::Acquire);
            let index = head & INDEX_MASK;

            if index == 0 {
                return None;
            }

            let id = index - 1;

            // Through `queue_link`, since a retired node still has to be
            // stepped over
            let Some(next) = executor::queue_link(id) else {
                // No memory behind this id, so nothing behind it can be
                // reached. The band is emptied rather than wedged on it
                let tag = (head >> TAG_SHIFT).wrapping_add(1);

                let _ = self.ready[band].compare_exchange(
                    head,
                    tag << TAG_SHIFT,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                );

                return None;
            };

            let tag = (head >> TAG_SHIFT).wrapping_add(1);
            let new = (tag << TAG_SHIFT) | next;

            if self.ready[band]
                .compare_exchange_weak(head, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                continue;
            }

            // A retired task is counted out and stepped over, rather than
            // left at the head to wedge the band
            if executor::slot(id).is_none() {
                self.len.fetch_sub(1, Ordering::Relaxed);

                continue;
            }

            return Some(id);
        }
    }

    /// Turns a band's pushed side into its served side
    ///
    /// ## Returns
    /// Whether the served side is worth looking at again
    ///
    /// Only a flip fills the served side, and only one runs at a
    /// time, so its list is stored rather than merged
    fn flip(&self, band: usize) -> bool {
        if self.flipping[band].swap(true, Ordering::AcqRel) {
            // Somebody else is reversing it
            return true;
        }

        let head = self.ready[band].load(Ordering::Acquire);

        // Filled while this was taking the lock
        if head & INDEX_MASK != 0 {
            self.flipping[band].store(false, Ordering::Release);
            return true;
        }

        let mut cursor = self.incoming[band].swap(0, Ordering::AcqRel);

        if cursor == 0 {
            self.flipping[band].store(false, Ordering::Release);
            return false;
        }

        let mut reversed = 0;

        while cursor != 0 {
            // Can't happen: a queued id always has a mapped slot
            let Some(data) = executor::slot(cursor - 1) else {
                break;
            };

            let next = data.queue_next();
            data.set_queue_next(reversed);

            reversed = cursor;
            cursor = next;
        }

        // Tag bumped, so a popper holding a stale head can't land
        let tag = (head >> TAG_SHIFT).wrapping_add(1);
        self.ready[band].store((tag << TAG_SHIFT) | reversed, Ordering::Release);

        self.flipping[band].store(false, Ordering::Release);

        reversed != 0
    }
}
