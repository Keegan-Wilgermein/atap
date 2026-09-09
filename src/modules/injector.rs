//! # Injector
//! The queue every spawned task lands in, and the one every
//! worker falls back to when it has nothing of its own left
//!
//! There is no ceiling on it. `spawn` promises not to block
//! the calling thread, so a task that arrives faster than the
//! pool can drain it has to go somewhere rather than push back,
//! and the link it queues on lives inside the task's own slot.
//! An unbounded queue that allocates nothing is the only shape
//! that keeps both of those promises at once
//!
//! Each band is two stacks rather than one queue. Pushing onto
//! a stack is a single compare exchange, and a stack read back
//! to front is a queue, so reversing the pushed side when the
//! served side runs dry gives first in first out order for the
//! cost of one pass over a batch. That pass is O(n) once per
//! batch, so O(1) for each task in it, and it runs on whichever
//! thread had nothing better to do anyway

use crate::{
    constants::{INDEX_MASK, PRIORITY_BANDS, STARVE_RELIEF, TAG_SHIFT},
    executor,
};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

/// Every task waiting for a worker to pick it up
pub(crate) struct Injector {
    /// Where new tasks are pushed, newest first
    ///
    /// Untagged on purpose. Nothing ever pops one of these a
    /// node at a time, they are only taken whole by `flip`, so
    /// there is no node that can leave and come back between a
    /// pusher's load and its compare exchange. Tagging would
    /// be defending against something that can't happen
    incoming: [AtomicUsize; PRIORITY_BANDS],

    /// Reversed and ready to serve, oldest first
    ///
    /// Tagged, unlike `incoming`, because this side genuinely
    /// is popped one node at a time by every worker at once.
    /// See `TaskTable::alloc` for what the tag defends against
    ready: [AtomicUsize; PRIORITY_BANDS],

    /// Whether a reversal is under way on a band
    ///
    /// One reversal at a time per band is what lets the
    /// installed list simply be stored rather than merged into
    /// whatever a second reversal might have put there first
    flipping: [AtomicBool; PRIORITY_BANDS],

    /// Tasks pushed but not yet taken
    ///
    /// The signal the manager grows the pool on, and the
    /// number `PoolStats` reports
    len: AtomicUsize,

    /// Pops still owed to the oldest work, as a budget the
    /// manager tops up whenever it finds the queue starving
    ///
    /// A count rather than a flag, and that is the whole point.
    /// A flag stays set for as long as the backlog is deep, and
    /// serving oldest first for that whole time is priority
    /// inverted rather than priority aged. A budget spends
    /// itself and then the order goes back to what the caller
    /// asked for
    ///
    /// Counted rather than compared on age because a pop
    /// shouldn't be paying to dereference a slot it may not
    /// even take, and starvation is a millisecond scale problem
    /// being watched by a millisecond scale tick
    relief: AtomicU32,
}

impl Injector {
    /// An empty injector
    ///
    /// A `const fn` so the pool holding it can be a plain
    /// static with no lazy initialisation on every access
    pub(crate) const fn new() -> Self {
        Self {
            incoming: [const { AtomicUsize::new(0) }; PRIORITY_BANDS],
            ready: [const { AtomicUsize::new(0) }; PRIORITY_BANDS],
            flipping: [const { AtomicBool::new(false) }; PRIORITY_BANDS],
            len: AtomicUsize::new(0),
            relief: AtomicU32::new(0),
        }
    }

    /// Queues a task in the band its class picks
    pub(crate) fn push(&self, id: usize) {
        let Some(data) = executor::slot(id) else {
            return;
        };

        let band = data.band().min(PRIORITY_BANDS - 1);
        let index = id + 1;

        // Counted before it is published rather than after, and
        // the order is the whole point. The instant the swing
        // below lands, another thread can flip this task onto
        // the served side, take it, and subtract for it — so an
        // add left until afterwards can arrive second and leave
        // the count at nought minus one, which reads as a queue
        // of eighteen quintillion tasks that don't exist
        //
        // Early is the safe direction for the park handshake
        // too. It can only bring the moment `is_empty` starts
        // saying no forward, and a worker that doesn't park
        // when it could have costs a lap of the queue, where
        // one that parks on a queue with work in it costs
        // however long it takes somebody to notice
        //
        // Sequentially consistent because a worker about to
        // park reads this after publishing that it is parking,
        // and this is read against that publication. See
        // `Worker::park`
        self.len.fetch_add(1, Ordering::SeqCst);

        loop {
            let head = self.incoming[band].load(Ordering::Acquire);
            data.set_queue_next(head);

            if self.incoming[band]
                .compare_exchange_weak(head, index, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Takes the task that should be served next
    ///
    /// ## Behaviour
    /// Bands are served highest first, so a class only ever
    /// waits behind its own or better. The one exception is a
    /// starving queue, where the order is turned upside down
    /// for as long as the manager says it is starving, which
    /// drains the oldest work at full speed rather than one
    /// task per tick
    pub(crate) fn pop(&self) -> Option<usize> {
        if self.relieving() {
            for band in 0..PRIORITY_BANDS {
                if let Some(id) = self.take(band) {
                    return Some(id);
                }
            }

            return None;
        }

        for band in (0..PRIORITY_BANDS).rev() {
            if let Some(id) = self.take(band) {
                return Some(id);
            }
        }

        None
    }

    /// Tasks queued and not yet taken
    ///
    /// ## Behaviour
    /// Approximate, and approximate in one direction only. A
    /// push counts the task before it publishes it and a pop
    /// subtracts after it has taken it, so every window either
    /// side of a real change reads high — never low, and never
    /// through nought into the top of the range
    ///
    /// That asymmetry is deliberate rather than incidental.
    /// Everything reading this treats a queue as emptier than
    /// it is as the expensive mistake: a worker parks on work
    /// that was already there, and waits for somebody to notice
    #[inline(always)]
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Whether anything is waiting at all
    ///
    /// Sequentially consistent, unlike `len`, because this is
    /// the read a worker makes on its way into a park and it
    /// is ordered against the push that would make parking the
    /// wrong thing to do
    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.len.load(Ordering::SeqCst) == 0
    }

    /// Says whether the oldest queued task is starving
    ///
    /// Grants a fresh budget of oldest first pops when it is,
    /// and takes any left over away when it isn't
    #[inline(always)]
    pub(crate) fn set_starving(&self, starving: bool) {
        let budget = match starving {
            true => STARVE_RELIEF,
            false => 0,
        };

        self.relief.store(budget, Ordering::Relaxed);
    }

    /// Spends one pop of the starvation budget, if there is any
    ///
    /// The exchange is what keeps two workers from spending the
    /// same unit, and it only ever loops while relief is
    /// actually armed
    #[inline(always)]
    fn relieving(&self) -> bool {
        self.relief
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| match left {
                0 => None,
                _ => Some(left - 1),
            })
            .is_ok()
    }

    /// The oldest task waiting in the lowest occupied band
    ///
    /// What the manager measures an age against. Reads the
    /// served side only, so it wants calling after a flip
    pub(crate) fn oldest(&self) -> Option<(usize, usize)> {
        for band in 0..PRIORITY_BANDS {
            let index = self.ready[band].load(Ordering::Acquire) & INDEX_MASK;

            if index != 0 {
                return Some((band, index - 1));
            }
        }

        None
    }

    /// Moves the oldest task in a band up into the one above
    ///
    /// ## Behaviour
    /// The task keeps the class it was spawned at, because the
    /// caller asked for that class and aging is a decision
    /// about where to put a task rather than about what it is
    ///
    /// #### Note
    /// It lands on the pushed side of the band above, so it is
    /// served after that band's current batch rather than
    /// ahead of it. Still far sooner than it would have been,
    /// which is the whole point, and it costs no walk to the
    /// far end of a list to arrange
    pub(crate) fn promote(&self, band: usize) {
        if band + 1 >= PRIORITY_BANDS {
            return;
        }

        let Some(id) = self.pop_ready(band) else {
            return;
        };

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

    /// Reverses every band's pushed side onto its served side
    ///
    /// Pops do this lazily as they need it, so this exists for
    /// the manager rather than for the pool: measuring the age
    /// of the oldest queued task means reading the served
    /// side, and a task that hasn't been reversed yet isn't on
    /// it to be read
    pub(crate) fn refill(&self) {
        for band in 0..PRIORITY_BANDS {
            self.flip(band);
        }
    }

    /// Empties every band into a list of ids
    ///
    /// Only used when the pool is being torn down, where the
    /// point is to account for what was queued rather than to
    /// serve it
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
    /// At most one reversal per call, so a band that another
    /// thread is already reversing is left for the next look
    /// rather than spun on. Nothing is lost by moving along:
    /// the work is still queued and the next pop finds it
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
    /// The tag is bumped on the way out, so a thread that read
    /// this head and stalled long enough for the task to be
    /// taken, run, freed and queued again can't mistake the
    /// head it comes back to for the one it left
    fn pop_ready(&self, band: usize) -> Option<usize> {
        loop {
            let head = self.ready[band].load(Ordering::Acquire);
            let index = head & INDEX_MASK;

            if index == 0 {
                return None;
            }

            let id = index - 1;
            let next = executor::slot(id)?.queue_next();

            let tag = (head >> TAG_SHIFT).wrapping_add(1);
            let new = (tag << TAG_SHIFT) | next;

            if self.ready[band]
                .compare_exchange_weak(head, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(id);
            }
        }
    }

    /// Turns a band's pushed side into its served side
    ///
    /// ## Returns
    /// Whether it is worth looking at the served side again.
    /// `false` means the band is genuinely empty
    ///
    /// ## Behaviour
    /// The lock is taken before the served side is checked,
    /// which is the ordering that makes the install a plain
    /// store rather than a merge. Only a flip can put anything
    /// on the served side, and only one flip runs at a time,
    /// so a served side found empty under the lock stays empty
    /// until this one fills it
    fn flip(&self, band: usize) -> bool {
        if self.flipping[band].swap(true, Ordering::AcqRel) {
            // Somebody else is part way through. Whatever they
            // install will be there for the caller's re-check
            return true;
        }

        let head = self.ready[band].load(Ordering::Acquire);

        // Filled while this was reaching for the lock, so
        // there is nothing to do but let the caller take it
        if head & INDEX_MASK != 0 {
            self.flipping[band].store(false, Ordering::Release);
            return true;
        }

        let mut cursor = self.incoming[band].swap(0, Ordering::AcqRel);

        if cursor == 0 {
            self.flipping[band].store(false, Ordering::Release);
            return false;
        }

        // Newest first going in, oldest first coming out, which
        // is the order the sequence numbers already imply
        let mut reversed = 0;

        while cursor != 0 {
            // The link to the rest of the chain lives in the
            // slot, so a slot that can't be read takes the
            // whole tail behind it — there is no way to reach
            // past a node you can't look inside
            //
            // Written down rather than handled, because it
            // can't happen. `slot` refuses an id only past
            // `MAX_TASK_ID` or one whose block was never
            // mapped, and nothing queued here is either:
            // allocation maps the block before it hands the id
            // out, blocks are never unmapped, and the sentinel
            // id a failed spawn carries is never queued at all.
            // `trim` doesn't reach it either — it gives pages
            // back with `madvise` and every address stays valid
            let Some(data) = executor::slot(cursor - 1) else {
                break;
            };

            let next = data.queue_next();
            data.set_queue_next(reversed);

            reversed = cursor;
            cursor = next;
        }

        // Bumped even though this is a store, so that a popper
        // holding a stale head can't complete against it
        let tag = (head >> TAG_SHIFT).wrapping_add(1);
        self.ready[band].store((tag << TAG_SHIFT) | reversed, Ordering::Release);

        self.flipping[band].store(false, Ordering::Release);

        reversed != 0
    }
}
