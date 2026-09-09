//! # Task Table
//! Every live task in the process, addressed by id
//!
//! The table is a set of blocks, each twice the size of the
//! one before it. A block is mapped the first time an id
//! lands in it and is never given back, so a slot's address
//! is fixed for the life of the process and finding one is
//! arithmetic rather than a walk
//!
//! The blocks hold the slots themselves rather than pointers
//! to them, so a task's memory is found by multiplying its id
//! out rather than by chasing anything. That also means an id
//! and the memory behind it are recycled by the same act, and
//! there is one free list rather than two
//!
//! Ids are handed out from a free list before the table is
//! allowed to grow, so the table settles at the peak number
//! of tasks alive at once rather than the number ever spawned
//!
//! There isn't a lock anywhere in here. Growing the table and
//! taking an id are both a single compare exchange, and both
//! are safe to lose, so a thread that loses a race simply
//! looks again

use crate::{
    RuntimeError,
    constants::{
        FIRST_BLOCK, FIRST_BLOCK_LOG2, INDEX_MASK, MAX_TASK_ID, SLOT_SIZE, TABLE_BLOCKS, TAG_SHIFT,
        TRIM_KEEP_PERCENT, TRIM_MINIMUM, TRIM_THRESHOLD,
    },
    modules::{mapping, task_data::TaskData},
};
use std::{
    ptr,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
};

/// Every task in the process, by id
pub(crate) struct TaskTable {
    /// The blocks, mapped as they are first needed
    ///
    /// Held as bytes because slots sit `SLOT_SIZE` apart
    /// rather than end to end, so the arithmetic is done in
    /// bytes and cast at the last moment
    blocks: [AtomicPtr<u8>; TABLE_BLOCKS],

    /// The highest id ever handed out
    ///
    /// Only ever climbs, and only when the free list has
    /// nothing left to reuse
    next_id: AtomicUsize,

    /// Slots handed out and not yet given back
    ///
    /// Counted rather than worked out, because trimming needs
    /// to know how much of the table is genuinely in use and
    /// walking every slot to find out would cost more than the
    /// counting does
    live: AtomicUsize,

    /// Whether a trim is already under way
    ///
    /// One at a time, because two would fight over the same
    /// free list: the first takes it whole to walk it, and the
    /// second finds nothing there and concludes the table is
    /// too busy to touch. Waiting costs nothing, since a trim
    /// is a tidy up and nothing is waiting on the answer
    trimming: AtomicBool,

    /// The head of the free list
    ///
    /// Packed as `tag << TAG_SHIFT | index + 1`, with a
    /// whole word of zero meaning the list is empty. See
    /// `alloc` for what the tag is for
    free: AtomicUsize,
}

impl TaskTable {
    /// An empty table
    ///
    /// A `const fn` so the table can be a plain static with
    /// no lazy initialisation guarding every single access
    pub(crate) const fn new() -> Self {
        Self {
            blocks: [const { AtomicPtr::new(ptr::null_mut()) }; TABLE_BLOCKS],
            next_id: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            trimming: AtomicBool::new(false),
            free: AtomicUsize::new(0),
        }
    }

    /// The slot for an id, if its block has been mapped
    ///
    /// #### Note
    /// Says nothing about whether there is a task in it. A
    /// slot that has never been used reads as `Free`, which
    /// is what the `Executor` filters on
    #[inline(always)]
    pub(crate) fn slot(&self, id: usize) -> Option<&'static TaskData> {
        if id >= MAX_TASK_ID {
            return None;
        }

        let (block, offset) = position(id);

        let base = self.blocks[block].load(Ordering::Acquire);

        if base.is_null() {
            return None;
        }

        // Blocks are never unmapped, so a slot borrowed out
        // of one is good for as long as the process is
        Some(unsafe { &*base.add(offset * SLOT_SIZE).cast::<TaskData>() })
    }

    /// Takes an id, reusing a retired one if there is one
    ///
    /// ## Returns
    /// `None` only if the kernel refuses a block, or if the
    /// table has somehow run past `TABLE_BLOCKS`
    ///
    /// #### Note
    /// The tag in the head is the whole reason this is safe.
    /// Without it, a thread that reads the head and the id
    /// behind it, then stalls long enough for that id to be
    /// popped, used, freed and pushed again, would find the
    /// head unchanged and swing it to an id that is now live.
    /// Bumping the tag on every pop means the head it left
    /// behind can never be mistaken for the head it comes
    /// back to
    pub(crate) fn alloc(&self) -> Option<usize> {
        loop {
            let head = self.free.load(Ordering::Acquire);
            let index = head & INDEX_MASK;

            if index == 0 {
                break;
            }

            let id = index - 1;
            let slot = self.slot(id)?;

            // Safe to read because a retired slot is only
            // written by the thread that retired it, which
            // finished before it published the head above
            let next = slot.next();

            let tag = (head >> TAG_SHIFT).wrapping_add(1);
            let new = (tag << TAG_SHIFT) | next;

            if self
                .free
                .compare_exchange_weak(head, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.live.fetch_add(1, Ordering::Relaxed);
                return Some(id);
            }
        }

        // Nothing to reuse, so the table grows by one
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.block_for(id)?;

        // Counted here as well as on the reuse path above. Both
        // hand out an id and both are given back through `free`,
        // so a count kept on only one of them runs away
        self.live.fetch_add(1, Ordering::Relaxed);

        Some(id)
    }

    /// Hands an id back to be used again
    ///
    /// Only the thread that freed the task's memory may call
    /// this, and only once it has, since the id is live again
    /// the moment it lands on the list
    ///
    /// #### Note
    /// Pushing doesn't need to bump the tag. The slot going
    /// on the list can't be on it already, so no other thread
    /// can be reading its link, and the compare exchange is
    /// enough on its own to prove the head hasn't moved
    pub(crate) fn free(&self, id: usize) {
        let Some(slot) = self.slot(id) else {
            return;
        };

        self.live.fetch_sub(1, Ordering::Relaxed);
        self.push_free(id, slot);
    }

    /// Puts an id on the free list without touching the count
    ///
    /// Kept apart from `free` so a trim can put back what it
    /// took without the ids being counted as having become free
    /// twice over
    fn push_free(&self, id: usize, slot: &TaskData) {
        let index = id + 1;

        loop {
            let head = self.free.load(Ordering::Acquire);
            slot.set_next(head & INDEX_MASK);

            let tag = head >> TAG_SHIFT;
            let new = (tag << TAG_SHIFT) | index;

            if self
                .free
                .compare_exchange_weak(head, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// Slots handed out and not yet given back
    #[inline(always)]
    pub(crate) fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// The highest id the table has ever handed out
    ///
    /// The `Executor` walks this far when it is recovering,
    /// which is the only time anything needs to look at every
    /// task at once
    #[inline(always)]
    pub(crate) fn high_water(&self) -> usize {
        self.next_id.load(Ordering::Acquire)
    }

    /// Gives back the pages behind the top of the table
    ///
    /// ## Returns
    /// Bytes handed back to the kernel, or `StillInUse` when
    /// the table is too close to what is live in it for any of
    /// it to be worth taking
    ///
    /// ## Behaviour
    /// Pages are released with `madvise` rather than unmapped.
    /// A slot's address is what its listeners block on, so an
    /// address that could be taken away isn't one anything
    /// could safely hold. The mapping stays, the physical pages
    /// go, and a reclaimed slot reads as zeros, which is
    /// already what an empty slot reads as
    ///
    /// Only whole pages, and only ones where every slot in them
    /// was on the free list. A page holds many slots and one
    /// live task in it is enough to keep the lot
    ///
    /// #### Note
    /// The order here is the part that has to be right. The
    /// free list is emptied first, so nothing can be allocated
    /// out of the range while it is being worked on. The pages
    /// are released *before* the high water mark comes down,
    /// because lowering it first would let a fresh task be
    /// allocated into the range and written, and then have its
    /// page released out from under it. Doing it the other way
    /// round means any task allocated afterwards writes to the
    /// page and cancels the reclaim, which is exactly what
    /// `MADV_FREE` promises
    pub(crate) fn trim(&self) -> Result<usize, RuntimeError> {
        // Held for the whole walk, so a second caller turns
        // straight round rather than emptying the list out from
        // under the first one and then reporting that the table
        // is busy — which it would be, with itself
        if self.trimming.swap(true, Ordering::AcqRel) {
            return Err(RuntimeError::StillInUse);
        }

        let given = self.reclaim();

        self.trimming.store(false, Ordering::Release);

        given
    }

    /// The trim itself, once it is known to be the only one
    fn reclaim(&self) -> Result<usize, RuntimeError> {
        let current = self.next_id.load(Ordering::Acquire);
        let live = self.live.load(Ordering::Acquire);

        let floor = (live + TRIM_THRESHOLD)
            .max(current / 100 * TRIM_KEEP_PERCENT)
            .max(TRIM_MINIMUM);

        if floor >= current {
            return Err(RuntimeError::StillInUse);
        }

        let taken = self.drain_free(floor);

        if taken.is_empty() {
            return Err(RuntimeError::StillInUse);
        }

        // A slot is only safe to give back if it was on the
        // free list, which is now entirely in hand. Anything
        // handed out and not yet given back isn't in here, so
        // its page is never picked
        let mut held = vec![0u64; current.div_ceil(u64::BITS as usize)];

        for id in taken.iter() {
            if *id < current {
                held[id / u64::BITS as usize] |= 1 << (id % u64::BITS as usize);
            }
        }

        let mut keep = current;

        while keep > floor {
            let id = keep - 1;

            if held[id / u64::BITS as usize] & (1 << (id % u64::BITS as usize)) == 0 {
                break;
            }

            keep -= 1;
        }

        if keep >= current {
            self.restore(&taken, current);
            return Err(RuntimeError::StillInUse);
        }

        let released = self.release_pages(keep, current);

        // Nothing has been handed out since the snapshot, so
        // nothing is living in the range that was just given
        // back. A failure here means somebody grew the table
        // while this was working, and everything goes back
        if self
            .next_id
            .compare_exchange(current, keep, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.restore(&taken, current);
            return Err(RuntimeError::StillInUse);
        }

        // Everything below the new mark goes back on the list.
        // Everything above it is reached by the high water mark
        // climbing again, which costs no write and so leaves the
        // pages given back
        self.restore(&taken, keep);

        Ok(released)
    }

    /// Puts back every id below `limit`
    fn restore(&self, taken: &[usize], limit: usize) {
        for id in taken.iter() {
            if *id >= limit {
                continue;
            }

            let Some(slot) = self.slot(*id) else {
                continue;
            };

            self.push_free(*id, slot);
        }
    }

    /// Takes the free list, keeping only what is worth keeping
    ///
    /// ## Behaviour
    /// Anything below the floor can't be part of what gets
    /// given back, so it goes straight back on the list as the
    /// walk passes it rather than being held for the length of
    /// it. That matters: while the list is empty every spawn
    /// has to grow the table instead of reusing an id, so a
    /// trim that held the lot would inflate the very thing it
    /// is trying to shrink
    ///
    /// The window isn't gone, only made small. The list is
    /// still empty between being taken and the first id going
    /// back, and a spawn landing exactly there still grows the
    /// table by one
    ///
    /// The tag is bumped rather than thrown away, so a thread
    /// part way through a pop still fails its exchange against
    /// the head this leaves behind
    fn drain_free(&self, floor: usize) -> Vec<usize> {
        let mut cursor = loop {
            let head = self.free.load(Ordering::Acquire);
            let index = head & INDEX_MASK;

            if index == 0 {
                return Vec::new();
            }

            let tag = (head >> TAG_SHIFT).wrapping_add(1);

            if self
                .free
                .compare_exchange_weak(
                    head,
                    tag << TAG_SHIFT,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break index;
            }
        };

        let mut taken = Vec::new();

        while cursor != 0 {
            let id = cursor - 1;

            let Some(slot) = self.slot(id) else {
                break;
            };

            // Read before anything writes to it, since putting
            // this slot back overwrites the very link being
            // followed
            cursor = slot.next();

            if id < floor {
                self.push_free(id, slot);
                continue;
            }

            taken.push(id);
        }

        taken
    }

    /// Hands back every whole page between two ids
    fn release_pages(&self, from: usize, to: usize) -> usize {
        let page = mapping::page_size();
        let per_page = page / SLOT_SIZE;

        if per_page == 0 {
            return 0;
        }

        let mut released = 0;

        for block in 0..TABLE_BLOCKS {
            let base = self.blocks[block].load(Ordering::Acquire);

            if base.is_null() {
                continue;
            }

            let slots = FIRST_BLOCK << block;
            let first = slots - FIRST_BLOCK;

            let start = from.max(first);
            let end = to.min(first + slots);

            if start >= end {
                continue;
            }

            // Rounded inward, so a page only goes back when the
            // whole of it is inside the range
            let head = (start - first).div_ceil(per_page) * per_page;
            let tail = (end - first) / per_page * per_page;

            if head >= tail {
                continue;
            }

            let len = (tail - head) * SLOT_SIZE;

            if unsafe { mapping::release(base.add(head * SLOT_SIZE), len) } {
                released += len;
            }
        }

        released
    }

    /// The block holding an id, mapping it on first use
    fn block_for(&self, id: usize) -> Option<*mut u8> {
        if id >= MAX_TASK_ID {
            return None;
        }

        let (block, _) = position(id);

        let existing = self.blocks[block].load(Ordering::Acquire);

        if !existing.is_null() {
            return Some(existing);
        }

        let len = (FIRST_BLOCK << block) * SLOT_SIZE;
        let fresh = mapping::alloc(len);

        if fresh.is_null() {
            return None;
        }

        // The mapping comes back zeroed, and a zeroed slot is
        // already a valid retired one, so there is nothing to
        // write before it can be published
        match self.blocks[block].compare_exchange(
            ptr::null_mut(),
            fresh,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Some(fresh),
            Err(won) => {
                // Another thread mapped this block first, so
                // this one goes back rather than leaking
                mapping::free(fresh, len);
                Some(won)
            }
        }
    }
}

/// Splits an id into the block holding it and its place in it
///
/// Block `b` holds `FIRST_BLOCK << b` slots, so shifting the
/// id up past the first block turns the block number into the
/// position of the id's highest set bit. That makes finding a
/// slot a few instructions rather than a walk down a list, no
/// matter how far the table has grown
///
/// Only valid below `MAX_TASK_ID`, which every caller checks
#[inline(always)]
fn position(id: usize) -> (usize, usize) {
    let shifted = id + FIRST_BLOCK;
    let highest = (usize::BITS - 1 - shifted.leading_zeros()) as usize;
    let block = highest - FIRST_BLOCK_LOG2 as usize;

    (block, shifted - (FIRST_BLOCK << block))
}
