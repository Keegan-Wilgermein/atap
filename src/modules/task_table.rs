//! # Task Table
//! Every live task in the process, addressed by id
//!
//! Blocks double in size and are never unmapped, so a slot's
//! address never moves. Ids are reused from a free list before
//! the table is allowed to grow. There are no locks

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
    blocks: [AtomicPtr<u8>; TABLE_BLOCKS],

    /// One past the highest id the table currently spans
    ///
    /// A trim lowers it, so on its own it isn't the peak
    next_id: AtomicUsize,

    /// The highest `next_id` had been when a trim last lowered it
    ///
    /// `next_id` only climbs between trims, so the larger of the
    /// two is the exact peak, with no work on the spawn path
    peak: AtomicUsize,

    /// Slots handed out and not yet given back
    live: AtomicUsize,

    /// Whether a trim is already under way, since two would fight
    /// over the free list
    trimming: AtomicBool,

    /// The head of the free list
    ///
    /// Packed as `tag << TAG_SHIFT | index + 1`, zero meaning empty
    free: AtomicUsize,
}

impl TaskTable {
    /// An empty table
    pub(crate) const fn new() -> Self {
        Self {
            blocks: [const { AtomicPtr::new(ptr::null_mut()) }; TABLE_BLOCKS],
            next_id: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            live: AtomicUsize::new(0),
            trimming: AtomicBool::new(false),
            free: AtomicUsize::new(0),
        }
    }

    /// The slot for an id, if its block has been mapped
    ///
    /// Says nothing about whether a task is in it
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

        // Blocks are never unmapped, so this is good for the life of
        // the process
        Some(unsafe { &*base.add(offset * SLOT_SIZE).cast::<TaskData>() })
    }

    /// Takes an id, reusing a retired one if there is one
    ///
    /// ## Returns
    /// `None` only if the kernel refuses a block
    ///
    /// #### Note
    /// The tag in the head is bumped on every pop, so a thread that
    /// stalled while this id was popped, used and pushed again
    /// can't swing the head onto a live id
    pub(crate) fn alloc(&self) -> Option<usize> {
        loop {
            let head = self.free.load(Ordering::Acquire);
            let index = head & INDEX_MASK;

            if index == 0 {
                break;
            }

            let id = index - 1;
            let slot = self.slot(id)?;

            // Safe to read: only the thread that retired this slot wrote
            // it, and it did so before publishing the head
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

        // Counted on both paths, since both are given back through
        // `free`
        self.live.fetch_add(1, Ordering::Relaxed);

        Some(id)
    }

    /// Hands an id back to be used again
    ///
    /// Only once the task's memory has been freed, since the id is
    /// live again the moment it lands on the list
    pub(crate) fn free(&self, id: usize) {
        let Some(slot) = self.slot(id) else {
            return;
        };

        self.live.fetch_sub(1, Ordering::Relaxed);
        self.push_free(id, slot);
    }

    /// Puts an id on the free list without touching the count
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

    /// One past the highest id the table currently spans
    ///
    /// Walking up to here sees every live task, since a trim only
    /// lowers it past free ids
    #[inline(always)]
    pub(crate) fn high_water(&self) -> usize {
        self.next_id.load(Ordering::Acquire)
    }

    /// The most slots the table has ever spanned at once
    ///
    /// #### Note
    /// `next_id` is read before `peak`. A trim writes `peak` before
    /// lowering `next_id`, so a read that sees the lowered value
    /// also sees the record
    #[inline(always)]
    pub(crate) fn peak(&self) -> usize {
        let now = self.next_id.load(Ordering::Acquire);

        now.max(self.peak.load(Ordering::Acquire))
    }

    /// Gives back the pages behind the top of the table
    ///
    /// ## Returns
    /// Bytes handed back to the kernel, or `StillInUse` when the
    /// table is too close to what is live in it
    ///
    /// ## Behaviour
    /// Pages are released with `madvise`, not unmapped, so every
    /// slot address stays valid. Only whole pages whose every slot
    /// was free go back
    ///
    /// #### Note
    /// The pages go back before the high water mark comes down.
    /// The other order would let a new task be written into a page
    /// as it was being released
    pub(crate) fn trim(&self) -> Result<usize, RuntimeError> {
        // Held for the whole walk, so a second trim turns straight
        // round
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

        // Only slots taken off the free list are candidates, so a live
        // slot's page is never picked
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

        // Recorded before `next_id` comes down, so a reader never sees
        // the lowered value without the peak. Harmless if the exchange
        // below fails
        self.peak.fetch_max(current, Ordering::Release);

        // Fails if somebody grew the table meanwhile, and then
        // everything goes back
        if self
            .next_id
            .compare_exchange(current, keep, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            self.restore(&taken, current);
            return Err(RuntimeError::StillInUse);
        }

        // Ids above the new mark come back as `next_id` climbs again,
        // which leaves their pages given back
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

    /// Takes the free list, putting back straight away every id
    /// below the floor
    ///
    /// Keeps the window where spawns find no free id, and grow the
    /// table instead, as short as it can. The tag is bumped so a
    /// thread part way through a pop fails its exchange
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

            // Read first, since putting this slot back overwrites the link
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

            // Rounded inward, so only whole pages go back
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

        // A zeroed slot is already a valid retired one
        match self.blocks[block].compare_exchange(
            ptr::null_mut(),
            fresh,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Some(fresh),
            Err(won) => {
                // Another thread mapped it first
                mapping::free(fresh, len);
                Some(won)
            }
        }
    }
}

/// Splits an id into the block holding it and its place in it
///
/// Block `b` holds `FIRST_BLOCK << b` slots, so the block comes
/// from the id's highest set bit. Only valid below
/// `MAX_TASK_ID`
#[inline(always)]
fn position(id: usize) -> (usize, usize) {
    let shifted = id + FIRST_BLOCK;
    let highest = (usize::BITS - 1 - shifted.leading_zeros()) as usize;
    let block = highest - FIRST_BLOCK_LOG2 as usize;

    (block, shifted - (FIRST_BLOCK << block))
}
