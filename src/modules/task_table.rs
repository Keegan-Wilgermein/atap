//! # Task Table
//! Every live task in the process, addressed by id
//!
//! The table is a set of blocks, each twice the size of the
//! one before it. A block is mapped the first time an id
//! lands in it and is never given back, so a slot's address
//! is fixed for the life of the process and finding one is
//! arithmetic rather than a walk
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
    constants::{
        FIRST_BLOCK, FIRST_BLOCK_LOG2, FREE_INDEX_MASK, FREE_TAG_SHIFT, MAX_TASK_ID, TABLE_BLOCKS,
    },
    modules::{mapping, task_slot::TaskSlot},
};
use std::{
    mem, ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

/// Every task in the process, by id
pub(crate) struct TaskTable {
    /// The blocks, mapped as they are first needed
    blocks: [AtomicPtr<TaskSlot>; TABLE_BLOCKS],

    /// The highest id ever handed out
    ///
    /// Only ever climbs, and only when the free list has
    /// nothing left to reuse
    next_id: AtomicUsize,

    /// The head of the free list
    ///
    /// Packed as `tag << FREE_TAG_SHIFT | index + 1`, with a
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
        return Self {
            blocks: [const { AtomicPtr::new(ptr::null_mut()) }; TABLE_BLOCKS],
            next_id: AtomicUsize::new(0),
            free: AtomicUsize::new(0),
        };
    }

    /// The slot for an id, if its block has been mapped
    #[inline(always)]
    pub(crate) fn slot(&self, id: usize) -> Option<&'static TaskSlot> {
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
        return Some(unsafe { &*base.add(offset) });
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
            let index = head & FREE_INDEX_MASK;

            if index == 0 {
                break;
            }

            let id = index - 1;
            let slot = self.slot(id)?;

            // Safe to read because a retired slot is only
            // written by the thread that retired it, which
            // finished before it published the head above
            let next = slot.next();

            let tag = (head >> FREE_TAG_SHIFT).wrapping_add(1);
            let new = (tag << FREE_TAG_SHIFT) | next;

            if self
                .free
                .compare_exchange_weak(head, new, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                return Some(id);
            }
        }

        // Nothing to reuse, so the table grows by one
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.block_for(id)?;

        return Some(id);
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

        let index = id + 1;

        loop {
            let head = self.free.load(Ordering::Acquire);
            slot.set_next(head & FREE_INDEX_MASK);

            let tag = head >> FREE_TAG_SHIFT;
            let new = (tag << FREE_TAG_SHIFT) | index;

            if self
                .free
                .compare_exchange_weak(head, new, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return;
            }
        }
    }

    /// The highest id the table has ever handed out
    ///
    /// The `Executor` walks this far when it is recovering,
    /// which is the only time anything needs to look at every
    /// task at once
    #[inline(always)]
    pub(crate) fn high_water(&self) -> usize {
        return self.next_id.load(Ordering::Acquire);
    }

    /// The block holding an id, mapping it on first use
    fn block_for(&self, id: usize) -> Option<*mut TaskSlot> {
        if id >= MAX_TASK_ID {
            return None;
        }

        let (block, _) = position(id);

        let existing = self.blocks[block].load(Ordering::Acquire);

        if !existing.is_null() {
            return Some(existing);
        }

        let len = (FIRST_BLOCK << block) * mem::size_of::<TaskSlot>();
        let fresh = mapping::alloc(len).cast::<TaskSlot>();

        if fresh.is_null() {
            return None;
        }

        // The mapping comes back zeroed, and a zeroed slot is
        // already a valid retired one, so there is nothing to
        // write before it can be published
        return match self.blocks[block].compare_exchange(
            ptr::null_mut(),
            fresh,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => Some(fresh),
            Err(won) => {
                // Another thread mapped this block first, so
                // this one goes back rather than leaking
                mapping::free(fresh.cast::<u8>(), len);
                Some(won)
            }
        };
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

    return (block, shifted - (FIRST_BLOCK << block));
}
