//! # Task Slot
//! One entry in the `TaskTable`
//!
//! A slot is either live, holding the mapping for a task, or
//! retired, sitting on the free list waiting for its id to be
//! handed out again. It is never both, which is why the free
//! list link can live in the slot itself and cost nothing

use crate::modules::task_data::TaskData;
use std::{
    ptr,
    sync::atomic::{AtomicPtr, AtomicUsize, Ordering},
};

/// One id's worth of table
///
/// Blocks of these are mapped straight from the kernel, and a
/// zeroed slot is already a valid retired one, so a fresh
/// block needs nothing written to it before it goes into use
pub(crate) struct TaskSlot {
    /// The task's mapping, or null while the slot is retired
    data: AtomicPtr<TaskData>,

    /// The next retired id, stored as the index plus one so
    /// that zero can mean the end of the list
    ///
    /// Only meaningful while this slot is on the free list.
    /// Nothing can be reading it at that point, because a
    /// slot only goes on the list once its last listener
    /// has gone
    next: AtomicUsize,
}

impl TaskSlot {
    /// The task in this slot, or null if there isn't one
    #[inline(always)]
    pub(crate) fn data(&self) -> *mut TaskData {
        return self.data.load(Ordering::Acquire);
    }

    /// Publishes a task into the slot
    ///
    /// The release pairs with the acquire in `data`, so a
    /// thread that finds the pointer also sees every byte
    /// written into the mapping before it was published
    #[inline(always)]
    pub(crate) fn publish(&self, data: *mut TaskData) {
        self.data.store(data, Ordering::Release);
    }

    /// Empties the slot, so nothing can find the task again
    #[inline(always)]
    pub(crate) fn clear(&self) {
        self.data.store(ptr::null_mut(), Ordering::Release);
    }

    /// The encoded id of the next retired slot
    #[inline(always)]
    pub(crate) fn next(&self) -> usize {
        return self.next.load(Ordering::Relaxed);
    }

    /// Points this retired slot at the next one
    #[inline(always)]
    pub(crate) fn set_next(&self, next: usize) {
        self.next.store(next, Ordering::Relaxed);
    }
}
