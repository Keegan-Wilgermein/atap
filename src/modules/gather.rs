//! # Gather
//! Collecting an output from every task in a set into the one value
//! a task that receives the set is handed
//!
//! A round fills one slot per task. Once every slot is full the
//! slots are emptied into the value and handed on, and the next
//! round starts. A value that arrives for a slot already filled this
//! round replaces it

use crate::{
    executor,
    modules::{
        forward::Forward, handle_kind::Waiting, handle_set::HandleSet, task_handle::TaskHandle,
    },
};
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

/// Reaches from a whole set's slots down to part of them
pub(crate) type Access<R, S> = Arc<dyn Fn(&mut R) -> &mut S + Send + Sync>;

/// Makes an access from a closure
///
/// Taking the closure through a bound is what lets it hand back part
/// of whatever it is given, for every borrow
pub(crate) fn access<R, S, F>(pick: F) -> Access<R, S>
where
    F: Fn(&mut R) -> &mut S + Send + Sync + 'static,
    R: 'static,
    S: 'static,
{
    Arc::new(pick)
}

/// The slots of one set, and the task they are handed to
///
/// Public only so a set's link can name it. Nothing outside the
/// crate can reach it
#[doc(hidden)]
pub struct Gather<R>
where
    R: HandleSet,
{
    /// This round's values, shaped like the set
    slots: Mutex<R::Slots>,

    /// The task a full round is given to, until the set can never
    /// fill again
    target: Mutex<Option<TaskHandle<(), Waiting<R::Output>>>>,

    /// Tasks in the set that were linked
    leaves: AtomicUsize,

    /// Whether a task in the set publishes nothing more, so no round
    /// after this one can fill
    broken: AtomicBool,

    /// Rounds emptied out of the slots and not yet given
    ///
    /// Raised under the slots lock, so a task leaving the set can tell
    /// a round already on its way from one that never filled
    giving: AtomicUsize,
}

impl<R> Gather<R>
where
    R: HandleSet,
{
    /// Empty slots, handed to `target` once full
    pub(crate) fn new(slots: R::Slots, target: TaskHandle<(), Waiting<R::Output>>) -> Self {
        Self {
            slots: Mutex::new(slots),
            target: Mutex::new(Some(target)),
            leaves: AtomicUsize::new(0),
            broken: AtomicBool::new(false),
            giving: AtomicUsize::new(0),
        }
    }

    /// Counts a task linked into the set
    #[inline(always)]
    pub(crate) fn add_leaf(&self) {
        self.leaves.fetch_add(1, Ordering::SeqCst);
    }

    /// Fills part of this round, and hands the round on once it is
    /// full
    pub(crate) fn fill(&self, fill: impl FnOnce(&mut R::Slots)) {
        let full = {
            let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);

            fill(&mut slots);

            match R::filled(&slots) {
                true => {
                    self.giving.fetch_add(1, Ordering::SeqCst);

                    Some(R::assemble(&mut slots))
                }
                false => None,
            }
        };

        let Some(output) = full else {
            return;
        };

        // A copy, so nothing is locked while the give runs
        let target = self
            .target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .map(TaskHandle::clone);

        if let Some(target) = target {
            let _ = target.give(output);
        }

        // Given before this is lowered, and lowered before `broken` is read,
        // against `leaf_gone`, which raises `broken` and then reads this
        self.giving.fetch_sub(1, Ordering::SeqCst);

        if self.broken.load(Ordering::SeqCst) {
            self.let_go();
        }
    }

    /// Hands an empty set on at once, since it is full before anything
    /// arrives, and lets go, since nothing ever will
    pub(crate) fn settle(&self) {
        if self.leaves.load(Ordering::SeqCst) != 0 {
            return;
        }

        self.fill(|_| {});
        self.let_go();
    }

    /// Records that a task in the set publishes nothing more
    ///
    /// No later round can fill. This one still can if that task's slot
    /// is already full, and a round already emptied out of the slots is
    /// still on its way, so in either case the target is let go by the
    /// fill that gives it, not here
    pub(crate) fn leaf_gone(&self, filled: impl FnOnce(&mut R::Slots) -> bool) {
        self.broken.store(true, Ordering::SeqCst);

        let still_coming = {
            let mut slots = self.slots.lock().unwrap_or_else(PoisonError::into_inner);

            filled(&mut slots) || self.giving.load(Ordering::SeqCst) != 0
        };

        if !still_coming {
            self.let_go();
        }
    }

    /// Whether the target takes nothing more
    pub(crate) fn finished(&self) -> bool {
        self.target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_ref()
            .is_none_or(|target| !target.takes_gives())
    }

    /// Writes the target off, after a copy into the set panicked
    pub(crate) fn write_off(&self) {
        let target = self
            .target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();

        if let Some(target) = target {
            executor::write_off(target.id());
        }
    }

    /// Gives up this set's claim on the target, so it finishes once
    /// nothing else can give to it
    fn let_go(&self) {
        let target = self
            .target
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();

        drop(target);
    }
}

/// One task's place in a set
pub(crate) struct GatherLeaf<R, U>
where
    R: HandleSet,
{
    /// The set it fills part of
    gather: Arc<Gather<R>>,

    /// Its own slot in the set
    slot: Access<R::Slots, Option<U>>,
}

impl<R, U> GatherLeaf<R, U>
where
    R: HandleSet,
{
    /// Fills `slot` of `gather`
    pub(crate) fn new(gather: Arc<Gather<R>>, slot: Access<R::Slots, Option<U>>) -> Self {
        Self { gather, slot }
    }
}

impl<R, U> Forward for GatherLeaf<R, U>
where
    R: HandleSet,
    U: Clone + Send + 'static,
{
    unsafe fn deliver(&self, payload: *const u8) {
        let value = unsafe { (*payload.cast::<U>()).clone() };

        self.gather.fill(|slots| *(self.slot)(slots) = Some(value));
    }

    fn finished(&self) -> bool {
        self.gather.finished()
    }

    fn fail(&self) {
        self.gather.write_off();
    }
}

impl<R, U> Drop for GatherLeaf<R, U>
where
    R: HandleSet,
{
    /// Tells the set this task publishes nothing more
    fn drop(&mut self) {
        self.gather.leaf_gone(|slots| (self.slot)(slots).is_some());
    }
}
