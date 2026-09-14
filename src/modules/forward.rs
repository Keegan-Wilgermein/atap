//! # Forward
//! What one registration on a task does with that task's outputs

use crate::{
    executor,
    modules::{handle_kind::Waiting, task_handle::TaskHandle},
};

/// One registration on a task's outputs
pub(crate) trait Forward: Send + Sync {
    /// Hands the output at `payload` on
    ///
    /// ## Safety
    /// `payload` must point at a readable output of the forwarding
    /// task's type, and the read must be held for the whole call
    unsafe fn deliver(&self, payload: *const u8);

    /// Whether the far end takes nothing more, so the registration
    /// can go
    fn finished(&self) -> bool;

    /// Writes the far end off, after a delivery panicked
    fn fail(&self);
}

/// Gives each output to a task that waits for gives, turned into
/// what that task takes on the way
pub(crate) struct Forwarder<V>
where
    V: Send + 'static,
{
    /// The task given to, which this counts as a giver of
    target: TaskHandle<(), Waiting<V>>,

    /// Reads an output and turns it into what the target takes
    take: unsafe fn(*const u8) -> V,
}

impl<V> Forwarder<V>
where
    V: Send + 'static,
{
    /// Forwards to `target`, reading each output with `take`
    pub(crate) fn new(target: TaskHandle<(), Waiting<V>>, take: unsafe fn(*const u8) -> V) -> Self {
        Self { target, take }
    }
}

impl<V> Forward for Forwarder<V>
where
    V: Send + 'static,
{
    unsafe fn deliver(&self, payload: *const u8) {
        let value = unsafe { (self.take)(payload) };

        // A target that has stopped taking gives is let go on the next walk
        let _ = self.target.give(value);
    }

    fn finished(&self) -> bool {
        !self.target.takes_gives()
    }

    fn fail(&self) {
        executor::write_off(self.target.id());
    }
}

/// Copies an output out and turns it into what the far end takes
///
/// ## Safety
/// `payload` must point at a readable `U`
pub(crate) unsafe fn cloned_into<U, V>(payload: *const u8) -> V
where
    U: Clone + Into<V>,
{
    unsafe { (*payload.cast::<U>()).clone() }.into()
}

/// Starts the far end without reading the output at all, for a task
/// that takes nothing
///
/// ## Safety
/// Always safe, since nothing is read
pub(crate) unsafe fn nothing(_payload: *const u8) {}
