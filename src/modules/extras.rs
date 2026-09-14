//! # Extras
//! What only some tasks carry, kept off the slot's header so a
//! plain task pays nothing for it

use crate::modules::{gate::Gate, receivers::Receivers, series::SeriesTask};
use libc::c_void;
use std::{
    mem, ptr,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicPtr, Ordering},
    },
};

/// A schedule's prototype, the gate of a task that waits, the
/// registrations on a task's outputs, and what a task that receives
/// holds onto
pub(crate) struct Extras {
    /// The task a `Series` clones its runs from, or null
    prototype: AtomicPtr<c_void>,

    /// The gate a task spawned with `wait_for` is started through
    gate: Option<Arc<Gate>>,

    /// Tasks this one's outputs are forwarded to
    receivers: Receivers,

    /// Claims kept until the task is finished, like those on the tasks
    /// it receives from
    held: Mutex<Vec<Box<dyn Send>>>,
}

impl Extras {
    /// Extras with no prototype, registrations or claims yet
    pub(crate) fn new(gate: Option<Arc<Gate>>) -> Self {
        Self {
            prototype: AtomicPtr::new(ptr::null_mut()),
            gate,
            receivers: Receivers::new(),
            held: Mutex::new(Vec::new()),
        }
    }

    /// The task a series clones its runs from, or null
    #[inline(always)]
    pub(crate) fn prototype(&self) -> *mut c_void {
        self.prototype.load(Ordering::Acquire)
    }

    /// Gives a series the task it makes copies of
    ///
    /// The pointer must be a `Box<Box<dyn SeriesTask>>`
    #[inline(always)]
    pub(crate) fn set_prototype(&self, prototype: *mut c_void) {
        self.prototype.store(prototype, Ordering::Release);
    }

    /// The gate, for a task that waits
    #[inline(always)]
    pub(crate) fn gate(&self) -> Option<&Gate> {
        self.gate.as_deref()
    }

    /// The registrations on this task's outputs
    #[inline(always)]
    pub(crate) fn receivers(&self) -> &Receivers {
        &self.receivers
    }

    /// Keeps `claims` until the task is finished
    pub(crate) fn hold(&self, claims: Vec<Box<dyn Send>>) {
        self.held
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend(claims);
    }

    /// Lets go of every claim kept, once the task is finished
    pub(crate) fn release_held(&self) {
        let claims = mem::take(&mut *self.held.lock().unwrap_or_else(PoisonError::into_inner));

        drop(claims);
    }
}

impl Drop for Extras {
    /// A series owns its prototype
    fn drop(&mut self) {
        let prototype = self.prototype.swap(ptr::null_mut(), Ordering::AcqRel);

        if !prototype.is_null() {
            drop(unsafe { Box::from_raw(prototype.cast::<Box<dyn SeriesTask>>()) });
        }
    }
}
