//! # Extras
//! What only some tasks carry, kept off the slot's header

use crate::{
    constants::NO_TASK,
    modules::{gate::Gate, receivers::Receivers, series::SeriesTask},
};
use libc::c_void;
use std::{
    mem, ptr,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicPtr, Ordering},
    },
    time::Duration,
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

    /// How long each run may take, if it has a limit
    timeout: Option<Duration>,

    /// The series a run of it publishes into, which times out with
    /// it, or `NO_TASK`
    parent: usize,
}

impl Extras {
    /// Extras with no prototype, registrations or claims yet
    pub(crate) fn new(gate: Option<Arc<Gate>>) -> Self {
        Self {
            prototype: AtomicPtr::new(ptr::null_mut()),
            gate,
            receivers: Receivers::new(),
            held: Mutex::new(Vec::new()),
            timeout: None,
            parent: NO_TASK,
        }
    }

    /// The same extras, with each run limited to `timeout`, and a
    /// run past it ending `parent` too
    pub(crate) fn timed(mut self, timeout: Option<Duration>, parent: usize) -> Self {
        self.timeout = timeout;
        self.parent = parent;
        self
    }

    /// How long each run may take, if it has a limit
    #[inline(always)]
    pub(crate) fn timeout(&self) -> Option<Duration> {
        self.timeout
    }

    /// The series this run publishes into, or `NO_TASK`
    #[inline(always)]
    pub(crate) fn parent(&self) -> usize {
        self.parent
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
