//! # Receivers
//! Every registration on a task's outputs, and the one walk at a
//! time that hands each output to them
//!
//! Anyone can register at any moment, and whoever registers walks
//! the list straight away, so an output published before the
//! registration still reaches it. Walks never overlap: a walk that
//! can't take the lock leaves a request for whoever holds it. Each
//! registration remembers the last output it was handed, so no
//! output reaches it twice however the walks and publishes race

use crate::modules::{forward::Forward, task_data::TaskData};
use std::{
    panic::{self, AssertUnwindSafe},
    ptr,
    sync::atomic::{AtomicPtr, AtomicU8, AtomicU64, Ordering},
};

/// A walk is under way
const LOCKED: u8 = 1;

/// Something changed since the walk under way last looked
const REWALK: u8 = 2;

/// The task publishes nothing more
const CLOSED: u8 = 4;

/// Marks a registration that has been handed nothing yet
const NONE_HANDED: u64 = u64::MAX;

/// One registration, hung off the task it forwards from
struct Node {
    /// The next registration down
    ///
    /// Written by a registrant before the node can be seen, and after
    /// that only by the walk holding the lock
    next: *mut Node,

    /// The output this was last handed, by generation
    ///
    /// Only the walk holding the lock reads or writes it
    handed: u64,

    /// What it does with an output
    forward: Box<dyn Forward>,
}

/// The registrations on one task's outputs
pub(crate) struct Receivers {
    /// The latest registration, with the rest linked behind it
    head: AtomicPtr<Node>,

    /// Counts the outputs published, moved on before each is readable
    generation: AtomicU64,

    /// `LOCKED`, `REWALK` and `CLOSED`
    flags: AtomicU8,
}

impl Receivers {
    /// No registrations
    pub(crate) const fn new() -> Self {
        Self {
            head: AtomicPtr::new(ptr::null_mut()),
            generation: AtomicU64::new(0),
            flags: AtomicU8::new(0),
        }
    }

    /// Registers `forward` on `upstream`'s outputs
    ///
    /// Walks straight away, so an output already there is handed over
    /// at once, and a task that already publishes nothing more lets
    /// the registration go
    pub(crate) fn register(&self, forward: Box<dyn Forward>, upstream: &TaskData) {
        let node = Box::into_raw(Box::new(Node {
            next: ptr::null_mut(),
            handed: NONE_HANDED,
            forward,
        }));

        let mut head = self.head.load(Ordering::SeqCst);

        loop {
            unsafe { (*node).next = head };

            match self
                .head
                .compare_exchange_weak(head, node, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(now) => head = now,
            }
        }

        // A task already over had nothing to close when it ended, so the
        // registration closes it, after handing over what is readable
        if upstream.publishes_nothing_more() {
            self.flags.fetch_or(CLOSED, Ordering::SeqCst);
        }

        self.walk(upstream);
    }

    /// Says an output is about to become readable
    #[inline(always)]
    pub(crate) fn note_output(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Hands the readable output to every registration that hasn't had
    /// it, and lets go of any whose far end is finished
    pub(crate) fn walk(&self, upstream: &TaskData) {
        // Asked for before anything is looked at. A walk under way has taken
        // the whole list off the head, so an empty head can mean a pass that
        // looked before this output was readable, which has to look again
        self.flags.fetch_or(REWALK, Ordering::SeqCst);

        // Nothing registered and nobody walking, which is every task until
        // something registers. A registrant walks for itself once it has
        // pushed
        if self.head.load(Ordering::SeqCst).is_null()
            && self.flags.load(Ordering::SeqCst) & LOCKED == 0
        {
            return;
        }

        loop {
            // Held by another walk, which sees the request before it lets go
            if self.flags.fetch_or(LOCKED, Ordering::SeqCst) & LOCKED != 0 {
                return;
            }

            // Lets go of the lock even if the walk unwinds, so a thread going
            // down part way through never stops every later walk
            let unlock = Unlock(&self.flags);

            while self.flags.fetch_and(!REWALK, Ordering::SeqCst) & REWALK != 0 {
                self.pass(upstream);
            }

            drop(unlock);

            // A request that landed after the last look, before the lock went
            if self.flags.load(Ordering::SeqCst) & REWALK == 0 {
                return;
            }
        }
    }

    /// Says the task publishes nothing more
    ///
    /// The last output still goes to any registration that hasn't had
    /// it, then every registration is let go, including any made later
    pub(crate) fn close(&self, upstream: &TaskData) {
        self.flags.fetch_or(CLOSED | REWALK, Ordering::SeqCst);

        self.walk(upstream);
    }

    /// One look at every registration, under the lock
    fn pass(&self, upstream: &TaskData) {
        let closed = self.flags.load(Ordering::SeqCst) & CLOSED != 0;

        // Taken off whole, so a registrant pushing meanwhile starts a fresh
        // list rather than racing this walk for a link
        let mut node = self.head.swap(ptr::null_mut(), Ordering::SeqCst);

        if node.is_null() {
            return;
        }

        // Held for the whole pass, so the output can't be taken or
        // replaced while it is being copied out
        let readable = upstream.enter_read();
        let generation = self.generation.load(Ordering::SeqCst);

        let mut kept: *mut Node = ptr::null_mut();
        let mut kept_tail: *mut Node = ptr::null_mut();

        while !node.is_null() {
            let current = node;

            node = unsafe { (*current).next };

            if unsafe { visit(&mut *current, upstream, readable, generation, closed) } {
                unsafe { (*current).next = kept };

                if kept.is_null() {
                    kept_tail = current;
                }

                kept = current;
                continue;
            }

            let gone = unsafe { Box::from_raw(current) };

            // A far end's drop can run a program's own code, which can't be
            // allowed to leave the lock held
            let _ = panic::catch_unwind(AssertUnwindSafe(|| drop(gone)));
        }

        if readable {
            upstream.leave_read();
        }

        if kept.is_null() {
            return;
        }

        // Back beside anything registered meanwhile
        let mut head = self.head.load(Ordering::SeqCst);

        loop {
            unsafe { (*kept_tail).next = head };

            match self
                .head
                .compare_exchange_weak(head, kept, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(now) => head = now,
            }
        }
    }
}

/// Lets go of a walk's lock when dropped
struct Unlock<'a>(&'a AtomicU8);

impl Drop for Unlock<'_> {
    fn drop(&mut self) {
        self.0.fetch_and(!LOCKED, Ordering::SeqCst);
    }
}

/// Hands a registration the output if it hasn't had it
///
/// ## Returns
/// Whether the registration stays
///
/// ## Safety
/// Only under the lock, with the read held if `readable`
unsafe fn visit(
    node: &mut Node,
    upstream: &TaskData,
    readable: bool,
    generation: u64,
    closed: bool,
) -> bool {
    let unseen = node.handed == NONE_HANDED || node.handed < generation;

    if readable && unseen {
        node.handed = generation;

        let payload = upstream.payload();
        let forward = &node.forward;

        // Clones and conversions are the program's own code. One that panics
        // costs its far end, not the task or the walk
        let handed = panic::catch_unwind(AssertUnwindSafe(|| unsafe { forward.deliver(payload) }));

        if handed.is_err() {
            let _ = panic::catch_unwind(AssertUnwindSafe(|| forward.fail()));

            return false;
        }
    }

    !closed && !node.forward.finished()
}

impl Drop for Receivers {
    /// Lets every registration still hung here go, without handing
    /// anything over
    fn drop(&mut self) {
        let mut node = *self.head.get_mut();

        while !node.is_null() {
            let gone = unsafe { Box::from_raw(node) };

            node = gone.next;

            drop(gone);
        }
    }
}
