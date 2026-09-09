//! # Sleep task
//! The tasks associated with sleeping
//!
//! Performs sleep functions defined by the `Sleep`
//! struct
//!
//! Holds every impl `SleepTask` has — the data, the `Task` it
//! satisfies, the `KernelWait` underneath it, and the two ways
//! it waits. A reader asking what a sleep does has one file to
//! read, and `task.rs` is left holding the definition alone

use crate::{
    EventDesc,
    constants::SLEEP_TOLERANCE,
    executor,
    futures::{kernel_wait::KernelWait, task::Task, task::sealed},
    modules::{
        int_check::IntCheck, kevent::KEvent, kqueue, waiter::Waiter, wake_target::WakeTarget,
    },
};
use std::{
    ptr,
    time::{Duration, Instant},
};

/// The version of `Sleep` that implements `Task`
///
/// It can be passed into async functions
/// and contains info on its functionality
///
/// All it's runtime functions output `Duration`
/// describing the time it took for the function
/// to run in it's entirety
///
/// #### Note
/// `Clone` because `.at_rate()` makes a fresh copy of its task
/// for every run it starts. A copy carries the duration and the
/// mode across and nothing else that matters — the start time
/// is overwritten by `prepare` before the copy is ever run, so
/// each run is timed from its own beginning
#[derive(Clone)]
pub struct SleepTask {
    /// How long to sleep for
    pub(crate) sleep_for: Duration,

    /// When the task started execution
    pub(crate) created: Instant,

    /// Whether to trade cpu for precision
    ///
    /// On, the last stretch of the wait is spun rather than
    /// slept. Off, every sleep is handed to the kernel and
    /// whatever comes back is the answer
    pub(crate) p_mode: bool,
}

impl SleepTask {
    /// Creates a new `SleepTask`
    pub(crate) fn new(time: Duration, p_mode: bool) -> Self {
        Self {
            sleep_for: time,
            created: Instant::now(),
            p_mode,
        }
    }

    /// Spins the cpu until hitting the given time parameter
    pub(crate) fn spinlock(&self, until: Instant) {
        while Instant::now() < until {}
    }

    /// Puts the timer on this thread's own queue and waits
    /// there, with nothing else involved in the wake
    ///
    /// `udata` is left empty, because the only thread that
    /// could be woken is the one already sitting on the queue
    ///
    /// ## Returns
    /// Whether the sleep is still worth finishing. A spawned
    /// sleep that was cancelled comes back here early and has
    /// nothing left to do
    #[inline(always)]
    fn wait_on_own(&self, queue: i32, task_id: usize) -> bool {
        let registered = unsafe {
            KEvent::register(
                queue,
                task_id,
                self.get_intptr_t_data(),
                WakeTarget::None.encode(),
                EventDesc::new_timer(),
            )
        }
        .check();

        // Nothing waits on a registration the kernel refused,
        // and a spin is left to make up the time as best it can
        if registered.is_err() {
            return true;
        }

        // Cancelled before the wait even started, so the timer
        // comes straight back off rather than going off later
        // into a queue nobody is waiting on it in
        if !executor::waiting_on(queue) {
            let _ = unsafe {
                KEvent::register(
                    queue,
                    task_id,
                    0,
                    ptr::null_mut(),
                    EventDesc::new_timer_delete(),
                )
            }
            .check();

            return false;
        }

        kqueue::wait_for(queue, task_id, libc::EVFILT_TIMER);

        executor::stopped_waiting()
    }

    /// Puts the timer on the `Reactor`'s queue and parks
    ///
    /// ## Behaviour
    /// The event goes on the `Reactor`'s queue carrying the way
    /// back to this thread, and the `Reactor` sets the flag and
    /// unparks it. Slower than waiting on a queue of your own,
    /// and the only reason `Waiter` exists
    ///
    /// Only reached when this thread couldn't get a queue of
    /// its own, which takes a kernel out of descriptors
    ///
    /// #### Note
    /// Nothing waits on a registration that didn't take. A wake
    /// only ever comes from an event the kernel accepted, so
    /// waiting on one it refused waits for good
    #[inline(always)]
    fn wait_on_reactor(&self, reactor_id: i32, task_id: usize) {
        let waiter = Waiter::new();

        let registered = unsafe {
            KEvent::register(
                reactor_id,
                task_id,
                self.get_intptr_t_data(),
                WakeTarget::Parked(&waiter as *const Waiter as *mut Waiter).encode(),
                EventDesc::new_timer(),
            )
        }
        .check();

        if registered.is_err() {
            return;
        }

        waiter.wait();
    }
}

impl sealed::Sealed for SleepTask {}

impl Task for SleepTask {
    type Output = Duration;

    #[inline(always)]
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        if !self.p_mode || self.sleep_for > SLEEP_TOLERANCE {
            return self.offload(kqueue::id().ok(), reactor_id, task_id);
        }

        let until = self.created + self.sleep_for;
        self.spinlock(until);

        self.created.elapsed()
    }

    /// Times every run from its own beginning
    ///
    /// One of the few tasks that needs this. A repeat puts the
    /// same box back in the same slot, so a `created` left over
    /// from the last run would have the next one measuring from
    /// a start it didn't have
    #[inline(always)]
    fn prepare(&mut self) {
        self.created = Instant::now();
    }

    /// The same question `execute` asks itself
    ///
    /// A spin never reaches a sleep thread, because it never
    /// gives the thread up in the first place. Everything else
    /// ends up inside a `kevent` call for the whole duration,
    /// which is precisely what a worker shouldn't be doing
    #[inline(always)]
    fn blocking(&self) -> bool {
        !self.p_mode || self.sleep_for > SLEEP_TOLERANCE
    }
}

impl KernelWait for SleepTask {
    #[inline(always)]
    fn get_intptr_t_data(&self) -> libc::intptr_t {
        let target = if self.p_mode {
            self.sleep_for.saturating_sub(SLEEP_TOLERANCE)
        } else {
            self.sleep_for
        };

        target.saturating_sub(self.created.elapsed()).as_nanos() as libc::intptr_t
    }

    #[inline(always)]
    fn offload(&self, queue: Option<i32>, reactor_id: i32, task_id: usize) -> Self::Output {
        // Sleep functions wait on their own thread's
        // queue rather than going through the reactor,
        // which avoids the overhead of the unpark
        //
        // Unless the thread couldn't get a queue, in which
        // case the timer goes to the reactor like anything
        // else and the thread parks for it
        let carry_on = match queue {
            Some(queue) => self.wait_on_own(queue, task_id),
            None => {
                self.wait_on_reactor(reactor_id, task_id);
                true
            }
        };

        // A cancelled sleep has nothing left to be accurate
        // about. Spinning out the rest of a duration nobody is
        // waiting for would give the thread straight back to
        // the kernel wait it was just taken out of
        if carry_on && self.p_mode {
            let until = self.created + self.sleep_for;
            self.spinlock(until);
        }

        self.created.elapsed()
    }
}
