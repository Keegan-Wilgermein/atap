//! # Sleep task
//! The tasks associated with sleeping
//!
//! Performs sleep functions defined by the `Sleep`
//! struct

use crate::{
    EventDesc,
    constants::SLEEP_TOLERANCE,
    executor,
    futures::{kernel_wait::KernelWait, task::Task, task::sealed},
    modules::{
        int_check::IntCheck,
        kevent::KEvent,
        kqueue::{self, Waited},
        waiter::Waiter,
        wake_target::WakeTarget,
    },
};
use std::{
    ptr,
    time::{Duration, Instant},
};

/// What happened to a sleep that went to the kernel
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slept {
    /// The kernel held the thread for the duration asked of it
    Waited,

    /// Somebody cancelled the sleep part way through
    Cancelled,

    /// The kernel never took the wait, so the whole duration is
    /// still owed
    Refused,
}

/// The version of `Sleep` that implements `Task`
///
/// It can be passed into async functions
/// and contains info on its functionality
///
/// All it's runtime functions output `Duration`
/// describing the time it took for the function
/// to run in it's entirety
#[derive(Clone)]
pub struct SleepTask {
    /// How long to sleep for
    pub(crate) sleep_for: Duration,

    /// When the task started execution
    pub(crate) created: Instant,

    /// Whether to trade cpu for precision
    ///
    /// On, the last stretch of the wait is spun rather than
    /// slept
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
    /// there
    #[inline(always)]
    fn wait_on_own(&self, queue: i32, task_id: usize) -> Slept {
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

        // Spun out instead, since the alternative is a sleep that
        // doesn't sleep
        if registered.is_err() {
            return Slept::Refused;
        }

        // Cancelled before the wait started, so the timer comes
        // straight back off
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

            return Slept::Cancelled;
        }

        let waited = kqueue::wait_for(queue, task_id, libc::EVFILT_TIMER);

        // Also spins out a cancel still part way through its
        // syscalls against this queue
        if !executor::stopped_waiting() {
            return Slept::Cancelled;
        }

        match waited {
            Waited::Failed => Slept::Refused,
            Waited::Cancelled => Slept::Cancelled,
            Waited::Arrived => Slept::Waited,
        }
    }

    /// Puts the timer on the `Reactor`'s queue and parks
    ///
    /// Only used when this thread couldn't get a queue of its
    /// own
    #[inline(always)]
    fn wait_on_reactor(&self, reactor_id: i32, task_id: usize) -> Slept {
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
            return Slept::Refused;
        }

        waiter.wait();

        Slept::Waited
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
    #[inline(always)]
    fn prepare(&mut self) {
        self.created = Instant::now();
    }

    /// Whether this sleep ends up waiting in the kernel
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

        let nanos = target.saturating_sub(self.created.elapsed()).as_nanos();

        nanos.min(libc::intptr_t::MAX as u128) as libc::intptr_t
    }

    #[inline(always)]
    fn offload(&self, queue: Option<i32>, reactor_id: i32, task_id: usize) -> Self::Output {
        // The thread's own queue avoids the overhead of an unpark,
        // and the reactor is only used when there isn't one
        let slept = match queue {
            Some(queue) => self.wait_on_own(queue, task_id),
            None => self.wait_on_reactor(reactor_id, task_id),
        };

        let spin = match slept {
            Slept::Cancelled => false,

            Slept::Waited => self.p_mode,

            // Nothing waited, so the whole duration is spun out
            Slept::Refused => true,
        };

        if spin {
            let until = self.created + self.sleep_for;
            self.spinlock(until);
        }

        self.created.elapsed()
    }
}
