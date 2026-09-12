//! # Sleep task
//! The tasks associated with sleeping
//!
//! Performs sleep functions defined by the `Sleep`
//! struct

use crate::{
    EventDesc,
    constants::SLEEP_TOLERANCE,
    executor,
    futures::{kernel_wait::KernelWait, sleep::SleepMode, task::Task, task::sealed},
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
    pub(crate) mode: SleepMode,
}

impl SleepTask {
    /// Creates a new `SleepTask`, precise until told otherwise
    pub(crate) fn new(time: Duration) -> Self {
        Self {
            sleep_for: time,
            created: Instant::now(),
            mode: SleepMode::Precise,
        }
    }

    /// Trades cpu time for precision, or gives it back
    ///
    /// ## Behaviour
    /// [`SleepMode::Precise`], the default, spins the last stretch
    /// of the wait rather than sleeping it.
    /// [`SleepMode::Relaxed`] leaves all of it to the kernel and
    /// burns nothing
    ///
    /// ## Accuracy
    /// Measured on Apple silicon, for targets from 400 nanoseconds
    /// to 30 seconds
    ///
    /// `Precise` is around 200 nanoseconds over at every duration,
    /// about 5200x more accurate than `thread::sleep()`, and holds a
    /// core for up to 500 microseconds
    ///
    /// `Relaxed` is around 4 microseconds over on short waits and
    /// around 60 on waits of a few milliseconds or more, about 37x
    /// more accurate than `thread::sleep()`
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn mode(mut self, mode: SleepMode) -> Self {
        self.mode = mode;
        self
    }

    /// Whether this sleep spins its last stretch
    #[inline(always)]
    pub(crate) fn precise(&self) -> bool {
        self.mode == SleepMode::Precise
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
        if !self.precise() || self.sleep_for > SLEEP_TOLERANCE {
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
        !self.precise() || self.sleep_for > SLEEP_TOLERANCE
    }
}

impl KernelWait for SleepTask {
    #[inline(always)]
    fn get_intptr_t_data(&self) -> libc::intptr_t {
        let target = if self.precise() {
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

            Slept::Waited => self.precise(),

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
