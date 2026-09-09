//! # Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use crate::{
    EventDesc,
    constants::{SLEEP_TOLERANCE, WAKE_IDENT},
    executor,
    futures::sleep_task::SleepTask,
    modules::{
        int_check::IntCheck, kevent::KEvent, kqueue, waiter::Waiter, wake_target::WakeTarget,
    },
};
use std::{
    ptr,
    time::{Duration, Instant},
};

/// Marker that closes `Task` to the outside world
///
/// `Task` itself can't be crate private. `Runtime::block` and
/// every `TaskBuilder::spawn` are public and all of them name
/// `Task::Output` in their signatures, and naming a crate
/// private associated type from a public one is an error
/// rather than something that can be allowed away
///
/// Sealing gets to the same place from the other side. The
/// trait can be named from outside the crate, which is all
/// the public signatures need, but it can't be implemented,
/// because implementing it means implementing this first and
/// this can't be named out there at all
pub(crate) mod sealed {
    /// Implemented for every type this crate allows as a task
    pub trait Sealed {}
}

/// Definition of a task that all things
/// passed into a runtime function must implement
/// to function correctly
///
/// #### Note
/// `Send` and `'static` are on the trait rather than on
/// `spawn`, because a spawned task is moved onto the
/// `Executor`'s thread and its output is read from a third
/// thread again. Blocking calls don't need either, but
/// splitting the trait in two to say so isn't worth it
#[allow(private_bounds)]
pub trait Task: sealed::Sealed + Send + 'static {
    /// The final output type
    type Output: Send + 'static;

    /// Executes the task, offloading
    /// to the kernal if required
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output;

    /// Any preperation the `Task`
    /// must do before execution
    ///
    /// Delegated to a seperate function
    /// in case it determines whether a
    /// function runs `.execute()` at all
    fn prepare(&mut self);

    /// Gets the type specific data to be passed into the event
    fn get_intptr_t_data(&self) -> libc::intptr_t;

    /// Whether running this will hold the thread long enough
    /// to be worth giving it to a thread that exists to be held
    ///
    /// ## Behaviour
    /// A spawned task that says yes is handed to its worker's
    /// sleep thread rather than run on the worker, so the
    /// worker goes straight back to the queue instead of
    /// sitting inside a `kevent` call for the duration
    ///
    /// Blocking calls ignore this entirely. `Runtime::block`
    /// runs on the caller's thread because that is what the
    /// caller asked for
    ///
    /// #### Note
    /// A hint, and only a hint. Saying no when the answer was
    /// yes costs throughput while a worker sits blocked, and
    /// saying yes when the answer was no costs a hand off that
    /// wasn't needed. Neither is a correctness problem, which
    /// is why the default is the cheaper of the two
    #[inline(always)]
    fn blocking(&self) -> bool {
        false
    }

    /// Registers the event with the
    /// kernel and waits for a response
    /// from the `Reactor`
    ///
    /// ## Behaviour
    /// The event goes on the `Reactor`'s queue carrying the
    /// way back to this thread, and this thread then waits on
    /// a queue of its own. Waking it is a single trigger, with
    /// no thread handle to pass around and nothing to free
    ///
    /// A thread that can't get a queue parks instead, and the
    /// `Reactor` sets its flag and unparks it. Slower, and the
    /// only reason `Waiter` exists
    ///
    /// #### Note
    /// Nothing waits on a registration that didn't take. A
    /// wake only ever comes from an event the kernel accepted,
    /// so waiting on one it refused waits for good
    #[inline(always)]
    fn register_event(&self, reactor_id: i32, task_id: usize, desc: EventDesc) {
        // Split rather than shared so the fast path never
        // builds a `Waiter` it has no use for. Taking one
        // costs a `thread::current`, which is the sort of
        // thing waiting on your own queue exists to avoid
        let Ok(queue) = kqueue::id() else {
            let waiter = Waiter::new();

            let registered = unsafe {
                KEvent::register(
                    reactor_id,
                    task_id,
                    self.get_intptr_t_data(),
                    WakeTarget::Parked(&waiter as *const Waiter as *mut Waiter).encode(),
                    desc,
                )
            }
            .check();

            if registered.is_ok() {
                waiter.wait();
            }

            return;
        };

        let registered = unsafe {
            KEvent::register(
                reactor_id,
                task_id,
                self.get_intptr_t_data(),
                WakeTarget::Queue(queue).encode(),
                desc,
            )
        }
        .check();

        if registered.is_err() {
            return;
        }

        kqueue::wait_for(queue, WAKE_IDENT, libc::EVFILT_USER);
    }

    /// Prepares data and handles what comes back
    /// from the kernel
    ///
    /// Not required to make any syscalls
    ///
    /// `queue` is this thread's own kqueue when it has one,
    /// which is the cheap path, and `None` when it doesn't
    fn offload(&self, queue: Option<i32>, reactor_id: i32, task_id: usize) -> Self::Output;
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

impl SleepTask {
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
    /// Only reached when this thread couldn't get a queue of
    /// its own, which takes a kernel out of descriptors
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
