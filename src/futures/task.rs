//! # Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use crate::{
    EventDesc, constants::{SLEEP_TOLERANCE, WAKE_IDENT}, futures::sleep_task::SleepTask, modules::{
        int_check::IntCheck,
        kevent::KEvent,
        kqueue, thread_policy,
        waiter::Waiter,
        wake_target::WakeTarget,
    },
};
use std::time::{Duration, Instant};

/// Marker that closes `Task` to the outside world
///
/// `Task` itself can't be crate private. `Runtime::block` and
/// `Runtime::spawn` are public and both name `Task::Output`
/// in their signatures, and naming a crate private associated
/// type from a public one is an error rather than something
/// that can be allowed away
///
/// Sealing gets to the same place from the other side. The
/// trait can be named from outside the crate, which is all
/// the public signatures need, but it can't be implemented,
/// because implementing it means implementing this first and
/// this can't be named out there at all
mod sealed {
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
    fn register_event(
        &self,
        reactor_id: i32,
        task_id: usize,
        desc: EventDesc
    ) {
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

        return self.created.elapsed();
    }

    #[inline(always)]
    fn prepare(&mut self) {
        self.created = Instant::now();

        if self.p_mode {
            thread_policy::promote();
        }
    }

    #[inline(always)]
    fn get_intptr_t_data(&self) -> libc::intptr_t {
        let target = if self.p_mode {
            self.sleep_for.saturating_sub(SLEEP_TOLERANCE)
        } else {
            self.sleep_for
        };

        return target.saturating_sub(self.created.elapsed()).as_nanos() as libc::intptr_t;
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
        match queue {
            Some(queue) => self.wait_on_own(queue, task_id),
            None => self.wait_on_reactor(reactor_id, task_id),
        }

        if self.p_mode {
            let until = self.created + self.sleep_for;
            self.spinlock(until);
        }

        return self.created.elapsed();
    }
}

impl SleepTask {
    /// Puts the timer on this thread's own queue and waits
    /// there, with nothing else involved in the wake
    ///
    /// `udata` is left empty, because the only thread that
    /// could be woken is the one already sitting on the queue
    #[inline(always)]
    fn wait_on_own(&self, queue: i32, task_id: usize) {
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

        if registered.is_err() {
            return;
        }

        kqueue::wait_for(queue, task_id, libc::EVFILT_TIMER);
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
