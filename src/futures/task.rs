//! # Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use crate::{
    EventDesc, constants::SLEEP_TOLERANCE, futures::sleep_task::SleepTask, modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        kqueue, thread_policy,
    },
};
use libc::c_void;
use std::{ptr, thread, time::{Duration, Instant}};

/// Definition of a task that all things
/// passed into a runtime function must implement
/// to function correctly
pub trait Task {
    /// The final output type
    type Output;

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

    /// Gets the user data to send through `kevent`
    fn get_udata(&self) -> *mut c_void;

    /// Registers the event with the
    /// kernel and waits for a response
    /// from the `Reactor`
    #[inline(always)]
    fn register_event(
        &self,
        reactor_id: i32,
        task_id: usize,
        desc: EventDesc
    ) {
        let _ = unsafe {
            KEvent::register(
                reactor_id,
                task_id,
                self.get_intptr_t_data(),
                self.get_udata(),
                desc,
            )
        }
        .check();

        thread::park();
    }

    /// Prepares data and handles what comes back
    /// from the kernel
    /// 
    /// Not required to make any syscalls
    fn offload(&self, reactor_id: i32, task_id: usize) -> Self::Output;
}

impl Task for SleepTask {
    type Output = Duration;

    #[inline(always)]
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        if !self.p_mode || self.sleep_for > SLEEP_TOLERANCE {
            let id = kqueue::id().unwrap_or(-reactor_id); // Negate so the failure can be detected later
            return self.offload(id, task_id);
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
    fn get_udata(&self) -> *mut c_void {
        ptr::null_mut()
    }

    #[inline(always)]
    fn offload(&self, sleep_id: i32, task_id: usize) -> Self::Output {
        // Sleep functions wait on their own thread's
        // queue rather than going through the reactor,
        // which avoids the overhead of the unpark
        //
        // Unless the unique reactor id could not be resolved
        // in which case it falls back to the usual path
        let _ = unsafe {
            let udata = if sleep_id < 0 {
                Box::into_raw(Box::new(thread::current())) as *mut c_void
            } else {
                self.get_udata()
            };

            KEvent::register(
                sleep_id.abs(),
                task_id,
                self.get_intptr_t_data(),
                udata,
                EventDesc::new_timer(),
            )
        }
        .check();

        let mut events = eventlist();
        let _ = unsafe { KEvent::listen(sleep_id.abs(), &mut events) }.check();

        if self.p_mode {
            let until = self.created + self.sleep_for;
            self.spinlock(until);
        }

        return self.created.elapsed();
    }
}
