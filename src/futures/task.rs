//! Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use std::{ptr, thread::self, time::{Duration, Instant}};
use libc::c_void;
use crate::{constants::SLEEP_TOLERANCE, futures::sleep_task::SleepTask, modules::{event_type::EventType, int_check::IntCheck, kevent::{KEvent, eventlist}, kqueue, thread_policy}};

/// Definition of a task that all things
/// passed into a runtime function must implement
/// to function correctly
pub trait Task {
    type Output;

    /// Executes the task, offloading
    /// to the kernal if required
    fn execute(
        &self,
        reactor_id: i32,
        called_at: Instant,
    ) -> Self::Output;

    /// Gets the type of event
    fn as_event(&self) -> EventType;

    /// Gets the type specific data to be passed into the event
    fn get_intptr_t_data(&self, called_at: Instant) -> libc::intptr_t;

    /// Gets the user data to send through `kevent`
    fn get_udata(&self) -> *mut c_void;

    /// Offloads the work to
    /// the kernal via a
    /// kqueue syscall
    /// and waits for a response
    #[inline(always)]
    fn syscalls(
        &self,
        reactor_id: i32,
        called_at: Instant,
    ) {
        let _ = unsafe {
            KEvent::register(
                reactor_id,
                self.as_event(),
                self.get_intptr_t_data(called_at),
                self.get_udata(),
            )
        }.check();

        thread::park();
    }

    /// Handling of data from the kernel
    fn offload(
        &self,
        reactor_id: i32,
        called_at: Instant,
    ) -> Self::Output;
}

impl Task for SleepTask {
    type Output = Duration;

    #[inline(always)]
    fn execute(
        &self,
        _reactor_id: i32,
        called_at: Instant,
    ) -> Self::Output {
        if self.p_mode {
            thread_policy::promote();
        }

        if !self.p_mode || self.sleep_for > SLEEP_TOLERANCE {
            return self.offload(kqueue::id(), called_at);
        }

        let until = called_at + self.sleep_for;
        self.spinlock(until);

        return called_at.elapsed();
    }

    #[inline(always)]
    fn as_event(&self) -> EventType {
        EventType::Sleep
    }

    #[inline(always)]
    fn get_intptr_t_data(&self, called_at: Instant) -> libc::intptr_t {
        let target = if self.p_mode {
            self.sleep_for.saturating_sub(SLEEP_TOLERANCE)
        } else {
            self.sleep_for
        };

        return target.saturating_sub(called_at.elapsed()).as_nanos() as libc::intptr_t;
    }

    #[inline(always)]
    fn get_udata(&self) -> *mut c_void {
        ptr::null_mut()
    }

    #[inline(always)]
    fn offload(
        &self,
        sleep_id: i32,
        called_at: Instant,
    ) -> Self::Output
    {
        // Sleep functions wait on their own thread's
        // queue rather than going through the reactor,
        // which avoids the overhead of the unpark
        let _ = unsafe {
            KEvent::register(
                sleep_id,
                self.as_event(),
                self.get_intptr_t_data(called_at),
                self.get_udata(),
            )
        }.check();

        let mut events = eventlist();
        let _ = unsafe { KEvent::listen(sleep_id, &mut events) }.check();

        if self.p_mode {
            let until = called_at + self.sleep_for;
            self.spinlock(until);
        }

        return called_at.elapsed();
    }
}
