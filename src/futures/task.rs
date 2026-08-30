//! Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use std::{thread, time::{Instant}};
use libc::c_void;
use crate::{constants::{SLEEP_TOLERANCE}, futures::sleep_task::SleepTask, modules::{event_type::EventType, int_check::IntCheck, kevent::KEvent}};

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
    ) -> Self::Output;

    /// Gets the type of event
    fn as_event(&self) -> EventType;

    /// Gets the type specific data to be passed into the event
    fn get_intptr_t_data(&self) -> libc::intptr_t;

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
    ) {
        let _ = unsafe {
            KEvent::register(
                reactor_id,
                self.as_event(),
                self.get_intptr_t_data(),
                self.get_udata(),
            )
        }.check();

        thread::park();
    }

    /// Handling of data from the kernel
    fn offload(
        &self,
        reactor_id: i32,
    ) -> Self::Output;
}

impl Task for SleepTask {
    type Output = ();

    #[inline(always)]
    fn execute(
        &self,
        reactor_id: i32,
    ) -> Self::Output {
        if self.sleep_for > SLEEP_TOLERANCE {
            self.offload(reactor_id);
        } else {
            let until = Instant::now() + self.sleep_for;
            self.spinlock(until);
        }
    }

    #[inline(always)]
    fn as_event(&self) -> EventType {
        EventType::Sleep
    }

    #[inline(always)]
    fn get_intptr_t_data(&self) -> libc::intptr_t {
        (self.sleep_for - SLEEP_TOLERANCE).as_nanos() as libc::intptr_t
    }

    #[inline(always)]
    fn get_udata(&self) -> *mut c_void {
        Box::into_raw(Box::new(thread::current())) as *mut c_void
    }

    #[inline(always)]
    fn offload(
        &self,
        reactor_id: i32,
    ) -> Self::Output
    {
        let start = Instant::now();

        self.syscalls(reactor_id);

        let until = Instant::now() + (self.sleep_for - start.elapsed());
        self.spinlock(until);
    }
}
