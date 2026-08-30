//! Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use std::thread;
use crate::{constants::SLEEP_TOLERANCE, futures::sleep_task::SleepTask, modules::{event_type::EventType, int_check::IntCheck, kevent::KEvent}};

/// Definition of a task that all things
/// passed into a runtime function must implement
/// to function correctly
pub trait Task {
    type Output;

    /// Executes the task, offloading
    /// to the kernal if required
    fn execute(&self, id: i32) -> Self::Output;

    /// Gets the type of event
    fn as_event(&self) -> EventType;

    /// Gets the type specific data to be passed into the event
    fn get_intptr_t_data(&self) -> libc::intptr_t;

    /// Offloads the work to
    /// the kernal via a
    /// kqueue syscall
    fn offload(&self, id: i32) {
        let _ = unsafe {
            KEvent::register(
                id,
                self.as_event(),
                self.get_intptr_t_data(),
            )
        }.check();
    }
}

impl Task for SleepTask {
    type Output = ();

    fn execute(&self, id: i32) -> Self::Output {
        if self.sleep_for > SLEEP_TOLERANCE {
            self.offload(id);
        } else {
            thread::sleep(self.sleep_for);
        }
    }

    fn as_event(&self) -> EventType {
        EventType::Sleep
    }

    fn get_intptr_t_data(&self) -> libc::intptr_t {
        self.sleep_for.as_nanos() as libc::intptr_t
    }
}
