//! Task
//! A trait that defines a task that can be
//! initialised and run asynchronously

use std::thread;

use crate::{constants::SLEEP_TOLERANCE, futures::sleep_task::SleepTask};

/// Definition of a task that all things
/// passed into a runtime function must implement
/// to function correctly
pub trait Task {
    type Output;

    /// Executes the task, offloading
    /// to the kernal if required
    fn execute(&self) -> Self::Output;

    /// Offloads the work to
    /// the kernal via a
    /// kqueue syscall
    fn offload(&self) {

    }
}

impl Task for SleepTask {
    type Output = ();

    fn execute(&self) -> Self::Output {
        if self.sleep_for > SLEEP_TOLERANCE {
            self.offload();
        } else {
            thread::sleep(self.sleep_for);
            println!("Slept")
        }
    }
}
