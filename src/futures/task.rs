//! # Task
//! A trait that defines a task that can be
//! initialised and run asynchronously
//!
//! Definitions only. Every implementor lives beside the type
//! it is implemented for — `SleepTask` in `sleep_task`, the
//! file tasks in `file/file_task` — so a reader who wants to
//! know what a task *does* is never sent here, and a reader
//! who wants to know what a task *is* is never sent anywhere
//! else

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
/// ## Behaviour
/// A task runs to completion. `execute` is called once, on one
/// thread, and whatever it returns is the output — there is no
/// way to hand back part of an answer and be called again,
/// because a native stack can't be put down half way through a
/// call and picked up elsewhere
///
/// That is the whole reason `blocking` exists. A task that
/// waits holds its thread for as long as it waits, so the only
/// choice the runtime has is which thread to hold
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
    ///
    /// #### Note
    /// Defaulted, because most tasks are immutable input and
    /// have nothing to set up — a path and some bytes are the
    /// same on the tenth run as on the first. Override it when
    /// a run would otherwise start with the last one's state,
    /// which is what a repeat makes possible: the same box goes
    /// back into the same slot, so anything left in it carries
    fn prepare(&mut self) {}

    /// Whether running this will hold the thread long enough
    /// to be worth giving it to a thread that exists to be held
    ///
    /// ## Behaviour
    /// A spawned task that says yes is handed to a sleep thread
    /// rather than run on a worker, so the worker goes straight
    /// back to the queue instead of sitting inside a syscall
    /// for the duration
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
    ///
    /// Asked once, at spawn, and kept in the slot. A re arm has
    /// no concrete type left to ask
    #[inline(always)]
    fn blocking(&self) -> bool {
        false
    }
}
