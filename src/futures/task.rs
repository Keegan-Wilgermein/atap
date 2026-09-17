//! # Task
//! The trait every task implements, and the input of a task that
//! takes none

use crate::modules::input::Token;

/// Stops `Task` being implemented outside the crate
pub(crate) mod sealed {
    use std::time::Instant;

    /// Implemented for every type this crate allows as a task
    pub trait Sealed {}

    /// How far one step of a task got
    pub enum Step<T> {
        /// The run is over, and this is its output
        Done(T),

        /// The run is waiting on something the kernel will report,
        /// and can give its thread back until it does
        Park(Park),
    }

    /// What a parked task is waiting for
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct Park {
        /// What to watch, which is whatever the filter takes: a
        /// descriptor for a socket, a signal number for a signal
        pub ident: libc::c_int,

        /// Which `EVFILT_` decides what counts as ready
        pub filter: i16,

        /// Which of the filter's notes count, for a filter that
        /// fires on nothing until it is told what to look for
        ///
        /// Zero for the filters that already know: a socket is
        /// ready or it isn't
        pub notes: u32,

        /// When to wake it anyway, so it can give up
        pub deadline: Option<Instant>,
    }
}

/// The input of a task that takes nothing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Nothing(pub(crate) ());

/// Implemented by everything the runtime can run
#[allow(private_bounds, private_interfaces)]
pub trait Task: sealed::Sealed + Send + 'static {
    /// The final output type
    type Output: Send + 'static;

    /// What a run is handed before it starts
    ///
    /// [`Nothing`] for every task that makes its own way. A compute
    /// takes whatever its closure does
    type Input: Send + 'static;

    /// Runs the task and returns its output
    #[doc(hidden)]
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output;

    /// Resets any state before a run
    ///
    /// Called before every `execute`, including every run of a
    /// repeat, which reuses the same task
    #[doc(hidden)]
    fn prepare(&mut self, _token: Token) {}

    /// Whether this task holds its thread long enough to be run on
    /// a sleep thread instead of a worker
    ///
    /// Asked once, at spawn. `Runtime::block` ignores it
    #[doc(hidden)]
    #[inline(always)]
    fn blocking(&self, _token: Token) -> bool {
        false
    }

    /// Hands the task the input its runs use
    ///
    /// Only a task that takes input keeps it
    #[doc(hidden)]
    #[inline(always)]
    fn give(&mut self, _token: Token, _input: Self::Input) {}

    /// Runs as much of the task as can be done without waiting
    ///
    /// ## Behaviour
    /// What a spawned run calls instead of `execute`. A task that
    /// waits on a socket parks instead, and is stepped again once
    /// the socket is ready, without `prepare` in between. Anything
    /// else finishes in one step
    #[doc(hidden)]
    #[inline(always)]
    fn step(
        &mut self,
        token: Token,
        reactor_id: i32,
        task_id: usize,
    ) -> sealed::Step<Self::Output> {
        sealed::Step::Done(self.execute(token, reactor_id, task_id))
    }
}
