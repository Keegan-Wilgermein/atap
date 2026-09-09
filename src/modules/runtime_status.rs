//! # Runtime Status
//! Whether the runtime is up, and whether the two threads that
//! keep it adapting are still there

use std::fmt;

/// What the runtime looked like at the moment it was asked
///
/// ## Why the parts are separate
/// The `Reactor` and the manager are supervised independently
/// and fail independently, and losing one is not the same as
/// losing the other. A manager that has given up costs the pool
/// its growing, reaping and rebalancing, and ends every
/// schedule in the process — a `Reactor` that has given up
/// costs a task the way back from the kernel. Folding them into
/// one flag would throw away the difference
///
/// #### Note
/// A snapshot rather than a lock, like `PoolStats`. Every field
/// was true when it was read
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeStatus {
    /// Whether `Runtime::init` has finished
    ///
    /// Says initialisation is over rather than that it worked.
    /// A failed init is a finished one, and the two flags below
    /// are what say whether anything came of it
    pub initialised: bool,

    /// Whether the runtime has been shut down
    ///
    /// One way. Nothing brings a runtime back after this
    pub shut_down: bool,

    /// Whether the `Reactor` still has a queue to watch
    ///
    /// False once its supervisor has stopped bringing it back
    pub reactor_alive: bool,

    /// Whether the manager still has a queue to work from
    ///
    /// False once its supervisor has given up on it, and once
    /// the runtime has been shut down
    ///
    /// #### Note
    /// Tasks still run without it. The pool finds its own work,
    /// reverses its own queue and clears up after its own dead
    /// whether anything is supervising it or not — what stops
    /// is the pool *adapting*, and every kind of repeat that
    /// leans on a timer
    pub manager_alive: bool,
}

impl RuntimeStatus {
    /// Whether everything is up and nothing has given up
    ///
    /// #### Note
    /// `false` is not the same as broken. A runtime with no
    /// manager still runs everything spawned onto it, so this
    /// is the question of whether anything has been lost rather
    /// than whether anything works
    pub fn healthy(&self) -> bool {
        self.initialised && !self.shut_down && self.reactor_alive && self.manager_alive
    }
}

impl fmt::Display for RuntimeStatus {
    /// One line, saying the state first and what is missing
    /// after it
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.initialised {
            return write!(formatter, "not initialised");
        }

        if self.shut_down {
            return write!(formatter, "shut down");
        }

        if self.healthy() {
            return write!(formatter, "healthy");
        }

        write!(
            formatter,
            "degraded: reactor {}, manager {}",
            alive(self.reactor_alive),
            alive(self.manager_alive),
        )
    }
}

/// A flag as the word for it
#[inline(always)]
fn alive(alive: bool) -> &'static str {
    match alive {
        true => "up",
        false => "gone",
    }
}
