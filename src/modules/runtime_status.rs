//! # Runtime Status
//! Whether the runtime is up, and whether the two threads that
//! keep it adapting are still there

use std::fmt;

/// What the runtime looked like at the moment it was asked
///
/// #### Note
/// A snapshot, not a lock. Every value was true when it was read
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RuntimeStatus {
    initialised: bool,
    shut_down: bool,
    reactor_alive: bool,
    manager_alive: bool,
}

impl RuntimeStatus {
    pub(crate) fn new(
        initialised: bool,
        shut_down: bool,
        reactor_alive: bool,
        manager_alive: bool,
    ) -> Self {
        Self {
            initialised,
            shut_down,
            reactor_alive,
            manager_alive,
        }
    }

    /// Whether `Runtime::init` has finished
    ///
    /// Says initialisation is over, not that it worked
    pub fn initialised(&self) -> bool {
        self.initialised
    }

    /// Whether the runtime has been shut down
    ///
    /// Stays true until `Runtime::init` starts it again
    pub fn shut_down(&self) -> bool {
        self.shut_down
    }

    /// Whether the `Reactor` is still running
    ///
    /// False once its supervisor stops restarting it
    pub fn reactor_alive(&self) -> bool {
        self.reactor_alive
    }

    /// Whether the manager is still running
    ///
    /// False once its supervisor gives up on it, or once the
    /// runtime has been shut down
    ///
    /// #### Note
    /// Spawned tasks still run without it. What stops is the pool
    /// growing and shrinking, and every repeat driven by a timer
    pub fn manager_alive(&self) -> bool {
        self.manager_alive
    }

    /// Whether everything is up and nothing has given up
    ///
    /// #### Note
    /// `false` doesn't mean spawned work has stopped running
    pub fn healthy(&self) -> bool {
        self.initialised && !self.shut_down && self.reactor_alive && self.manager_alive
    }
}

impl fmt::Display for RuntimeStatus {
    /// One line: the state, then anything that is down
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

#[inline(always)]
fn alive(alive: bool) -> &'static str {
    match alive {
        true => "up",
        false => "gone",
    }
}
