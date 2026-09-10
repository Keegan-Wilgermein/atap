//! # Event Descriptor
//! Describes events for creating `kevent` calls

/// A `kevent` description
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventDesc {
    pub(crate) filter: i16,
    pub(crate) flags: u16,
    pub(crate) fflags: u32,
}

impl EventDesc {
    /// Creates a new custom `EventDesc`
    pub fn new(filter: i16, flags: u16, fflags: u32) -> Self {
        Self {
            filter,
            flags,
            fflags,
        }
    }

    /// Returns the `kevent` flags
    /// required to make a new timer
    pub(crate) fn new_timer() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_NSECONDS | libc::NOTE_CRITICAL,
        }
    }

    /// Returns the `kevent` flags required to take a
    /// timer back off a queue
    ///
    /// Used to cancel a sleep that hasn't fired yet. Without
    /// it the timer stays armed and goes off into a queue
    /// nobody is waiting on it in any more
    pub(crate) fn new_timer_delete() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to make
    /// a timer that keeps firing
    ///
    /// Registered once and left alone, unlike `new_timer`,
    /// which is armed for a single shot and dropped by the
    /// kernel once it has been delivered
    ///
    /// #### Note
    /// `NOTE_CRITICAL` is deliberately absent. This drives
    /// the manager's policy pass, which does not care about
    /// microseconds and has no business asking the kernel to
    /// treat it as though it does
    pub(crate) fn new_interval() -> Self {
        Self {
            filter: libc::EVFILT_TIMER,
            flags: libc::EV_ADD | libc::EV_ENABLE,
            fflags: libc::NOTE_NSECONDS,
        }
    }

    /// Returns the `kevent` flags required to
    /// raise a user triggered event
    ///
    /// `EVFILT_USER` is the filter that exists purely so
    /// userspace can wake a kqueue on demand, rather than
    /// waiting on a timer or a descriptor
    ///
    /// #### Note
    /// Carrying `NOTE_TRIGGER` on the `EV_ADD` registers the
    /// event and fires it in the same syscall, so waking the
    /// `Executor` costs one call rather than two
    pub(crate) fn new_user_trigger() -> Self {
        Self {
            filter: libc::EVFILT_USER,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_TRIGGER,
        }
    }

    /// Returns the `kevent` flags required to wait
    /// for a child process to exit
    ///
    /// Registered at the child's pid rather than at a task id,
    /// because the pid is what the filter identifies a process
    /// by. Nothing else in the crate registers at an ident it
    /// didn't choose itself
    ///
    /// #### Note
    /// `NOTE_EXITSTATUS` is deliberately absent. The status has
    /// to be collected with `waitpid` anyway, since a child
    /// that is never reaped is a zombie for the life of the
    /// process, so reading it off the event as well would be a
    /// second source for something there is already one of
    pub(crate) fn new_proc_exit() -> Self {
        Self {
            filter: libc::EVFILT_PROC,
            flags: libc::EV_ADD | libc::EV_ONESHOT,
            fflags: libc::NOTE_EXIT,
        }
    }

    /// Returns the `kevent` flags required to take a
    /// process watch back off a queue
    ///
    /// ## Behaviour
    /// Removes the registration *and* anything it has already
    /// queued, which is the half that matters. A cancelled task
    /// leaves an exit note nobody read sitting on a per thread
    /// queue, and pids are reused — so a later task on the same
    /// thread could find it and take it for its own child
    pub(crate) fn new_proc_delete() -> Self {
        Self {
            filter: libc::EVFILT_PROC,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to watch a
    /// descriptor for something to read
    ///
    /// ## Behaviour
    /// Level triggered, which is the whole point. `EV_CLEAR`
    /// reports the edge and leaves the caller to read until it
    /// would block, which needs a non blocking descriptor and a
    /// loop around `EAGAIN`. Without it the kernel reports the
    /// descriptor ready for as long as it has anything on it,
    /// so one ordinary blocking `read` per wake is safe and
    /// nothing has to be `O_NONBLOCK`
    ///
    /// #### Note
    /// The cost of that choice is `new_read_delete`, which
    /// stops being optional the moment a descriptor hits its
    /// end. See the note there
    pub(crate) fn new_read() -> Self {
        Self {
            filter: libc::EVFILT_READ,
            flags: libc::EV_ADD,
            fflags: 0,
        }
    }

    /// Returns the `kevent` flags required to take a
    /// read watch back off a queue
    ///
    /// #### Note
    /// Not a tidy up. A descriptor at its end is *readable* as
    /// far as a level triggered filter is concerned — the read
    /// that returns zero returns immediately, every time — so
    /// leaving it registered turns a loop still waiting on
    /// another descriptor into a spin
    pub(crate) fn new_read_delete() -> Self {
        Self {
            filter: libc::EVFILT_READ,
            flags: libc::EV_DELETE,
            fflags: 0,
        }
    }
}
