//! # Signal task
//! The tasks the `Signal` constructors return, and everything
//! they do once run
//!
//! A wait parks on the signal the same way a socket task parks on
//! a descriptor, so a spawned one holds no thread

use crate::modules::input::Token;
use crate::{
    RuntimeError,
    constants::{INLINE_PAYLOAD, SIGNAL_POLL},
    futures::{
        net::step::settle,
        signal::{
            dispatch::{self, Watcher},
            signal::{SignalKind, SignalReleasePolicy},
        },
        task::{
            Nothing, Task,
            sealed::{self, Park, Step},
        },
    },
    modules::{int_check::IntCheck, park},
};
use std::{mem, time::Instant};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<u32, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<(), RuntimeError>>() <= INLINE_PAYLOAD);

/// Waits for a signal sent to this program
///
/// ## Returns
/// How many arrived since this task last reported one
///
/// ## Behaviour
/// A `.repeat()` of one loses nothing: each run carries on counting
/// from where the last stopped, so a signal that lands between runs
/// is still reported
///
/// #### Note
/// `.at_rate()` is the exception. Every run there is a fresh copy,
/// and each copy counts from where the original started, so runs
/// report the same deliveries over again. Use `.repeat()` for a
/// stream of signals
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct SignalTask {
    /// Which signal to wait for
    kind: SignalKind,

    /// What happens to it once nothing watches it
    policy: SignalReleasePolicy,

    /// The count this task last reported, and `None` before it has
    /// ever run
    ///
    /// Kept across runs, so a repeat loses nothing
    seen: Option<u32>,

    /// The claim on the signal, for a task that wants it handed back
    watch: Option<Watcher>,
}

impl SignalTask {
    /// Waits for `kind`
    pub(crate) fn new(kind: SignalKind) -> Self {
        Self {
            kind,
            policy: SignalReleasePolicy::default(),
            seen: None,
            watch: None,
        }
    }

    /// Decides what happens to the signal once nothing is watching
    /// it
    ///
    /// ## Behaviour
    /// [`SignalReleasePolicy::Hold`], the default, keeps the signal
    /// taken over for the life of the program.
    /// [`SignalReleasePolicy::OnDrop`] hands it back once this task and
    /// every copy of it are gone
    ///
    /// A signal anything else asked to hold stays held either way
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last, as long as it is
    /// set before the task runs: the first run is what takes the
    /// signal over
    pub fn release_policy(mut self, policy: SignalReleasePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Looks at the count, and parks if nothing new has arrived
    fn advance(&mut self) -> Result<Step<Result<u32, RuntimeError>>, RuntimeError> {
        let signo = self.kind.number();

        // The first run takes the signal over, and starts counting
        // from that moment
        if self.seen.is_none() {
            self.watch = dispatch::catch(signo, self.policy)?;
            self.seen = Some(dispatch::count(signo));
        }

        let seen = self.seen.unwrap_or_default();
        let arrived = dispatch::count(signo);

        // Wrapping
        if arrived != seen {
            self.seen = Some(arrived);

            return Ok(Step::Done(Ok(arrived.wrapping_sub(seen))));
        }

        // A delivery landing before the watch goes on wakes nothing, so
        // the backstop bounds the wait
        let deadline = Instant::now() + SIGNAL_POLL;

        Ok(Step::Park(Park {
            ident: signo,
            filter: libc::EVFILT_SIGNAL,
            notes: 0,
            deadline: Some(deadline),
        }))
    }
}

/// Sends a signal to another process
///
/// ## Returns
/// Nothing, once the kernel has taken it
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct SendSignalTask {
    /// Who to send it to
    pid: libc::pid_t,

    /// What to send
    kind: SignalKind,
}

impl SendSignalTask {
    /// Sends `kind` to `pid`
    pub(crate) fn new(pid: libc::pid_t, kind: SignalKind) -> Self {
        Self { pid, kind }
    }

    /// Sends it
    fn send(&self) -> Result<(), RuntimeError> {
        // A group, or everything this user can reach, is not something
        // to do by accident
        if self.pid <= 0 {
            return Err(RuntimeError::BadArgument);
        }

        let signo = self.kind.number();
        dispatch::sendable(signo)?;

        unsafe { libc::kill(self.pid, signo) }.check()?;

        Ok(())
    }
}

impl sealed::Sealed for SignalTask {}
impl sealed::Sealed for SendSignalTask {}

impl Task for SignalTask {
    type Output = Result<u32, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for SendSignalTask {
    type Output = Result<(), RuntimeError>;
    type Input = Nothing;

    /// One syscall, so this is the whole task
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.send()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A signal is only ever sent to one process
    #[test]
    fn a_group_is_not_a_process() {
        for pid in [0, -1, -42] {
            assert_eq!(
                SendSignalTask::new(pid, SignalKind::Terminate).send(),
                Err(RuntimeError::BadArgument),
            );
        }
    }

    /// Something that was never a signal can't be sent either
    #[test]
    fn a_non_signal_cannot_be_sent() {
        let me = std::process::id() as libc::pid_t;

        assert_eq!(
            SendSignalTask::new(me, SignalKind::Other(0)).send(),
            Err(RuntimeError::BadSignal),
        );
    }
}
