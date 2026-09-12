//! # Signal
//! The constructors every signal task is started from, and the
//! signals they name

use crate::{
    RuntimeError,
    futures::signal::{
        dispatch,
        signal_task::{SendSignalTask, SignalTask},
    },
};

/// Which signal a task waits for or sends
///
/// The usual ones are named. `Other` takes a number, for a signal
/// this list doesn't cover
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalKind {
    /// `SIGINT`, which is what Ctrl-C sends
    Interrupt,

    /// `SIGTERM`, the polite request to shut down
    Terminate,

    /// `SIGHUP`, which a terminal sends when it goes, and which
    /// daemons usually take as "reload your configuration"
    Hangup,

    /// `SIGQUIT`, a harder stop than `Terminate`
    Quit,

    /// `SIGUSR1`, which means whatever the program decides
    User1,

    /// `SIGUSR2`, the same
    User2,

    /// `SIGCHLD`, sent when a child process ends
    ///
    /// #### Note
    /// Process tasks wait for their own children, and watching this
    /// doesn't disturb them
    Child,

    /// `SIGWINCH`, sent when the terminal window is resized
    WindowChange,

    /// Any other signal, by number
    Other(libc::c_int),
}

impl SignalKind {
    /// The number the kernel knows this signal by
    pub fn number(self) -> libc::c_int {
        match self {
            Self::Interrupt => libc::SIGINT,
            Self::Terminate => libc::SIGTERM,
            Self::Hangup => libc::SIGHUP,
            Self::Quit => libc::SIGQUIT,
            Self::User1 => libc::SIGUSR1,
            Self::User2 => libc::SIGUSR2,
            Self::Child => libc::SIGCHLD,
            Self::WindowChange => libc::SIGWINCH,
            Self::Other(signo) => signo,
        }
    }

    /// Which signal a number is
    ///
    /// One this list doesn't name comes back as `Other`
    pub fn from_number(signo: libc::c_int) -> Self {
        match signo {
            libc::SIGINT => Self::Interrupt,
            libc::SIGTERM => Self::Terminate,
            libc::SIGHUP => Self::Hangup,
            libc::SIGQUIT => Self::Quit,
            libc::SIGUSR1 => Self::User1,
            libc::SIGUSR2 => Self::User2,
            libc::SIGCHLD => Self::Child,
            libc::SIGWINCH => Self::WindowChange,
            signo => Self::Other(signo),
        }
    }
}

/// What happens to a watched signal once nothing is watching it
///
/// Set with [`SignalTask::release_policy`], and `Hold` without it
///
/// [`SignalTask::release_policy`]: crate::SignalTask::release_policy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SigReleasePolicy {
    /// Keeps the signal for the life of the program
    ///
    /// Nothing hands it back except [`Signal::release`], so a
    /// Ctrl-C can never start killing the program again at a moment
    /// nobody chose
    #[default]
    Hold,

    /// Hands the signal back once the last task watching it this way
    /// is gone
    ///
    /// A signal anything else asked to `Hold` is kept regardless
    OnDrop,
}

/// Waits for signals sent to this program, and sends them to
/// others
///
/// It doesn't implement `Task`, so a method has to be called on it
/// to get something that does
///
/// ## Behaviour
/// [`Signal::wait`] settles when its signal next arrives, saying
/// how many arrived. A `.repeat()` of one is a handler: it reports
/// every delivery, including any that land between runs
///
/// ```ignore
/// // Ctrl-C, once
/// Runtime::block(Signal::wait(SignalKind::Interrupt))?;
///
/// // every reload request, for as long as the program runs
/// let reloads = Runtime::task(Signal::wait(SignalKind::Hangup)).repeat().spawn();
/// ```
///
/// ## Taking a signal over
/// A signal that isn't watched does what it normally does, which
/// for most of them is to end the program. So **watching one takes
/// it over**: Ctrl-C stops killing the program, and starts waking
/// tasks instead. That lasts for the life of the program, unless
/// the task asked for [`SigReleasePolicy::OnDrop`] or something
/// calls [`Signal::release`]
///
/// `SIGKILL` and `SIGSTOP` can't be taken over at all, and say so
/// with [`RuntimeError::BadSignal`]
///
/// ## Waiting
/// A spawned wait holds no thread, the same as a socket task, so
/// any number of them can wait at once. Timeouts and cancelling
/// work as they do everywhere else
///
/// #### Note
/// The count is of **deliveries**, not of sends. A signal sent
/// while the same one is already waiting to be delivered is dropped
/// by the kernel, so two sends can be one delivery. Signals that
/// arrive faster than a task is woken also merge into one wake,
/// which is why a wait answers with a count rather than just
/// happening
///
/// [`RuntimeError::BadSignal`]: crate::RuntimeError::BadSignal
pub struct Signal;

impl Signal {
    /// Waits for the next `kind` sent to this program
    ///
    /// ## Behaviour
    /// Takes the signal over when the task first runs, and counts
    /// from that moment: one that arrived beforehand is not waited
    /// for
    ///
    /// ## Returns
    /// How many arrived, which is one unless several landed before
    /// the task could be woken
    pub fn wait(kind: SignalKind) -> SignalTask {
        SignalTask::new(kind)
    }

    /// Sends `kind` to the process `pid`
    ///
    /// ## Behaviour
    /// One process only. A `pid` of zero or less means a whole
    /// process group, or every process this user can reach, so it is
    /// refused as [`RuntimeError::BadArgument`]
    ///
    /// ## Returns
    /// Nothing, once the kernel has taken it. A process that isn't
    /// there is `CheckError(Some(ESRCH))`, and one this user may not
    /// signal `EPERM`
    ///
    /// #### Note
    /// One syscall, so spawning it costs more than doing it.
    /// `Runtime::block(Signal::send(..))` is usually the better call
    ///
    /// [`RuntimeError::BadArgument`]: crate::RuntimeError::BadArgument
    pub fn send(pid: libc::pid_t, kind: SignalKind) -> SendSignalTask {
        SendSignalTask::new(pid, kind)
    }

    /// Hands a signal back to whatever it normally does
    ///
    /// ## Behaviour
    /// Undoes a takeover, however it was asked for, so the next
    /// delivery does what it would have done before anything
    /// watched it
    ///
    /// ## Returns
    /// Nothing, or `BadSignal` for a signal that could never have
    /// been taken over
    ///
    /// #### Note
    /// A task still waiting on the signal keeps waiting, and nothing
    /// will wake it: the next delivery goes to the signal's own
    /// behaviour instead. Release a signal once nothing is watching
    /// it
    pub fn release(kind: SignalKind) -> Result<(), RuntimeError> {
        dispatch::release(kind.number())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every named signal survives the trip to a number and back
    #[test]
    fn a_named_signal_round_trips() {
        let named = [
            SignalKind::Interrupt,
            SignalKind::Terminate,
            SignalKind::Hangup,
            SignalKind::Quit,
            SignalKind::User1,
            SignalKind::User2,
            SignalKind::Child,
            SignalKind::WindowChange,
        ];

        for kind in named {
            assert_eq!(SignalKind::from_number(kind.number()), kind);
        }

        assert_eq!(SignalKind::Interrupt.number(), libc::SIGINT);
        assert_eq!(SignalKind::Other(libc::SIGPIPE).number(), libc::SIGPIPE);
    }

    /// A signal the list doesn't name keeps its number
    #[test]
    fn an_unnamed_signal_keeps_its_number() {
        assert_eq!(SignalKind::from_number(libc::SIGALRM), SignalKind::Other(libc::SIGALRM));
    }

    /// Releasing something that was never a signal says so
    #[test]
    fn releasing_a_non_signal_is_a_bad_signal() {
        assert_eq!(
            Signal::release(SignalKind::Other(0)),
            Err(RuntimeError::BadSignal),
        );
    }
}
