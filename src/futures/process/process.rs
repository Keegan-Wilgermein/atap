//! # Process
//! Tasks that run other programs

use crate::futures::process::process_task::{OutputTask, StatusTask};
use std::ffi::OsStr;

/// Runs other programs
///
/// ## Behaviour
/// [`Process::run`] lets a child's output through to wherever
/// this program's own output goes and reports only how it
/// ended. [`Process::output`] pipes both streams and collects
/// them
///
/// Every task here runs on a sleep thread. Past `cores * 8`
/// of them, children queue rather than running at once
///
/// ## Settings
/// `input` feeds a child's standard input, `in_dir` starts it
/// somewhere else, and `env` or `env_only` decide what
/// environment it gets. Each keeps the last value it was given
///
/// ```ignore
/// Runtime::task(
///     Process::output("/bin/sh", ["-c", "cat; pwd"])
///         .input(b"fed\n".as_slice())
///         .in_dir("/usr")
///         .env([("V", "set")]),
/// )
/// .spawn();
/// ```
///
/// ## Cancellation
/// A cancelled task kills its child's whole process group with
/// `SIGKILL`, reaps it, and gives back
/// [`RuntimeError::Cancelled`]
///
/// [`Runtime::block`] cannot be cancelled, so a blocking call
/// on a child that never ends holds the calling thread for as
/// long as the child lives
///
/// #### Note
/// An error after the child started kills the child
///
/// [`RuntimeError::Cancelled`]: crate::RuntimeError::Cancelled
/// [`Runtime::block`]: crate::Runtime::block
pub struct Process;

impl Process {
    /// An empty argument list, for a program that takes none
    ///
    /// ```ignore
    /// Process::run("/bin/date", Process::NO_ARGS)
    /// ```
    pub const NO_ARGS: [&'static str; 0] = [];

    /// Runs a program and waits for it to finish
    ///
    /// ## Behaviour
    /// The child keeps this process's standard output and
    /// standard error. Nothing is captured
    ///
    /// Its standard input is `/dev/null` unless
    /// [`StatusTask::input`] gave it something, and never
    /// inherited
    ///
    /// The program is looked up in `PATH` when it has no slash
    /// in it, so both `"ls"` and `"/bin/ls"` work
    ///
    /// ## Returns
    /// How the child ended, as a code or the signal that killed
    /// it. **A child that ran and failed is not an error.** It
    /// comes back as an [`ExitStatus`] reporting a non zero code
    ///
    /// #### Note
    /// The program is handed its own name as its first argument
    /// automatically. Pass only the arguments that come after it,
    /// or it arrives twice
    ///
    /// [`ExitStatus`]: crate::ExitStatus
    pub fn run<S, A, I>(program: S, args: A) -> StatusTask
    where
        S: AsRef<OsStr>,
        A: IntoIterator<Item = I>,
        I: AsRef<OsStr>,
    {
        StatusTask::new(program, args)
    }

    /// Runs a program and collects everything it wrote
    ///
    /// ## Behaviour
    /// Both streams are piped and read together, along with
    /// anything [`OutputTask::input`] feeds in, so a child that
    /// fills a pipe never deadlocks against this side
    ///
    /// ## Returns
    /// Both streams and how the child ended. The bytes are
    /// everything the child wrote, not a prefix
    ///
    /// #### Note
    /// The whole of both streams lands in memory at once. A
    /// program whose output has no bound is one to run with
    /// [`Process::run`] and a redirect
    pub fn output<S, A, I>(program: S, args: A) -> OutputTask
    where
        S: AsRef<OsStr>,
        A: IntoIterator<Item = I>,
        I: AsRef<OsStr>,
    {
        OutputTask::new(program, args)
    }
}
