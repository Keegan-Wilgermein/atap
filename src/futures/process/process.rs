//! # Process
//! Tasks that run other programs
//!
//! Every task starts at a constructor here, and the reasons a
//! call behaves the way it does are written on it.
//! [`process_task`] is what those constructors return and what
//! actually runs
//!
//! [`process_task`]: crate::futures::process::process_task

use crate::futures::process::process_task::{OutputTask, StatusTask};
use std::ffi::OsStr;

/// Runs other programs
///
/// ## Behaviour
/// Two constructors, split by what they hand back.
/// [`Process::run`] lets a child's output through to wherever
/// this program's own output goes and reports only how it
/// ended; [`Process::output`] pipes both streams and collects
/// them
///
/// Every task here blocks, and goes to a sleep thread rather
/// than a worker. The pool is eight threads a core, so a burst
/// of children larger than that queues rather than running at
/// once — which is a throughput ceiling, not a deadlock
///
/// ## Cancellation
/// A cancelled task kills its child — the whole process group,
/// so a shell takes what it started with it — reaps it, and
/// gives back [`RuntimeError::Cancelled`]. There is no grace
/// period and no `SIGTERM` first, because an escalation needs a
/// deadline and this crate has no notion of one
///
/// [`Runtime::block`] cannot be cancelled, which is the promise
/// it already makes for everything else. For a process that is
/// a much longer rope than it is for a sleep: a blocking call
/// on a child that never ends holds the calling thread for as
/// long as the child lives, with nothing able to reach it.
/// Spawn rather than block for anything whose running time
/// isn't yours to know
///
/// #### Note
/// An error *after* the child started kills the child. A failed
/// read or a refused registration leaves nothing running, which
/// is the only honest option — the caller was never given a
/// handle it could use to end the thing itself
///
/// [`RuntimeError::Cancelled`]: crate::RuntimeError::Cancelled
/// [`Runtime::block`]: crate::Runtime::block
pub struct Process;

impl Process {
    /// An empty argument list
    ///
    /// ## Behaviour
    /// A bare `[]` has no element type for the compiler to work
    /// out, so a program that takes no arguments has nowhere
    /// obvious to say so. This is that place
    ///
    /// ```ignore
    /// Process::run("/bin/date", Process::NO_ARGS)
    /// ```
    pub const NO_ARGS: [&'static str; 0] = [];

    /// Runs a program and waits for it to finish
    ///
    /// ## Behaviour
    /// The child keeps this process's standard output and
    /// standard error, so anything it writes goes wherever this
    /// program's own output goes. Nothing is captured and
    /// nothing is read, which is what makes this the cheap one
    ///
    /// Its standard input is `/dev/null` rather than inherited.
    /// A child reading a terminal this process is also reading
    /// would be taking input meant for the program that spawned
    /// it, and one reading a terminal that isn't there would
    /// simply never finish
    ///
    /// The program is looked up in `PATH` when it has no slash
    /// in it, the same as a shell would, so both `"ls"` and
    /// `"/bin/ls"` work
    ///
    /// ## Returns
    /// How the child ended — a code, or the signal that killed
    /// it. **A child that ran and failed is not an error.** It
    /// comes back as an [`ExitStatus`] reporting a non zero
    /// code, and the error case is not being able to run it at
    /// all
    ///
    /// #### Note
    /// The program is handed its own name as its first argument
    /// automatically, the way a shell hands it one. Pass only
    /// the arguments that come after it, or it arrives twice
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
    /// Both streams are piped, and both are read to their end
    /// *before* the child's exit is waited on. That order is the
    /// whole design: a pipe holds 64 KiB, and a child that fills
    /// one blocks in `write` until somebody reads it — so a
    /// parent waiting for the exit first would be waiting for a
    /// child that is waiting for the parent
    ///
    /// The two streams are also read together rather than one
    /// after the other, for the same reason in miniature.
    /// Reading stdout to its end while stderr fills up deadlocks
    /// exactly as thoroughly
    ///
    /// ## Returns
    /// Both streams and how the child ended. The bytes are
    /// everything the child wrote, not a prefix — nothing is
    /// truncated and nothing is dropped
    ///
    /// #### Note
    /// The whole of both streams lands in memory at once, so a
    /// child that writes a great deal is a large allocation. A
    /// program whose output has no bound is a program to run
    /// with [`Process::run`] and a redirect, not this
    pub fn output<S, A, I>(program: S, args: A) -> OutputTask
    where
        S: AsRef<OsStr>,
        A: IntoIterator<Item = I>,
        I: AsRef<OsStr>,
    {
        OutputTask::new(program, args)
    }
}
