//! # Process task
//! The tasks the `Process` constructors return, and everything
//! they do once a thread picks them up
//!
//! Two types rather than one with a mode, grouped by what they
//! hand back. A caller who wanted an exit code gets an exit
//! code and nothing else to reach past, and a caller who wanted
//! the output gets both streams — the same split the file tasks
//! are built on
//!
//! Both spend their whole run inside the kernel: first waiting
//! for a child to write something, then waiting for it to end.
//! What makes that safe to do on a sleep thread is that every
//! wait is on a kqueue, which a cancel can reach into

use crate::{
    EventDesc, RuntimeError,
    constants::{FILE_CHUNK, INLINE_PAYLOAD, PROCESS_POLL},
    executor,
    futures::{
        process::exit_status::{ExitStatus, ProcessOutput},
        task::Task,
        task::sealed,
    },
    modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        kqueue,
        wake_target::WakeTarget,
    },
};
use std::{
    ffi::{CStr, CString, OsStr},
    mem,
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr,
    sync::{Arc, OnceLock},
};

// The line an output crosses at the cost of a page mapping per
// task rather than an allocation. `ProcessOutput` is two `Vec`s
// and a word precisely so it stays on this side of it
const _: () = assert!(mem::size_of::<Result<ExitStatus, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<ProcessOutput, RuntimeError>>() <= INLINE_PAYLOAD);

/// What a `Child` holds instead of a pid once it has
/// been reaped
///
/// A pid is only ours between the spawn and the wait. After the
/// wait the number belongs to the kernel again and may already
/// be somebody else's process, so a guard that still held it
/// would signal a stranger
const NO_CHILD: libc::pid_t = -1;

/// What `poll` is given in place of a descriptor it should
/// leave alone
///
/// A negative descriptor is skipped, which is how a stream that
/// has already reached its end is dropped out of the set
/// without changing the shape of the array around it
const IGNORED: libc::c_int = -1;

/// Asks a descriptor not to raise `SIGPIPE` when its reader
/// goes away
///
/// Not in `libc`'s bindings for this platform — only the socket
/// option is — so the number is written out. `sys/fcntl.h` has
/// defined it as 73 since 10.5
const F_SETNOSIGPIPE: libc::c_int = 73;

/// Where a child's standard input comes from
///
/// Never inherited. A child that reads stdin would otherwise be
/// competing with the parent for a terminal nobody told it
/// about, and `cat` with no arguments would hang the task
/// forever rather than finishing empty
const DEV_NULL: &CStr = c"/dev/null";

/// The attributes every child is spawned with
///
/// ## Behaviour
/// `CLOEXEC_DEFAULT` is the one that matters, and not for the
/// reason it usually is. Several process tasks spawn at once on
/// different sleep threads, and without it one task's pipe can
/// be inherited by another task's child — after which the first
/// task's drain never sees an end, because somebody else's
/// child is holding the write end open
///
/// `SETSIGDEF` against a full set puts every signal back to its
/// default. This is really about `SIGPIPE`: the standard
/// library sets it to ignored at startup, an ignored
/// disposition survives an `exec`, and a child that inherited
/// it spins on `EPIPE` instead of dying when its reader goes
/// away
///
/// `SETSIGMASK` against an empty set does the same for the
/// mask, which is also inherited and which the crate's own
/// threads may have altered
///
/// `SETPGROUP` with a group of zero makes the child the leader
/// of its own group, which is what lets a cancel take the whole
/// tree rather than just the child. See `kill_and_reap`
///
/// #### Note
/// `c_short` rather than `c_int`, because that is what
/// `posix_spawnattr_setflags` takes. Every flag here fits
const SPAWN_FLAGS: libc::c_short = (libc::POSIX_SPAWN_CLOEXEC_DEFAULT
    | libc::POSIX_SPAWN_SETSIGDEF
    | libc::POSIX_SPAWN_SETSIGMASK
    | libc::POSIX_SPAWN_SETPGROUP) as libc::c_short;

/// A program and its arguments, in the form the kernel takes
///
/// ## Behaviour
/// Converted once, here, rather than on every run. A repeat
/// puts the same task back in the same slot, and converting the
/// same arguments again every period is work with a known
/// answer
///
/// #### Note
/// Shared by both task types rather than spelled out in each.
/// The two differ in what they hand back and what they do with
/// the child's streams, not in what they run
#[derive(Debug, Clone)]
struct Program {
    /// The program to run
    ///
    /// `None` when it had a zero byte in it and could not be
    /// converted, which is reported when the task runs
    file: Option<CString>,

    /// What to pass it, not counting the name
    ///
    /// `None` when any one of them had a zero byte
    ///
    /// #### Note
    /// `Arc<[CString]>` rather than `Vec<CString>` because
    /// `.at_rate()` clones the whole task once per run. A `Vec`
    /// would copy every argument every period to run the same
    /// command again
    args: Option<Arc<[CString]>>,
}

impl Program {
    /// Converts a program and its arguments once
    fn new<S, A, I>(program: S, args: A) -> Self
    where
        S: AsRef<OsStr>,
        A: IntoIterator<Item = I>,
        I: AsRef<OsStr>,
    {
        Self {
            file: as_c_arg(program),
            args: args.into_iter().map(as_c_arg).collect(),
        }
    }

    /// Builds the pointer array `posix_spawn` actually takes
    ///
    /// ## Behaviour
    /// Per run rather than kept on the struct. These are
    /// pointers into the `CString`s above, and a raw pointer is
    /// not `Send` — a task has to cross a thread to be run at
    /// all, so they could not live here even if the allocation
    /// were worth saving. Next to a spawn it isn't
    ///
    /// ## Returns
    /// The program, and an argument vector with the program's
    /// own name in front of it and a null on the end, which is
    /// the shape `execve` reads
    fn argv(&self, dir: Option<&CStr>) -> Result<(CString, Vec<*mut libc::c_char>), RuntimeError> {
        let file = self.file.as_ref().ok_or(RuntimeError::BadPath)?;
        let args = self.args.as_ref().ok_or(RuntimeError::BadArgument)?;

        let mut argv = Vec::with_capacity(args.len() + 2);

        // Every program is handed its own name as its first
        // argument. A caller passing one itself would find it
        // arriving twice, so the constructors take only the
        // arguments that come after it
        //
        // Left exactly as the caller wrote it, even when the
        // spawn below is given somewhere else to look. A child
        // is told its own name the way a shell would tell it,
        // and a path this crate assembled is not that name
        argv.push(file.as_ptr().cast_mut());
        argv.extend(args.iter().map(|arg| arg.as_ptr().cast_mut()));
        argv.push(ptr::null_mut());

        // A relative program is resolved here rather than left
        // to the platform, which gets it wrong in a way this
        // module can't recover from. See `join`
        let spawn_as = match dir {
            Some(dir) if relative(file) => join(dir, file)?,
            _ => file.clone(),
        };

        Ok((spawn_as, argv))
    }
}

/// Everything a child is configured with beyond the program
/// itself
///
/// ## Behaviour
/// One struct rather than three fields on each task type,
/// because the spawn wants to be handed the answer rather than
/// assemble it out of parts. The setters that fill it are
/// spelled out on each task instead of shared, since two of
/// them have genuinely different things to say
///
/// #### Note
/// Every field here is input, converted once. None of it is a
/// measurement, which is why neither task needs a `prepare`
#[derive(Debug, Clone, Default)]
struct Setup {
    /// What to feed the child, or `None` for `/dev/null`
    ///
    /// `Arc<[u8]>` rather than `Vec<u8>` because `.at_rate()`
    /// clones the whole task once per run — the same bargain
    /// `WriteTask` makes with the bytes it writes
    input: Option<Arc<[u8]>>,

    /// Where it starts
    dir: Dir,

    /// What it is given for an environment
    env: Env,
}

/// Where a child starts
///
/// ## Behaviour
/// Three states rather than an `Option` and a flag. A directory
/// that couldn't be used is a third answer and not an absent
/// one, and an `Option<Option<CString>>` would be two questions
/// stacked on top of each other with a name on neither
#[derive(Debug, Clone, Default)]
enum Dir {
    /// Wherever this process happens to be
    #[default]
    Inherited,

    /// A directory of its own, absolute
    At(CString),

    /// One with a zero byte in it, or one that wasn't absolute
    Bad,
}

/// What environment a child is handed
///
/// #### Note
/// Entries are kept pre-joined as `NAME=VALUE`, which is the
/// form the kernel takes, so the joining happens once at
/// construction rather than on every run — the same bargain
/// `Program` makes with its arguments
#[derive(Debug, Clone, Default)]
enum Env {
    /// This process's own, unchanged
    #[default]
    Inherited,

    /// This process's own, with these written over the top
    Over(Arc<[CString]>),

    /// Exactly these, and nothing else
    Only(Arc<[CString]>),

    /// One of them couldn't be passed on as written
    Bad,
}

/// Runs a program and waits for it to finish
///
/// ## Behaviour
/// The child keeps this process's standard output and standard
/// error, so whatever it writes goes wherever this program's
/// own output goes. Nothing is read and nothing is captured
///
/// ## Returns
/// How the child ended, which is a code or a signal. A child
/// that ran and failed is an [`ExitStatus`] saying so rather
/// than an error — the error case is not being able to run it
#[derive(Debug, Clone)]
pub struct StatusTask {
    /// What to run
    program: Program,

    /// Everything else about how it runs
    setup: Setup,
}

/// Runs a program and collects everything it wrote
///
/// ## Behaviour
/// Both streams are piped and read to their end before the exit
/// is waited on, so a child that writes more than a pipe holds
/// keeps going rather than blocking against a parent that isn't
/// listening yet
///
/// ## Returns
/// Both streams and how the child ended
#[derive(Debug, Clone)]
pub struct OutputTask {
    /// What to run
    program: Program,

    /// Everything else about how it runs
    setup: Setup,
}

impl StatusTask {
    /// Runs a program, letting its output through
    pub(crate) fn new<S, A, I>(program: S, args: A) -> Self
    where
        S: AsRef<OsStr>,
        A: IntoIterator<Item = I>,
        I: AsRef<OsStr>,
    {
        Self {
            program: Program::new(program, args),
            setup: Setup::default(),
        }
    }

    /// Gives the child something to read
    ///
    /// ## Behaviour
    /// The bytes go down a pipe on the child's standard input,
    /// and the pipe is closed once they have all been taken —
    /// which is the only way a child ever learns its input has
    /// ended. Without this a child reads `/dev/null` and sees
    /// an end immediately
    ///
    /// There is nothing to read back here, so this waits on
    /// room to write and on nothing else. It still waits on a
    /// queue rather than simply writing, because a child that
    /// never reads its input would otherwise have this thread
    /// stuck inside a `write` that no cancel could reach — and
    /// a sleep thread held by a task nobody wants any more is
    /// the one failure this module is shaped to avoid
    ///
    /// ## Returns
    /// The task, so this can be written in the middle of a
    /// call. Calling it twice keeps the last
    ///
    /// #### Note
    /// A child is allowed to stop reading before it has taken
    /// everything, the way `head` does, and that is not an
    /// error. How much it actually took is not reported —
    /// neither of the outputs on these tasks could carry the
    /// number, and adding one for it would cost every task the
    /// slot space
    pub fn input(mut self, data: impl Into<Arc<[u8]>>) -> Self {
        self.setup.input = Some(data.into());
        self
    }

    /// Starts the child somewhere else
    ///
    /// ## Behaviour
    /// The directory has to be **absolute**. Nothing in this
    /// crate calls `chdir`, but nothing in it can promise no
    /// other library will, and this process has workers, sleep
    /// threads, a reactor and a manager that never agree on
    /// what "here" means for longer than a scheduling quantum.
    /// A relative directory is a question read at one moment
    /// and used at another; an absolute one is the same
    /// directory whenever it is read
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// A relative one gives [`RuntimeError::BadDirectory`] when
    /// the task runs, rather than being resolved against
    /// wherever this process happens to be. One that simply
    /// isn't there is a different answer — the kernel goes
    /// looking for that one, and it comes back as `ENOENT`
    ///
    /// [`RuntimeError::BadDirectory`]: crate::RuntimeError::BadDirectory
    pub fn in_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.setup.dir = as_dir(path);
        self
    }

    /// Writes variables over the environment the child inherits
    ///
    /// ## Behaviour
    /// A merge rather than an addition. A name already in this
    /// process's environment is *replaced*, so the child sees
    /// it once — appending and trusting the child to read the
    /// first of two is not something this crate is willing to
    /// say, since POSIX leaves duplicates unspecified and
    /// plenty of programs walk the environment themselves
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last, rather than
    /// accumulating — two sets are a `.chain()` away, and a
    /// setter that a later [`StatusTask::env_only`] could
    /// silently discard half of is a rule nobody can hold in
    /// their head
    ///
    /// #### Note
    /// An equals sign is fine in a value and refused in a name,
    /// because the first one is where the kernel splits
    pub fn env<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.setup.env = as_env(vars, Env::Over);
        self
    }

    /// Gives the child these variables and nothing else
    ///
    /// ## Behaviour
    /// Nothing is inherited. This is also how a variable is
    /// *removed*: "everything except this" is a set the caller
    /// already has, through `std::env::vars_os` and a filter,
    /// and a method that could only take variables away would
    /// be the one setting that couldn't be read off the call
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// An empty set gives the child an empty environment, which
    /// is a thing it is allowed to have. It is not the same as
    /// not calling this at all
    pub fn env_only<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.setup.env = as_env(vars, Env::Only);
        self
    }
}

impl OutputTask {
    /// Runs a program, keeping its output
    pub(crate) fn new<S, A, I>(program: S, args: A) -> Self
    where
        S: AsRef<OsStr>,
        A: IntoIterator<Item = I>,
        I: AsRef<OsStr>,
    {
        Self {
            program: Program::new(program, args),
            setup: Setup::default(),
        }
    }

    /// Gives the child something to read
    ///
    /// ## Behaviour
    /// The bytes go down a pipe on the child's standard input,
    /// and the pipe is closed once they have all been taken —
    /// which is the only way a child ever learns its input has
    /// ended. Without this a child reads `/dev/null` and sees
    /// an end immediately
    ///
    /// The writing happens **while** both output streams are
    /// being read, in one loop over all three. That is the only
    /// ordering that works: feeding a child and then reading it
    /// wedges the moment the input outgrows a pipe, and reading
    /// it and then feeding it wedges the moment its answer
    /// does. A child that reads a line and writes a line —
    /// which is most of them — wedges on either
    ///
    /// ## Returns
    /// The task, so this can be written in the middle of a
    /// call. Calling it twice keeps the last
    ///
    /// #### Note
    /// A child is allowed to stop reading before it has taken
    /// everything, the way `head` does, and that is not an
    /// error. How much it actually took is not reported —
    /// [`ProcessOutput`] has no room for the number without
    /// costing every task the slot space
    ///
    /// [`ProcessOutput`]: crate::ProcessOutput
    pub fn input(mut self, data: impl Into<Arc<[u8]>>) -> Self {
        self.setup.input = Some(data.into());
        self
    }

    /// Starts the child somewhere else
    ///
    /// ## Behaviour
    /// The directory has to be **absolute**. Nothing in this
    /// crate calls `chdir`, but nothing in it can promise no
    /// other library will, and this process has workers, sleep
    /// threads, a reactor and a manager that never agree on
    /// what "here" means for longer than a scheduling quantum.
    /// A relative directory is a question read at one moment
    /// and used at another; an absolute one is the same
    /// directory whenever it is read
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// A relative one gives [`RuntimeError::BadDirectory`] when
    /// the task runs, rather than being resolved against
    /// wherever this process happens to be. One that simply
    /// isn't there is a different answer — the kernel goes
    /// looking for that one, and it comes back as `ENOENT`
    ///
    /// [`RuntimeError::BadDirectory`]: crate::RuntimeError::BadDirectory
    pub fn in_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.setup.dir = as_dir(path);
        self
    }

    /// Writes variables over the environment the child inherits
    ///
    /// ## Behaviour
    /// A merge rather than an addition. A name already in this
    /// process's environment is *replaced*, so the child sees
    /// it once — appending and trusting the child to read the
    /// first of two is not something this crate is willing to
    /// say, since POSIX leaves duplicates unspecified and
    /// plenty of programs walk the environment themselves
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last, rather than
    /// accumulating — two sets are a `.chain()` away, and a
    /// setter that a later [`OutputTask::env_only`] could
    /// silently discard half of is a rule nobody can hold in
    /// their head
    ///
    /// #### Note
    /// An equals sign is fine in a value and refused in a name,
    /// because the first one is where the kernel splits
    pub fn env<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.setup.env = as_env(vars, Env::Over);
        self
    }

    /// Gives the child these variables and nothing else
    ///
    /// ## Behaviour
    /// Nothing is inherited. This is also how a variable is
    /// *removed*: "everything except this" is a set the caller
    /// already has, through `std::env::vars_os` and a filter,
    /// and a method that could only take variables away would
    /// be the one setting that couldn't be read off the call
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// An empty set gives the child an empty environment, which
    /// is a thing it is allowed to have. It is not the same as
    /// not calling this at all
    pub fn env_only<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.setup.env = as_env(vars, Env::Only);
        self
    }
}

impl sealed::Sealed for StatusTask {}
impl sealed::Sealed for OutputTask {}

impl Task for StatusTask {
    type Output = Result<ExitStatus, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        // Asked before the spawn rather than after it. A child
        // started for a task nobody is waiting on is a program
        // that ran for no reason, and killing it again is a poor
        // substitute for never running it
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let data = self.setup.input.as_deref().unwrap_or(&[]);

        // An empty input and no input are the same thing from
        // the child's side — an immediate end — so the cheaper
        // of the two is used for both, and a run with nothing to
        // feed keeps the shape it had before there was any way
        // to feed it
        if data.is_empty() {
            let stdio = Stdio {
                input: None,
                capture: None,
            };

            let pid = spawn_child(&self.program, &self.setup, stdio)?;
            let mut child = Child::new(pid);

            return wait_exit(&mut child, kqueue::id().ok());
        }

        let (in_read, in_write) = input_pipe()?;

        let stdio = Stdio {
            input: Some(in_read.0),
            capture: None,
        };

        let pid = spawn_child(&self.program, &self.setup, stdio)?;

        // Built before anything below can fail, so every way out
        // of here goes through its `Drop`
        let mut child = Child::new(pid);

        // The child has a copy of its own now, and this has to
        // go *here* rather than at the end of the scope. A pipe
        // whose reader is still open in this process is one that
        // never breaks, so a child that refuses to read its
        // input would have this thread waiting on room that is
        // never coming, instead of being told at once that
        // nobody is listening
        drop(in_read);

        let mut in_write = Some(in_write);
        let queue = kqueue::id().ok();

        // Nothing to read, so this waits on room to write and on
        // nothing else. It still waits on a queue rather than
        // simply writing: a plain `write` against a child that
        // never reads is a thread inside a syscall no cancel can
        // reach, and holding a sleep thread that way is the one
        // failure this module exists to avoid
        let fed = match queue {
            Some(queue) => exchange(queue, &mut in_write, data, None, None),
            None => poll_exchange(&mut in_write, data, None, None),
        };

        fed?;
        drop(in_write);

        wait_exit(&mut child, queue)
    }

    /// Held for as long as the child runs, which has no bound
    /// at all
    ///
    /// The strongest case in the crate for this answer. A file
    /// read is held for a syscall; this is held for however long
    /// somebody else's program takes
    fn blocking(&self) -> bool {
        true
    }
}

impl Task for OutputTask {
    type Output = Result<ProcessOutput, RuntimeError>;

    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let data = self.setup.input.as_deref().unwrap_or(&[]);

        // An empty input and no input are the same thing from
        // the child's side, so the cheaper of the two is used
        // for both — two descriptors and a registration saved
        let feeding = match data.is_empty() {
            true => None,
            false => Some(input_pipe()?),
        };

        let (out_read, out_write) = pipe()?;
        let (err_read, err_write) = pipe()?;

        let stdio = Stdio {
            input: feeding.as_ref().map(|(read, _)| read.0),
            capture: Some((out_write.0, err_write.0)),
        };

        let pid = spawn_child(&self.program, &self.setup, stdio)?;

        // Built before anything below can fail, so every way out
        // of here goes through its `Drop`
        let mut child = Child::new(pid);

        let mut in_write = match feeding {
            Some((in_read, in_write)) => {
                // The mirror image of the two below. Nothing
                // waits on this end — but a pipe whose reader is
                // still open is one that never breaks, so a
                // child that refuses to read its input would
                // have this thread waiting on room that is never
                // coming rather than being told at once that
                // nobody is listening
                drop(in_read);

                Some(in_write)
            }

            None => None,
        };

        // The child has copies of its own now, and these have to
        // go *here* rather than at the end of the scope. A write
        // end still open in this process is one the pipe is
        // still waiting on, so leaving them to fall out of scope
        // would have the exchange below waiting for an end that
        // this thread is itself holding back
        drop(out_write);
        drop(err_write);

        let queue = kqueue::id().ok();

        let (stdout, stderr) = match queue {
            Some(queue) => exchange(
                queue,
                &mut in_write,
                data,
                Some(&out_read),
                Some(&err_read),
            ),
            None => poll_exchange(&mut in_write, data, Some(&out_read), Some(&err_read)),
        }?;

        drop(in_write);
        drop(out_read);
        drop(err_read);

        let status = wait_exit(&mut child, queue)?;

        Ok(ProcessOutput::new(stdout, stderr, status))
    }

    fn blocking(&self) -> bool {
        true
    }
}

// `prepare` is deliberately left defaulted on both. Every field
// is input that was converted once and never changes, and each
// run makes its own pipes, its own child and its own buffers on
// the stack — so there is nothing a previous run could leave
// behind for the next one to inherit. `SleepTask` needs it only
// because its `created` is a measurement rather than an input
//
// The three settings added later change nothing here. A
// directory, an environment and a buffer of input are input like
// the program and its arguments — converted once, and read the
// same way on the tenth run as on the first. The one thing that
// would have forced a `prepare` is the position *within* that
// buffer, which is exactly why it lives in `Feed`, a local of
// the run: a cursor kept on the task would have the second run
// start where the first one stopped

/// An open descriptor that closes itself
///
/// The same bargain as the one the file tasks make. A process
/// task has more ways out than most — a spawn that failed, a
/// cancel part way through a drain, a panic unwinding through
/// `catch_unwind` — and a descriptor leaked from a sleep thread
/// is leaked for the life of the process
///
/// #### Note
/// Closing in `Drop` is also what keeps errno intact, since
/// `check` reads whatever the last call set
struct Fd(libc::c_int);

impl Drop for Fd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// A spawned child that is always reaped
///
/// ## Behaviour
/// A child left behind is worse than a leaked descriptor. It is
/// a zombie for the life of the process at best, and at worst a
/// program still running that nobody has a handle on any more
///
/// `Drop` kills before it waits, which is what keeps the wait
/// bounded. A guard that only reaped would hold a sleep thread
/// until a long running child happened to finish, which is the
/// exact failure it exists to prevent
///
/// #### Note
/// The consequence is worth saying plainly: an error *after* the
/// spawn kills the child. The alternative is leaving a program
/// running that the caller was never given a way to name
struct Child {
    /// The child, or `NO_CHILD` once it has been reaped
    pid: libc::pid_t,

    /// The queue its exit is registered on, if it is
    ///
    /// Carried so `Drop` can take the registration back off.
    /// `Executor::interrupt` can't — it deletes a timer at the
    /// task's own id, which is neither this filter nor this
    /// ident — so a cancelled task would otherwise leave an
    /// exit note nobody read on a queue that outlives it
    queue: Option<i32>,
}

impl Child {
    /// Takes hold of a freshly spawned child
    fn new(pid: libc::pid_t) -> Self {
        Self { pid, queue: None }
    }

    /// Says where this child's exit is registered
    fn watching(&mut self, queue: i32) {
        self.queue = Some(queue);
    }

    /// Says the child has been waited for
    ///
    /// Called on every path that successfully reaps, and the
    /// reason `Drop` can be trusted. Without it the guard would
    /// go on to signal a pid the kernel has already given back
    /// out, which on a busy machine is somebody else's process
    fn reaped(&mut self) {
        self.pid = NO_CHILD;
    }
}

impl Drop for Child {
    fn drop(&mut self) {
        if self.pid == NO_CHILD {
            return;
        }

        if let Some(queue) = self.queue {
            unwatch_proc(queue, self.pid);
        }

        kill_and_reap(self.pid);
    }
}

/// A `posix_spawn` attribute set that destroys itself
struct SpawnAttr(libc::posix_spawnattr_t);

impl SpawnAttr {
    /// Makes an empty attribute set
    fn new() -> Result<Self, RuntimeError> {
        let mut raw: libc::posix_spawnattr_t = ptr::null_mut();

        spawn_check(unsafe { libc::posix_spawnattr_init(&mut raw) })?;

        Ok(Self(raw))
    }
}

impl Drop for SpawnAttr {
    fn drop(&mut self) {
        unsafe { libc::posix_spawnattr_destroy(&mut self.0) };
    }
}

/// A `posix_spawn` file action list that destroys itself
struct FileActions(libc::posix_spawn_file_actions_t);

impl FileActions {
    /// Makes an empty action list
    fn new() -> Result<Self, RuntimeError> {
        let mut raw: libc::posix_spawn_file_actions_t = ptr::null_mut();

        spawn_check(unsafe { libc::posix_spawn_file_actions_init(&mut raw) })?;

        Ok(Self(raw))
    }
}

impl Drop for FileActions {
    fn drop(&mut self) {
        unsafe { libc::posix_spawn_file_actions_destroy(&mut self.0) };
    }
}

/// Checks a `posix_spawn` family return value
///
/// ## Behaviour
/// These are the one family in the crate that `IntCheck` is
/// wrong for. They return the errno *directly*, as a positive
/// number, and don't set `errno` at all — so `check`, which
/// looks for a negative and then reads `errno`, would call
/// every one of them a success and report a stale number on the
/// paths where it didn't
fn spawn_check(code: libc::c_int) -> Result<(), RuntimeError> {
    if code == 0 {
        return Ok(());
    }

    Err(RuntimeError::CheckError(Some(code)))
}

/// Converts one argument to the form the kernel takes
///
/// ## Returns
/// `None` when it has a zero byte in it. The kernel reads an
/// argument as bytes up to the first zero, so one containing
/// its own has no faithful form to be passed in — and passing
/// the part before it would run the program with a different
/// argument
fn as_c_arg(arg: impl AsRef<OsStr>) -> Option<CString> {
    CString::new(arg.as_ref().as_bytes()).ok()
}

/// The file action that sets a child's working directory
///
/// Looked up rather than declared, because it is not in
/// `libc`'s bindings for this platform and declaring it would
/// be worse than not having it: a symbol named in an `extern`
/// block is a *load time* dependency, so a macOS without it
/// would refuse to start the whole program — including every
/// part of it that never runs a child
type AddChdir =
    unsafe extern "C" fn(*mut libc::posix_spawn_file_actions_t, *const libc::c_char) -> libc::c_int;

/// What `dlsym` is given to search every image in the process
///
/// `libc` binds `RTLD_MAIN_ONLY` for this platform and none of
/// the other three, so the one that is wanted is written out.
/// `dlfcn.h` has defined it as `-2` for as long as it has
/// existed
const RTLD_DEFAULT: *mut libc::c_void = -2isize as *mut libc::c_void;

/// The name the call has had since macOS 10.15
///
/// Deprecated as of macOS 26 in favour of the POSIX one, but
/// deprecated is not gone — this is the name that answers on
/// every release from 10.15 up, which is why it is asked first
const ADD_CHDIR_NP: &CStr = c"posix_spawn_file_actions_addchdir_np";

/// The name POSIX.1-2024 gave it, which macOS 26 was the first
/// release to declare
const ADD_CHDIR: &CStr = c"posix_spawn_file_actions_addchdir";

/// Finds the file action that sets a working directory, once
///
/// ## Behaviour
/// The `_np` name first and the standard one second, which
/// between them cover every release that has either. Neither is
/// in `libc`, and neither can be declared without making it a
/// condition of the program starting at all
///
/// ## Returns
/// `None` on a macOS that has neither, which is every release
/// before 10.15
///
/// #### Note
/// Asked once and kept. `dlsym` against `RTLD_DEFAULT` walks
/// every image in the process, which `dlfcn.h` itself calls
/// expensive and advises against, and the answer cannot change
/// while the program is running
fn add_chdir() -> Option<AddChdir> {
    static FOUND: OnceLock<Option<AddChdir>> = OnceLock::new();

    *FOUND.get_or_init(|| {
        for name in [ADD_CHDIR_NP, ADD_CHDIR] {
            let symbol = unsafe { libc::dlsym(RTLD_DEFAULT, name.as_ptr()) };

            if symbol.is_null() {
                continue;
            }

            // Sound because the signature above is the one
            // `spawn.h` gives for both of these names, and a
            // symbol found under one of them is that function
            return Some(unsafe { mem::transmute::<*mut libc::c_void, AddChdir>(symbol) });
        }

        None
    })
}

/// Whether a program names a file relative to wherever the
/// child happens to start
///
/// ## Behaviour
/// The same three kinds a shell tells apart: a name with no
/// slash in it is looked up in `PATH`, a name starting with one
/// is absolute, and anything else is relative to the working
/// directory — which is the only one of the three that a change
/// of directory moves
fn relative(file: &CStr) -> bool {
    let bytes = file.to_bytes();

    !bytes.starts_with(b"/") && bytes.contains(&b'/')
}

/// Puts a relative program on the end of the directory it will
/// be run from
///
/// ## Behaviour
/// The reason this exists is a platform bug, not tidiness. A
/// relative program spawned alongside a directory change makes
/// macOS **launch the program and then report `ENOENT`
/// anyway** — and an error from a spawn that actually started
/// something is the one case this module cannot survive, since
/// the guard that would have killed the child is built from the
/// pid the spawn never handed back
///
/// Joining resolves it here instead. It is the same file the
/// child would have reached, because the directory is set
/// before the exec and a relative program resolves against it
/// — so nothing about *which* program runs changes. What
/// changes is that the platform is never asked the question it
/// gets wrong
///
/// #### Note
/// Only ever reached with an absolute directory, since a
/// relative one is refused long before this, so the result is
/// always absolute too
fn join(dir: &CStr, file: &CStr) -> Result<CString, RuntimeError> {
    let dir = dir.to_bytes();
    let file = file.to_bytes();

    let mut path = Vec::with_capacity(dir.len() + file.len() + 1);

    path.extend_from_slice(dir);

    if !dir.ends_with(b"/") {
        path.push(b'/');
    }

    path.extend_from_slice(file);

    // Neither half can hold a zero byte, both having come from
    // a `CStr`, so this is a shape the type system can't say
    // rather than a case that happens
    CString::new(path).map_err(|_| RuntimeError::BadDirectory)
}

/// Converts a working directory to the form the kernel takes
///
/// ## Returns
/// `Dir::Bad` for the two directories that can't be used: one
/// with a zero byte in it, which the kernel would read as a
/// shorter path naming somewhere else, and one that isn't
/// absolute, which names somewhere different depending on which
/// thread is asking
///
/// #### Note
/// Absolute is checked here rather than at the spawn because it
/// is a property of what was written, not of what the kernel
/// makes of it. A directory that doesn't exist is the kernel's
/// to answer and comes back as an `ENOENT` from the spawn
fn as_dir(path: impl AsRef<Path>) -> Dir {
    let path = path.as_ref();

    if !path.is_absolute() {
        return Dir::Bad;
    }

    match CString::new(path.as_os_str().as_bytes()) {
        Ok(dir) => Dir::At(dir),
        Err(_) => Dir::Bad,
    }
}

/// Joins one variable into the `NAME=VALUE` form the kernel
/// takes
///
/// ## Returns
/// `None` for the three that can't be passed on as written: a
/// zero byte in either half, an equals sign in the *name*, and
/// an empty name
///
/// #### Note
/// An equals sign in the value is deliberately allowed. The
/// kernel splits an entry at its *first* one, so everything
/// after that is value however many more there are
fn as_c_var(name: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Option<CString> {
    let name = name.as_ref().as_bytes();
    let value = value.as_ref().as_bytes();

    // A name carrying the character the kernel splits on would
    // be cut somewhere else, and the child would be handed a
    // variable under a name nobody asked for. An empty one
    // names nothing at all
    if name.is_empty() || name.contains(&b'=') {
        return None;
    }

    let mut entry = Vec::with_capacity(name.len() + value.len() + 1);

    entry.extend_from_slice(name);
    entry.push(b'=');
    entry.extend_from_slice(value);

    CString::new(entry).ok()
}

/// Converts a set of variables, or says one of them wouldn't
///
/// `into` is which kind of environment they are becoming, since
/// the conversion is the same for both and only the answer
/// differs
fn as_env<I, K, V>(vars: I, into: fn(Arc<[CString]>) -> Env) -> Env
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let converted = vars
        .into_iter()
        .map(|(name, value)| as_c_var(name, value))
        .collect::<Option<Arc<[CString]>>>();

    match converted {
        Some(vars) => into(vars),
        None => Env::Bad,
    }
}

impl Env {
    /// Builds the pointer array `posix_spawn` actually takes
    ///
    /// ## Behaviour
    /// Per run rather than kept, for the reason `Program::argv`
    /// gives: these are pointers into storage the task holds,
    /// and a raw pointer is not `Send`
    ///
    /// ## Returns
    /// `None` for an inherited environment, which is handed
    /// over as this process's own array rather than rebuilt —
    /// both the cheapest answer and the only one that is
    /// exactly right
    ///
    /// #### Note
    /// An overlay reads this process's environment as it goes,
    /// and nothing holds that still. A `setenv` on another
    /// thread while this is running is a data race the standard
    /// library made `unsafe` to write for precisely this
    /// reason, and there is nothing to be done about it here
    /// beyond not being the one doing it
    fn envp(&self) -> Result<Option<Vec<*mut libc::c_char>>, RuntimeError> {
        match self {
            Self::Inherited => Ok(None),

            Self::Bad => Err(RuntimeError::BadVariable),

            Self::Only(vars) => {
                let mut envp = Vec::with_capacity(vars.len() + 1);

                envp.extend(vars.iter().map(|var| var.as_ptr().cast_mut()));

                // An empty set still gets an array holding a
                // null, and never a null array. The two look
                // alike and mean opposite things — one is an
                // empty environment, the other is a request to
                // inherit
                envp.push(ptr::null_mut());

                Ok(Some(envp))
            }

            Self::Over(vars) => Ok(Some(merge(&inherited(), vars))),
        }
    }
}

/// Writes an overlay over a base environment
///
/// ## Behaviour
/// A real merge rather than an append. A duplicate name in an
/// environment is unspecified by POSIX — `getenv` on this
/// platform takes the first, but plenty of programs walk the
/// array themselves and would find the variable twice with two
/// different values. "Usually the right one" is not something
/// this crate says
///
/// ## Returns
/// The pointer array, null terminated. Both halves point at
/// storage that outlives the call: the base at this process's
/// own environment, the overlay at the task's own `Arc`
fn merge(base: &[*mut libc::c_char], over: &[CString]) -> Vec<*mut libc::c_char> {
    let mut envp = Vec::with_capacity(base.len() + over.len() + 1);

    for entry in base {
        // Sound because every pointer in `base` came from a
        // zero terminated entry — either this process's own
        // environment or a `CString` that outlives this
        let name = key(unsafe { CStr::from_ptr(*entry) }.to_bytes());

        if over.iter().any(|var| key(var.to_bytes()) == name) {
            continue;
        }

        envp.push(*entry);
    }

    envp.extend(over.iter().map(|var| var.as_ptr().cast_mut()));
    envp.push(ptr::null_mut());

    envp
}

/// This process's own environment, entry by entry
///
/// `libc` doesn't export `environ` for this platform, so the
/// array is reached through the accessor that does
fn inherited() -> Vec<*mut libc::c_char> {
    let mut found = Vec::new();
    let mut at = unsafe { *libc::_NSGetEnviron() };

    if at.is_null() {
        return found;
    }

    loop {
        let entry = unsafe { *at };

        if entry.is_null() {
            return found;
        }

        found.push(entry);
        at = unsafe { at.add(1) };
    }
}

/// The bytes of an entry that name the variable
///
/// ## Behaviour
/// Everything up to the first equals sign, which is the split
/// the kernel makes. An entry with no equals sign at all is
/// malformed and can only be its own name — this process's
/// environment is not something the crate put there, so it is
/// read as it is rather than assumed to be well formed
fn key(entry: &[u8]) -> &[u8] {
    match entry.iter().position(|byte| *byte == b'=') {
        Some(at) => &entry[..at],
        None => entry,
    }
}

/// Makes a pipe whose ends both close on exec
///
/// ## Returns
/// The read end and the write end, in that order
///
/// #### Note
/// macOS has no `pipe2`, so close on exec is a second call
/// rather than a flag on the first. `CLOEXEC_DEFAULT` covers
/// this crate's own spawns, which is where the real hazard is;
/// this covers a fork the crate knows nothing about, and leaves
/// a window between the two calls that the platform gives no
/// way to close
fn pipe() -> Result<(Fd, Fd), RuntimeError> {
    let mut ends: [libc::c_int; 2] = [-1, -1];

    unsafe { libc::pipe(ends.as_mut_ptr()) }.check()?;

    let read = Fd(ends[0]);
    let write = Fd(ends[1]);

    unsafe { libc::fcntl(read.0, libc::F_SETFD, libc::FD_CLOEXEC) }.check()?;
    unsafe { libc::fcntl(write.0, libc::F_SETFD, libc::FD_CLOEXEC) }.check()?;

    Ok((read, write))
}

/// Makes the pipe a child's input comes down
///
/// ## Behaviour
/// An ordinary pipe, and then two things done to the end this
/// process keeps
///
/// `O_NONBLOCK`, because this is the one descriptor in the
/// crate that cannot be written to with a blocking call. A wake
/// on a write filter says there is *room*, possibly one byte,
/// and a blocking write doesn't come back until it has placed
/// everything it was offered — so it would sit holding the rest
/// against a child that is itself waiting on this thread. See
/// `EventDesc::new_write`
///
/// Safe to set here and nowhere else: this end is a file
/// description this process alone holds. The child is given the
/// *read* end, which is a different description, so the flag
/// cannot reach it
///
/// ## Returns
/// The read end for the child, and the write end for here
fn input_pipe() -> Result<(Fd, Fd), RuntimeError> {
    let (read, write) = pipe()?;

    unsafe { libc::fcntl(write.0, libc::F_SETFL, libc::O_NONBLOCK) }.check()?;

    // Asks the kernel not to raise `SIGPIPE` for this
    // descriptor. The standard library sets `SIGPIPE` to
    // ignored at startup, which is what turns a child that
    // stopped reading into an `EPIPE` here rather than a dead
    // process — but that is somebody else's promise, and a
    // crate loaded into a host that isn't a Rust program never
    // had it. This asks for the same thing per descriptor, from
    // the only party that can be sure
    //
    // Best effort, and the result is deliberately dropped: the
    // fallback is the disposition that was going to be relied
    // on anyway
    let _ = unsafe { libc::fcntl(write.0, F_SETNOSIGPIPE, 1) };

    Ok((read, write))
}

/// What a child is handed for its three standard descriptors
///
/// A struct rather than the pair this used to be. Three
/// descriptors, with two of them having three states between
/// them, is past what a tuple can be read as
struct Stdio {
    /// The read end of an input pipe, or `None` for `/dev/null`
    input: Option<libc::c_int>,

    /// The write ends for the child's output and error, or
    /// `None` to leave them this process's own
    capture: Option<(libc::c_int, libc::c_int)>,
}

/// Starts a child
///
/// ## Behaviour
/// `posix_spawn` rather than a `fork` and an `exec`. This
/// process has worker threads, sleep threads, a reactor and a
/// manager, and between a `fork` and an `exec` only async
/// signal safe calls are legal — a child forked from a thread
/// that wasn't holding the allocator's lock deadlocks the first
/// time anything allocates. On macOS `posix_spawn` is a syscall
/// of its own, so that window doesn't exist to be got wrong
///
/// `streams` is the pair of write ends to give the child, or
/// `None` to let it keep this process's own output
fn spawn_child(
    program: &Program,
    setup: &Setup,
    stdio: Stdio,
) -> Result<libc::pid_t, RuntimeError> {
    let dir = match &setup.dir {
        Dir::Inherited => None,
        Dir::At(dir) => Some(dir.as_c_str()),
        Dir::Bad => return Err(RuntimeError::BadDirectory),
    };

    let (file, argv) = program.argv(dir)?;
    let envp = setup.env.envp()?;

    let mut actions = FileActions::new()?;
    let mut attr = SpawnAttr::new()?;

    match stdio.input {
        // A pipe this thread is holding the other end of, which
        // it closes once the child has taken everything — that
        // close is the only way a child learns its input ended
        Some(fd) => {
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, fd, 0) })?;
        }

        None => {
            spawn_check(unsafe {
                libc::posix_spawn_file_actions_addopen(
                    &mut actions.0,
                    0,
                    DEV_NULL.as_ptr(),
                    libc::O_RDONLY,
                    0,
                )
            })?;
        }
    }

    match stdio.capture {
        Some((out, err)) => {
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, out, 1) })?;
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, err, 2) })?;
        }

        // A descriptor duplicated onto itself is how an fd is
        // exempted from `CLOEXEC_DEFAULT`, which would otherwise
        // take this process's own output away from a child that
        // was supposed to inherit it
        None => {
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, 1, 1) })?;
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, 2, 2) })?;
        }
    }

    // Last of the file actions, so nothing above it is left
    // resolving against a directory the caller chose. Everything
    // above happens to be absolute or already open, which makes
    // the ordering moot today — it is written this way so it
    // stays right for anything added later
    if let Some(dir) = dir {
        // A directory that can't be set is never a directory
        // quietly ignored. Running somebody's program somewhere
        // other than where they said is the worst outcome
        // available here, and the one thing that must not happen
        let Some(chdir) = add_chdir() else {
            return Err(RuntimeError::CheckError(Some(libc::ENOSYS)));
        };

        spawn_check(unsafe { chdir(&mut actions.0, dir.as_ptr()) })?;
    }

    let mut empty: libc::sigset_t = unsafe { mem::zeroed() };
    let mut full: libc::sigset_t = unsafe { mem::zeroed() };

    unsafe { libc::sigemptyset(&mut empty) }.check()?;
    unsafe { libc::sigfillset(&mut full) }.check()?;

    spawn_check(unsafe { libc::posix_spawnattr_setsigmask(&mut attr.0, &empty) })?;
    spawn_check(unsafe { libc::posix_spawnattr_setsigdefault(&mut attr.0, &full) })?;
    spawn_check(unsafe { libc::posix_spawnattr_setpgroup(&mut attr.0, 0) })?;
    spawn_check(unsafe { libc::posix_spawnattr_setflags(&mut attr.0, SPAWN_FLAGS) })?;

    let mut pid: libc::pid_t = 0;

    // The environment has to be passed explicitly. A null envp
    // is not "inherit", it is an *empty* environment, which is
    // almost never what a caller meant
    let handed = match &envp {
        Some(envp) => envp.as_ptr(),
        None => (unsafe { *libc::_NSGetEnviron() }) as *const *mut libc::c_char,
    };

    let code = unsafe {
        libc::posix_spawnp(
            &mut pid,
            file.as_ptr(),
            &actions.0,
            &attr.0,
            argv.as_ptr(),
            handed,
        )
    };

    spawn_check(code)?;

    Ok(pid)
}

/// The input a child is being given while it is being read
///
/// ## Behaviour
/// Its own type rather than a third entry in the arrays beside
/// it, because it is genuinely not one of them: it is watched
/// under a different filter, it carries a position rather than
/// a buffer, and when it runs out it has to be *closed* — which
/// is the only way a child ever learns its input has ended. A
/// read that finishes is unregistered; a write that finishes is
/// unregistered and then shut
struct Feed<'a> {
    /// The write end, held so it can be closed the moment the
    /// last byte lands
    ///
    /// #### Note
    /// A borrow of the caller's `Option` rather than an owned
    /// `Fd`, so a loop that comes apart part way through still
    /// leaves the guard where it was. Closing has to happen
    /// *inside* the loop and an unwind has to close it too, and
    /// this is the only shape that gets both
    end: &'a mut Option<Fd>,

    /// Every byte the child is to be given
    data: &'a [u8],

    /// How many of them it has taken
    ///
    /// A local of the run rather than a field on the task. A
    /// repeat puts the same task back in the same slot, and a
    /// cursor kept there would have the second run start where
    /// the first one stopped
    sent: usize,

    /// The queue the watch sits on, so it can come back off
    /// before the end is shut
    queue: i32,
}

impl Feed<'_> {
    /// Whether there is anything left to give
    fn open(&self) -> bool {
        self.end.is_some()
    }

    /// Whether an event is this one
    fn is(&self, ident: usize, filter: i16) -> bool {
        match self.end.as_ref() {
            Some(end) => filter == libc::EVFILT_WRITE && ident == end.0 as usize,
            None => false,
        }
    }

    /// Puts the watch on the queue
    fn watch(&self) -> Result<(), RuntimeError> {
        let Some(end) = self.end.as_ref() else {
            return Ok(());
        };

        unsafe {
            KEvent::register(
                self.queue,
                end.0 as usize,
                0,
                WakeTarget::None.encode(),
                EventDesc::new_write(),
            )
        }
        .check()?;

        Ok(())
    }

    /// Gives the child as much as the pipe will take
    ///
    /// One piece at most, then back to the wait, so a large
    /// input never holds the loop away from the streams it is
    /// also reading
    fn push(&mut self) -> Result<(), RuntimeError> {
        let Some(fd) = self.end.as_ref().map(|end| end.0) else {
            return Ok(());
        };

        if !write_chunk(fd, self.data, &mut self.sent)? {
            self.finish();
        }

        Ok(())
    }

    /// Takes the watch off and shuts the end
    ///
    /// ## Behaviour
    /// The unregister comes first, and it is not a tidy up.
    /// Closing a descriptor drops its registrations by itself —
    /// but it also hands the number straight back out, and a
    /// later registration at the same number would be racing a
    /// knote the kernel is still taking down
    ///
    /// Then the close, which is the whole point. A child
    /// reading its standard input sees no end until every write
    /// end is shut, and this thread is holding the last one
    fn finish(&mut self) {
        let Some(end) = self.end.as_ref() else {
            return;
        };

        unwatch_write(self.queue, end.0);
        self.end.take();
    }
}

/// Reads everything a child writes while giving it everything
/// it was to be given
///
/// ## Behaviour
/// One loop over up to three descriptors, which is the only
/// ordering that works. Feeding a child and then reading it
/// wedges the moment the input outgrows a pipe; reading it and
/// then feeding it wedges the moment its answer does; and
/// reading one stream to its end before starting the other
/// wedges on a child that writes to both. A child that reads a
/// line and writes a line — which is most of them — wedges on
/// any of the three
///
/// The read watches are level triggered, so a wake means the
/// descriptor has something on it and one ordinary blocking
/// read is safe. The write watch is level triggered too and
/// means something weaker, which is why its descriptor is the
/// one that isn't blocking. See `EventDesc::new_write`
///
/// ## Returns
/// What the child wrote to each stream, in the order they were
/// given. Both are empty when there was nothing to read, which
/// is what a run with input and no capture asks for
fn exchange(
    queue: i32,
    input: &mut Option<Fd>,
    data: &[u8],
    out: Option<&Fd>,
    err: Option<&Fd>,
) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
    let ends = [
        out.map_or(IGNORED, |end| end.0),
        err.map_or(IGNORED, |end| end.0),
    ];

    let mut found = [Vec::new(), Vec::new()];
    let mut open = [false, false];

    let mut feed = Feed {
        end: input,
        data,
        sent: 0,
        queue,
    };

    let mut outcome = Ok(());

    for (slot, end) in ends.iter().enumerate() {
        if *end == IGNORED {
            continue;
        }

        let watched = unsafe {
            KEvent::register(
                queue,
                *end as usize,
                0,
                WakeTarget::None.encode(),
                EventDesc::new_read(),
            )
        }
        .check();

        match watched {
            Ok(_) => open[slot] = true,
            Err(error) => {
                outcome = Err(error);
                break;
            }
        }
    }

    if outcome.is_ok() {
        outcome = feed.watch();
    }

    // Recorded once, around the whole loop, rather than once per
    // wake. This is the field a cancel reaches for, and a loop
    // that never wrote it would sit in `listen` with nothing
    // able to bring it back — a child that says nothing and
    // doesn't end would hold the thread for as long as it felt
    // like
    if outcome.is_ok() {
        outcome = match executor::waiting_on(queue) {
            true => {
                let pumped = pump(queue, &ends, &mut open, &mut found, &mut feed);

                // Called whatever `pump` decided, because it
                // also spins out a cancel that is still part way
                // through its syscalls against this queue
                match executor::stopped_waiting() {
                    true => pumped,
                    false => Err(RuntimeError::Cancelled),
                }
            }

            // Cancelled before the loop even started
            false => Err(RuntimeError::Cancelled),
        };
    }

    // Whatever happened, nothing stays registered. A watch left
    // on a queue this thread keeps is a descriptor number that
    // will be handed out again
    for (slot, end) in ends.iter().enumerate() {
        if open[slot] {
            unwatch_read(queue, *end);
        }
    }

    // And the feed comes off too, for the loops that stopped
    // before it had finished giving
    feed.finish();

    outcome?;

    let [stdout, stderr] = found;

    Ok((stdout, stderr))
}

/// The loop `exchange` runs once everything is watched
///
/// ## Behaviour
/// Ends when both streams have reported their last byte and the
/// input has been taken. A stream that has finished is taken
/// off the queue immediately — a level triggered descriptor
/// sitting at its end reads as *ready* every time, so leaving it
/// on would turn the wait for the others into a spin
///
/// #### Note
/// The wake a cancel sends arrives here as an `EVFILT_USER`
/// event, which nothing below matches, so it falls through to
/// the cancellation check at the bottom of the batch. That is
/// deliberate: a wake left over from an earlier cancel would
/// otherwise cut a live exchange short and lose output that had
/// already been written
///
/// `EV_EOF` on the write filter falls through the same way,
/// into `push`, which finds the `EPIPE` and finishes. One path
/// to the decision rather than two that can disagree
fn pump(
    queue: i32,
    ends: &[libc::c_int; 2],
    open: &mut [bool; 2],
    found: &mut [Vec<u8>; 2],
    feed: &mut Feed<'_>,
) -> Result<(), RuntimeError> {
    let mut events = eventlist();

    while open[0] || open[1] || feed.open() {
        let count = match unsafe { KEvent::listen(queue, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        };

        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
                continue;
            }

            if feed.is(event.ident, event.filter) {
                feed.push()?;
                continue;
            }

            if event.filter != libc::EVFILT_READ {
                continue;
            }

            let Some(slot) = ends.iter().position(|end| *end as usize == event.ident) else {
                continue;
            };

            if !open[slot] {
                continue;
            }

            if !read_chunk(ends[slot], &mut found[slot])? {
                unwatch_read(queue, ends[slot]);
                open[slot] = false;
            }
        }

        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }
    }

    Ok(())
}

/// The same exchange without a queue to wait on
///
/// ## Behaviour
/// Only reached when the kernel wouldn't give this thread a
/// kqueue, which takes it running out of descriptors — so the
/// fallback deliberately uses `poll`, which needs none
///
/// #### Note
/// Cancellable, unlike the sleep task's fallback, which parks
/// and cannot be reached. The difference is what is at stake: a
/// sleep that misses a cancel costs latency, and a process that
/// misses one leaves a program running
fn poll_exchange(
    input: &mut Option<Fd>,
    data: &[u8],
    out: Option<&Fd>,
    err: Option<&Fd>,
) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
    let ends = [
        out.map_or(IGNORED, |end| end.0),
        err.map_or(IGNORED, |end| end.0),
    ];

    let mut found = [Vec::new(), Vec::new()];
    let mut open = [ends[0] != IGNORED, ends[1] != IGNORED];
    let mut sent = 0;

    while open[0] || open[1] || input.is_some() {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let writing = input.as_ref().map_or(IGNORED, |end| end.0);

        let mut watched = [
            libc::pollfd {
                fd: if open[0] { ends[0] } else { IGNORED },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: if open[1] { ends[1] } else { IGNORED },
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: writing,
                events: libc::POLLOUT,
                revents: 0,
            },
        ];

        let ready = unsafe {
            libc::poll(
                watched.as_mut_ptr(),
                watched.len() as libc::nfds_t,
                PROCESS_POLL.as_millis() as libc::c_int,
            )
        }
        .check();

        match ready {
            Ok(0) => continue,
            Ok(_) => {}
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        }

        for slot in 0..ends.len() {
            if !open[slot] || watched[slot].revents == 0 {
                continue;
            }

            if !read_chunk(ends[slot], &mut found[slot])? {
                open[slot] = false;
            }
        }

        if watched[2].revents != 0 && !write_chunk(writing, data, &mut sent)? {
            // Dropping the guard is what closes the end, which
            // is what tells the child its input has finished
            input.take();
        }
    }

    let [stdout, stderr] = found;

    Ok((stdout, stderr))
}

/// Gives a descriptor as much as it will take, from a position
///
/// ## Behaviour
/// One `FILE_CHUNK` at most, then back to the caller. A pipe
/// write is allowed to take fewer bytes than it is offered —
/// a blocking one isn't, but this end is not blocking — so the
/// count is carried rather than assumed and a short write
/// simply leaves the rest for the next wake
///
/// ## Returns
/// Whether there is anything left to give. `false` means either
/// everything has been taken, or the child stopped reading
///
/// #### Note
/// A child that stops reading is **not** an error. `head` does
/// it every time, and reporting it would make every such call
/// something the caller has to special case. The consequence is
/// that how much the child actually took isn't reported —
/// neither output type has room for the number
fn write_chunk(fd: libc::c_int, data: &[u8], sent: &mut usize) -> Result<bool, RuntimeError> {
    let want = (data.len() - *sent).min(FILE_CHUNK);

    if want == 0 {
        return Ok(false);
    }

    // Sound because `sent` never passes `data.len()`, which the
    // line above is what keeps true
    let from = unsafe { data.as_ptr().add(*sent) }.cast::<libc::c_void>();

    let written = unsafe { libc::write(fd, from, want) }.check();

    let put = match written {
        Ok(put) => put as usize,

        // Nothing was placed and nothing was lost
        Err(RuntimeError::CheckError(Some(libc::EINTR))) => return Ok(true),

        // The pipe filled between the wake and the write, which
        // a level triggered filter allows: the wake said there
        // was room, not that it would still be there
        Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => return Ok(true),

        // The child stopped reading, which is a choice it is
        // allowed to make
        Err(RuntimeError::CheckError(Some(libc::EPIPE))) => return Ok(false),

        Err(error) => return Err(error),
    };

    *sent += put;

    Ok(*sent < data.len())
}

/// Reads one chunk from a descriptor onto the end of a buffer
///
/// ## Returns
/// Whether the descriptor has anything left to give. `false`
/// means it reached its end, which for a pipe means the child
/// closed it
fn read_chunk(fd: libc::c_int, into: &mut Vec<u8>) -> Result<bool, RuntimeError> {
    into.reserve(FILE_CHUNK);

    let read = unsafe {
        libc::read(
            fd,
            into.spare_capacity_mut().as_mut_ptr().cast::<libc::c_void>(),
            FILE_CHUNK,
        )
    }
    .check();

    let got = match read {
        Ok(got) => got as usize,
        // Nothing was read and nothing was lost. The descriptor
        // is still ready, so the next wait comes straight back
        Err(RuntimeError::CheckError(Some(libc::EINTR))) => return Ok(true),
        Err(error) => return Err(error),
    };

    if got == 0 {
        return Ok(false);
    }

    // Sound because the kernel just wrote `got` bytes into the
    // spare capacity that `reserve` guaranteed
    unsafe { into.set_len(into.len() + got) };

    Ok(true)
}

/// Waits for a child to end, and reaps it
///
/// ## Behaviour
/// Registers the watch *before* asking whether the child has
/// already gone, which is what closes the race between the two.
/// A child that ended first is found by the ask; a child that
/// ends a moment later has a watch already waiting for it. There
/// is no order of those two calls that leaves a gap
///
/// The status comes from `waitpid` rather than off the event.
/// The child has to be reaped either way, so `waitpid` is
/// already the one call that must happen and there is nothing
/// to be gained by having a second answer to compare it against
fn wait_exit(child: &mut Child, queue: Option<i32>) -> Result<ExitStatus, RuntimeError> {
    let pid = child.pid;

    let Some(queue) = queue else {
        return poll_exit(child);
    };

    let watched = unsafe {
        KEvent::register(
            queue,
            pid as usize,
            0,
            WakeTarget::None.encode(),
            EventDesc::new_proc_exit(),
        )
    }
    .check();

    // Nothing waits on a registration the kernel refused. The
    // fallback needs no queue at all, which is the right shape
    // for a failure whose likeliest cause is not having one
    if watched.is_err() {
        return poll_exit(child);
    }

    child.watching(queue);

    if let Some(status) = try_reap(pid)? {
        child.reaped();
        unwatch_proc(queue, pid);

        return Ok(status);
    }

    // Cancelled before the wait even started, so the watch comes
    // straight back off rather than sitting on a queue nobody is
    // listening to it on
    if !executor::waiting_on(queue) {
        unwatch_proc(queue, pid);

        return Err(RuntimeError::Cancelled);
    }

    kqueue::wait_for(queue, pid as usize, libc::EVFILT_PROC);

    let carry_on = executor::stopped_waiting();

    unwatch_proc(queue, pid);

    if !carry_on {
        return Err(RuntimeError::Cancelled);
    }

    let status = reap(pid)?;
    child.reaped();

    Ok(status)
}

/// Waits for a child to end without a queue to wait on
///
/// Asks, gives the thread up for a moment, and asks again. Slow
/// by design — it is only reached when the kernel has no
/// descriptors left, and the thing it must not do in that state
/// is ask for another one
fn poll_exit(child: &mut Child) -> Result<ExitStatus, RuntimeError> {
    loop {
        if let Some(status) = try_reap(child.pid)? {
            child.reaped();

            return Ok(status);
        }

        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        // A poll of nothing at all, which is the cheapest sleep
        // available to a thread that can't be given a timer
        unsafe { libc::poll(ptr::null_mut(), 0, PROCESS_POLL.as_millis() as libc::c_int) };
    }
}

/// Waits for a child, without blocking
///
/// ## Returns
/// `None` when the child is still running
fn try_reap(pid: libc::pid_t) -> Result<Option<ExitStatus>, RuntimeError> {
    let mut status: libc::c_int = 0;

    loop {
        let waited = unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) }.check();

        return match waited {
            Ok(0) => Ok(None),
            Ok(_) => Ok(Some(ExitStatus::from_raw(status))),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => Err(error),
        };
    }
}

/// Waits for a child, however long it takes
fn reap(pid: libc::pid_t) -> Result<ExitStatus, RuntimeError> {
    let mut status: libc::c_int = 0;

    loop {
        let waited = unsafe { libc::waitpid(pid, &mut status, 0) }.check();

        return match waited {
            Ok(_) => Ok(ExitStatus::from_raw(status)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => Err(error),
        };
    }
}

/// Ends a child and waits for it
///
/// ## Behaviour
/// The group goes first. Every child is spawned as the leader
/// of its own, so a signal to the group takes whatever it
/// started with it — which matters because the usual way to run
/// anything is through a shell, and a shell's children are not
/// the shell
///
/// Then the child itself, in case the group never took. Both
/// are ignored if they fail, since the only thing to do about a
/// signal that didn't land is the wait that follows it
///
/// `SIGKILL` rather than a gentler signal followed by one. An
/// escalation needs a deadline, a deadline needs a timer, and a
/// timer needs a wait that a cancel can reach into — which is
/// scheduling policy this crate doesn't have anywhere else.
/// `SIGKILL` also can't be caught, which is the only reason the
/// wait underneath it is bounded at all
fn kill_and_reap(pid: libc::pid_t) {
    unsafe { libc::kill(-pid, libc::SIGKILL) };
    unsafe { libc::kill(pid, libc::SIGKILL) };

    let _ = reap(pid);
}

/// Takes a read watch back off a queue
fn unwatch_read(queue: i32, fd: libc::c_int) {
    let _ = unsafe {
        KEvent::register(
            queue,
            fd as usize,
            0,
            ptr::null_mut(),
            EventDesc::new_read_delete(),
        )
    };
}

/// Takes a write watch back off a queue
///
/// Always before the descriptor is closed rather than after.
/// See `Feed::finish`
fn unwatch_write(queue: i32, fd: libc::c_int) {
    let _ = unsafe {
        KEvent::register(
            queue,
            fd as usize,
            0,
            ptr::null_mut(),
            EventDesc::new_write_delete(),
        )
    };
}

/// Takes an exit watch back off a queue
///
/// Unconditional, on every path out of a wait. It removes the
/// registration *and* anything it already queued, which is what
/// stops a later task on this thread finding a stale exit note
/// at a pid that has since been handed out again
fn unwatch_proc(queue: i32, pid: libc::pid_t) {
    let _ = unsafe {
        KEvent::register(
            queue,
            pid as usize,
            0,
            ptr::null_mut(),
            EventDesc::new_proc_delete(),
        )
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    /// Both process tasks hold their thread for as long as the
    /// child runs, and have to say so
    ///
    /// A task that forgot would sit on a worker for the whole
    /// life of somebody else's program, which is worse than the
    /// file case it borrows this test from — a file read ends
    /// when the disk says so, and this ends whenever the child
    /// feels like it
    #[test]
    fn every_process_task_says_it_blocks() {
        assert!(StatusTask::new("a", [""; 0]).blocking(), "run");
        assert!(OutputTask::new("a", [""; 0]).blocking(), "output");

        // A configured one blocks for longer, not less. The
        // setters return `Self`, so a task that lost the answer
        // on the way through one of them would be a task the
        // pool put on a worker
        assert!(
            StatusTask::new("a", [""; 0])
                .input(b"x".as_slice())
                .in_dir("/usr")
                .env([("A", "b")])
                .blocking(),
            "a configured run"
        );

        assert!(
            OutputTask::new("a", [""; 0])
                .input(b"x".as_slice())
                .in_dir("/usr")
                .env_only([("A", "b")])
                .blocking(),
            "a configured output"
        );
    }

    /// An argument the kernel can't be given doesn't convert
    ///
    /// The failure that matters isn't the refusal, it is what
    /// would happen without one: the kernel reads an argument up
    /// to its first zero, so passing this through would quietly
    /// run the program with a different one
    #[test]
    fn an_argument_with_a_zero_byte_does_not_convert() {
        assert!(as_c_arg("a\0b").is_none(), "a zero byte must not convert");
        assert!(as_c_arg("ab").is_some(), "an ordinary argument must convert");
    }

    /// Each half of the conversion reports as itself
    ///
    /// The two fail for the same reason and are not the same
    /// answer. A caller told its *path* was bad when the fault
    /// was in argument three would go looking in the wrong place
    #[test]
    fn a_bad_program_and_a_bad_argument_are_told_apart() {
        let bad_program = Program::new("a\0b", ["fine"]);
        let bad_argument = Program::new("fine", ["a\0b"]);

        assert_eq!(
            bad_program.argv(None).unwrap_err(),
            RuntimeError::BadPath,
            "a zero byte in the program is a bad path"
        );

        assert_eq!(
            bad_argument.argv(None).unwrap_err(),
            RuntimeError::BadArgument,
            "a zero byte in an argument is a bad argument"
        );
    }

    /// The argument vector carries the program's own name in
    /// front and a null on the end
    ///
    /// Both are what `execve` reads rather than what a caller
    /// passed, so neither is visible anywhere else to be checked
    #[test]
    fn the_argument_vector_is_the_shape_exec_reads() {
        let program = Program::new("/bin/echo", ["one", "two"]);
        let (file, argv) = program.argv(None).expect("an ordinary program must convert");

        assert_eq!(file.to_bytes(), b"/bin/echo", "the program is passed as itself");
        assert_eq!(argv.len(), 4, "name, two arguments, and a null");
        assert!(argv[3].is_null(), "the vector must end in a null");

        let name = unsafe { std::ffi::CStr::from_ptr(argv[0]) };

        assert_eq!(
            name.to_bytes(),
            b"/bin/echo",
            "the program's own name comes first"
        );
    }

    /// A child the guard drops is killed rather than waited for
    ///
    /// ## Behaviour
    /// The honest half of "no child is left behind". That a
    /// child is *reaped* is hard to assert from outside — the
    /// test doesn't own the pid and can't ask about one it has
    /// given back — but that the guard returns promptly is the
    /// half that would actually break, and it only returns after
    /// its `waitpid` comes back
    ///
    /// A guard that only reaped would sit here for thirty
    /// seconds
    #[test]
    fn dropping_a_child_does_not_wait_for_it() {
        let program = Program::new("/bin/sleep", ["30"]);
        let stdio = Stdio {
            input: None,
            capture: None,
        };

        let pid =
            spawn_child(&program, &Setup::default(), stdio).expect("sleep must spawn");

        let child = Child::new(pid);
        let started = Instant::now();

        drop(child);

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the guard must kill rather than wait, took {:?}",
            started.elapsed()
        );
    }

    /// An entry is split at its *first* equals sign
    ///
    /// The one that matters is the middle case. A value is
    /// allowed to hold as many as it likes — a `PATH` or a
    /// command line stored in a variable routinely does — and a
    /// split at the last one would name the variable after most
    /// of its own value
    #[test]
    fn an_entry_is_split_at_its_first_equals() {
        assert_eq!(key(b"A=B"), b"A", "the ordinary case");
        assert_eq!(key(b"A=B=C"), b"A", "a value may hold more of them");
        assert_eq!(key(b"NOEQUALS"), b"NOEQUALS", "a malformed entry is its own name");
        assert_eq!(key(b"=X"), b"", "an empty name is still where the split is");
    }

    /// A variable the kernel can't be given doesn't convert
    ///
    /// Three ways to write one, against one way for an argument.
    /// The equals sign is the interesting one: it is refused in
    /// a name because that is where the kernel splits, and
    /// allowed in a value because everything after the first one
    /// is value
    #[test]
    fn a_variable_that_cannot_be_passed_on_does_not_convert() {
        assert!(as_c_var("A\0B", "x").is_none(), "a zero byte in the name");
        assert!(as_c_var("A", "x\0y").is_none(), "a zero byte in the value");
        assert!(as_c_var("A=B", "x").is_none(), "an equals sign in the name");
        assert!(as_c_var("", "x").is_none(), "an empty name names nothing");

        let ordinary = as_c_var("A", "x").expect("an ordinary variable must convert");
        assert_eq!(ordinary.to_bytes(), b"A=x", "joined as the kernel takes it");

        let valued = as_c_var("A", "x=y").expect("an equals sign in a value is fine");
        assert_eq!(valued.to_bytes(), b"A=x=y", "and is left where it was");
    }

    /// An overlay writes over rather than alongside
    ///
    /// ## Behaviour
    /// Run against a base made up here rather than against this
    /// process's own environment, which nothing holds still and
    /// which the test would have to change to make an assertion
    /// about
    ///
    /// The failure this catches is the cheap implementation:
    /// appending the overlay and trusting the child to read the
    /// first of two entries. POSIX doesn't say which one it
    /// reads, and a program walking the array itself sees both
    #[test]
    fn an_overlay_writes_over_rather_than_alongside() {
        let base = [
            CString::new("HOME=/old").unwrap(),
            CString::new("PATH=/bin").unwrap(),
        ];

        let pointers = base.iter().map(|var| var.as_ptr().cast_mut()).collect::<Vec<_>>();
        let over = [CString::new("HOME=/new").unwrap()];

        let merged = merge(&pointers, &over);

        assert!(merged.last().expect("never empty").is_null(), "must end in a null");

        let entries = merged[..merged.len() - 1]
            .iter()
            .map(|entry| unsafe { CStr::from_ptr(*entry) }.to_bytes().to_vec())
            .collect::<Vec<_>>();

        assert_eq!(
            entries.len(),
            2,
            "the overlay replaces rather than adds, got {entries:?}"
        );

        assert!(
            entries.iter().any(|entry| entry == b"PATH=/bin"),
            "an untouched variable stays"
        );
        assert!(
            entries.iter().any(|entry| entry == b"HOME=/new"),
            "the overlay's value is the one that survives"
        );
        assert!(
            !entries.iter().any(|entry| entry == b"HOME=/old"),
            "and the old one is gone rather than alongside"
        );
    }

    /// The three kinds of program name are told apart
    ///
    /// Only the middle one moves when the child changes
    /// directory, which is the whole reason to ask
    #[test]
    fn a_relative_program_is_told_from_the_others() {
        assert!(relative(c"./foo"), "a leading dot is relative");
        assert!(relative(c"a/b"), "so is anything else with a slash in it");
        assert!(!relative(c"/bin/ls"), "a leading slash is absolute");
        assert!(!relative(c"ls"), "no slash at all is a PATH lookup");
    }

    /// A directory has to be absolute and free of zero bytes
    #[test]
    fn a_directory_that_cannot_be_used_does_not_convert() {
        assert!(matches!(as_dir("/usr"), Dir::At(_)), "an absolute path converts");
        assert!(matches!(as_dir("build"), Dir::Bad), "a relative one does not");
        assert!(matches!(as_dir("./build"), Dir::Bad), "nor a leading dot");
        assert!(matches!(as_dir("/a\0b"), Dir::Bad), "nor a zero byte");
    }

    /// A relative program is joined onto its directory, and
    /// keeps its own name
    ///
    /// ## Behaviour
    /// Two claims at once. The *file* becomes absolute, so the
    /// platform is never asked to resolve a relative program
    /// against a directory it gets wrong. And `argv[0]` stays
    /// exactly as written, because a child is told its own name
    /// the way a shell tells it and a path this crate assembled
    /// is not that name
    ///
    /// #### Note
    /// The join is a join and not a tidy up. `./sh` under `/bin`
    /// comes out as `/bin/./sh`, which names the same file — the
    /// kernel resolves a `.` like any other component — and is
    /// left that way on purpose. Taking it out means taking out
    /// `.//`, `././`, `..` and trailing slashes too, which is a
    /// path canonicaliser written for a difference nobody can
    /// see: the path is handed to the spawn and never to the
    /// caller, and a `CheckError` carries the errno alone
    #[test]
    fn a_relative_program_is_joined_onto_its_directory() {
        let program = Program::new("./sh", ["-c", "true"]);
        let (file, argv) = program.argv(Some(c"/bin")).expect("must convert");

        assert_eq!(
            file.to_bytes(),
            b"/bin/./sh",
            "the spawn is given an absolute path"
        );

        let plain = Program::new("a/b", [""; 0]);
        let (file, _) = plain.argv(Some(c"/usr")).expect("must convert");

        assert_eq!(
            file.to_bytes(),
            b"/usr/a/b",
            "a relative path without a leading dot joins plainly"
        );

        let name = unsafe { CStr::from_ptr(argv[0]) };

        assert_eq!(name.to_bytes(), b"./sh", "argv[0] is left as the caller wrote it");

        // An absolute program is already resolved, and a bare
        // name belongs to the `PATH` walk rather than to the
        // directory — neither is joined
        let absolute = Program::new("/bin/sh", [""; 0]);
        let (file, _) = absolute.argv(Some(c"/usr")).expect("must convert");
        assert_eq!(file.to_bytes(), b"/bin/sh", "an absolute program is left alone");

        let looked_up = Program::new("sh", [""; 0]);
        let (file, _) = looked_up.argv(Some(c"/usr")).expect("must convert");
        assert_eq!(file.to_bytes(), b"sh", "a PATH lookup is left alone");
    }

    /// A pipe can be asked not to raise `SIGPIPE`
    ///
    /// ## Behaviour
    /// Settles whether the belt-and-braces call in `input_pipe`
    /// does anything on this platform. It is best effort there
    /// and its result is dropped, so being wrong costs nothing —
    /// but knowing which it is beats assuming
    ///
    /// If this ever starts failing, the call can go and the
    /// disposition the standard library sets carries it alone
    #[test]
    fn a_pipe_can_be_asked_not_to_raise_sigpipe() {
        /// The other half of the pair, for reading it back
        const F_GETNOSIGPIPE: libc::c_int = 74;

        let (_read, write) = pipe().expect("a pipe must be made");

        let set = unsafe { libc::fcntl(write.0, F_SETNOSIGPIPE, 1) };

        assert_eq!(set, 0, "the kernel refused the request outright");

        let read_back = unsafe { libc::fcntl(write.0, F_GETNOSIGPIPE) };

        assert_eq!(read_back, 1, "it was accepted but did not stick");
    }
}
