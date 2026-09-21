//! # Running
//! A child that has been started and is still being talked to

use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    executor,
    futures::{
        net::stream::{Pipe, RecvTask, SendTask, Source},
        process::{
            exit_status::ExitStatus,
            process_task::{
                Child, Env, Program, Setup, Stdio, as_dir, as_env, input_pipe, kill_and_reap, pipe,
                spawn_child, wait_exit,
            },
        },
        signal::{SignalKind, dispatch},
        task::{
            Nothing, Task,
            sealed::{self},
        },
    },
    modules::{input::Token, int_check::IntCheck, kqueue},
};
use std::{
    ffi::OsStr,
    fmt, mem,
    path::Path,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
};

const _: () = assert!(mem::size_of::<Result<RunningChild, RuntimeError>>() <= INLINE_PAYLOAD);

/// Starts a program and hands back a way to talk to it
///
/// ## Returns
/// The [`RunningChild`], as soon as the program has started
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct SpawnTask {
    /// What to run
    program: Program,

    /// Everything else about how it runs
    setup: Setup,
}

impl SpawnTask {
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

    /// Starts the child somewhere else
    ///
    /// ## Behaviour
    /// The directory has to be **absolute**
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// A relative one gives [`RuntimeError::BadDirectory`] when
    /// the task runs. One that doesn't exist gives `ENOENT`
    ///
    /// [`RuntimeError::BadDirectory`]: crate::RuntimeError::BadDirectory
    pub fn in_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.setup.dir = as_dir(path);
        self
    }

    /// Writes variables over the environment the child inherits
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
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
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn env_only<I, K, V>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        self.setup.env = as_env(vars, Env::Only);
        self
    }

    fn spawn(&self) -> Result<RunningChild, RuntimeError> {
        let (in_read, in_write) = input_pipe()?;
        let (out_read, out_write) = pipe()?;
        let (err_read, err_write) = pipe()?;

        nonblocking(&out_read)?;
        nonblocking(&err_read)?;

        let stdio = Stdio {
            input: Some(in_read.raw()),
            capture: Some((out_write.raw(), err_write.raw())),
        };

        let pid = spawn_child(&self.program, &self.setup, stdio)?;

        Ok(RunningChild {
            inner: Arc::new(Inner {
                pid,
                status: Mutex::new(None),
                ended: AtomicBool::new(false),
                stdin: Mutex::new(Some(Arc::new(Pipe::new(in_write)))),
                stdout: Arc::new(Pipe::new(out_read)),
                stderr: Arc::new(Pipe::new(err_read)),
            }),
        })
    }
}

/// A program that has been started, with its standard streams
/// piped here
///
/// ## Behaviour
/// Cloning it gives another handle on the same child. Its methods
/// build the tasks that wait for it, signal it, and talk to it
///
/// ```no_run
/// # use atap::{Runtime, process::Process};
/// # fn main() -> Result<(), atap::RuntimeError> {
/// let child = Runtime::block(Process::spawn("/bin/cat", Process::NO_ARGS))?;
///
/// let input = child.stdin().expect("still open");
/// Runtime::block(input.send(b"hello\n".as_slice()))?;
///
/// let line = Runtime::block(child.stdout().recv_until(b"\n", 64))?;
/// assert_eq!(line, b"hello\n");
///
/// child.close_stdin();
/// drop(input);
///
/// assert!(Runtime::block(child.wait())?.success());
/// # Ok(())
/// # }
/// ```
///
/// #### Note
/// Dropping the last handle kills the child's whole process group
/// and reaps it, unless it has already been waited for. A stream
/// nobody reads can fill, and a child writing to it then waits
#[derive(Clone)]
pub struct RunningChild {
    inner: Arc<Inner>,
}

/// What every handle on one child shares
struct Inner {
    /// The child's pid
    pid: libc::pid_t,

    /// How it ended, once it has been waited for
    ///
    /// Held by whoever is waiting, so only one thread reaps
    status: Mutex<Option<ExitStatus>>,

    /// Whether it has been reaped, so its pid is never signalled
    /// again
    ended: AtomicBool,

    /// The child's standard input, until it is closed
    stdin: Mutex<Option<Arc<Pipe>>>,

    /// The child's standard output
    stdout: Arc<Pipe>,

    /// The child's standard error
    stderr: Arc<Pipe>,
}

impl RunningChild {
    /// The child's process id
    ///
    /// #### Note
    /// Once the child has been waited for, the id may belong to
    /// another process
    pub fn id(&self) -> u32 {
        self.inner.pid as u32
    }

    /// A task that waits for the child to end and reaps it
    ///
    /// ## Returns
    /// How it ended. Every wait gives the same answer, and a
    /// cancelled wait leaves the child running
    pub fn wait(&self) -> ChildWaitTask {
        ChildWaitTask {
            child: self.clone(),
        }
    }

    /// A task that sends `kind` to the child
    ///
    /// ## Returns
    /// Nothing once the kernel has taken it, or
    /// [`RuntimeError::Finished`] if the child has already been
    /// waited for
    ///
    /// [`RuntimeError::Finished`]: crate::RuntimeError::Finished
    pub fn signal(&self, kind: SignalKind) -> ChildSignalTask {
        ChildSignalTask {
            child: self.clone(),
            signo: kind.number(),
            group: false,
        }
    }

    /// A task that kills the child and everything it started
    ///
    /// ## Returns
    /// Nothing once the kernel has taken it. Waiting afterwards
    /// reports the kill
    pub fn kill(&self) -> ChildSignalTask {
        ChildSignalTask {
            child: self.clone(),
            signo: libc::SIGKILL,
            group: true,
        }
    }

    /// The child's standard input, or `None` once it has been
    /// closed
    pub fn stdin(&self) -> Option<ChildStdin> {
        let stdin = self
            .inner
            .stdin
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        stdin.as_ref().map(|pipe| ChildStdin {
            pipe: Arc::clone(pipe),
        })
    }

    /// Lets go of the child's standard input
    ///
    /// ## Behaviour
    /// The child reads the end of its input once every
    /// [`ChildStdin`] and every send on one is gone too
    pub fn close_stdin(&self) {
        let closed = self
            .inner
            .stdin
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();

        drop(closed);
    }

    /// The child's standard output
    pub fn stdout(&self) -> ChildOutput {
        ChildOutput {
            pipe: Arc::clone(&self.inner.stdout),
        }
    }

    /// The child's standard error
    pub fn stderr(&self) -> ChildOutput {
        ChildOutput {
            pipe: Arc::clone(&self.inner.stderr),
        }
    }

    /// Waits for the child on this thread
    fn wait_here(&self) -> Result<ExitStatus, RuntimeError> {
        let mut status = self
            .inner
            .status
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        if let Some(status) = *status {
            return Ok(status);
        }

        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let mut child = Child::new(self.inner.pid);
        let waited = wait_exit(&mut child, kqueue::id().ok());

        match waited {
            Ok(ended) => {
                *status = Some(ended);
                self.inner.ended.store(true, Ordering::SeqCst);
            }

            // Not reaped, so the guard is told to leave the child be
            Err(RuntimeError::Cancelled) => child.reaped(),

            Err(_) => self.inner.ended.store(true, Ordering::SeqCst),
        }

        waited
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if !self.ended.load(Ordering::SeqCst) {
            kill_and_reap(self.pid);
        }
    }
}

impl fmt::Debug for RunningChild {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunningChild")
            .field("id", &self.inner.pid)
            .finish_non_exhaustive()
    }
}

/// A running child's standard input
///
/// Cloning it keeps the input open, like any other copy
#[derive(Clone)]
pub struct ChildStdin {
    pipe: Arc<Pipe>,
}

impl ChildStdin {
    /// Writes every byte of `data` to the child
    ///
    /// ## Returns
    /// The number of bytes written, which is always all of them.
    /// A child that has closed its input gives `EPIPE`
    pub fn send(&self, data: impl Into<Arc<[u8]>>) -> SendTask {
        SendTask::new(Source::Pipe(Arc::clone(&self.pipe)), data.into())
    }
}

impl fmt::Debug for ChildStdin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("ChildStdin").finish_non_exhaustive()
    }
}

/// A running child's standard output or standard error
///
/// The receives are the same as a connection's, and so is what
/// happens to bytes read past what was asked for
#[derive(Clone)]
pub struct ChildOutput {
    pipe: Arc<Pipe>,
}

impl ChildOutput {
    fn source(&self) -> Source {
        Source::Pipe(Arc::clone(&self.pipe))
    }

    /// Receives whatever has arrived, up to `max` bytes
    ///
    /// ## Returns
    /// At least one byte, or none once the child has closed the
    /// stream
    pub fn recv(&self, max: usize) -> RecvTask {
        RecvTask::some(self.source(), max)
    }

    /// Receives exactly `len` bytes
    ///
    /// ## Returns
    /// [`RuntimeError::Closed`] if the stream ends first
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_exact(&self, len: usize) -> RecvTask {
        RecvTask::exact(self.source(), len)
    }

    /// Receives up to and including `delimiter`, reading at most
    /// `max` bytes
    ///
    /// ## Returns
    /// [`RuntimeError::TooLong`] if `max` is reached first, and
    /// [`RuntimeError::Closed`] if the stream ends first
    ///
    /// [`RuntimeError::TooLong`]: crate::RuntimeError::TooLong
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_until(&self, delimiter: impl AsRef<[u8]>, max: usize) -> RecvTask {
        RecvTask::until(self.source(), Arc::from(delimiter.as_ref()), max)
    }

    /// Receives everything until the child closes the stream
    pub fn recv_to_end(&self) -> RecvTask {
        RecvTask::to_end(self.source())
    }
}

impl fmt::Debug for ChildOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ChildOutput")
            .finish_non_exhaustive()
    }
}

/// Waits for a running child to end
///
/// ## Returns
/// How it ended
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ChildWaitTask {
    child: RunningChild,
}

/// Sends a signal to a running child
///
/// ## Returns
/// Nothing once the kernel has taken it
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ChildSignalTask {
    child: RunningChild,

    /// What to send
    signo: libc::c_int,

    /// Whether the child's whole group gets it too
    group: bool,
}

impl ChildSignalTask {
    fn send(&self) -> Result<(), RuntimeError> {
        dispatch::sendable(self.signo)?;

        let inner = &self.child.inner;

        // Held so a wait can't reap the child between the look and
        // the signal
        let reaped = inner.status.try_lock().map(|status| status.is_some());

        if reaped.unwrap_or(false) || inner.ended.load(Ordering::SeqCst) {
            return Err(RuntimeError::Finished);
        }

        if self.group {
            let _ = unsafe { libc::kill(-inner.pid, self.signo) };
        }

        unsafe { libc::kill(inner.pid, self.signo) }.check()?;

        Ok(())
    }
}

impl sealed::Sealed for SpawnTask {}
impl sealed::Sealed for ChildWaitTask {}
impl sealed::Sealed for ChildSignalTask {}

impl Task for SpawnTask {
    type Output = Result<RunningChild, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        self.spawn()
    }

    /// Starting a program can take a while
    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for ChildWaitTask {
    type Output = Result<ExitStatus, RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.child.wait_here()
    }

    /// Held for as long as the child runs
    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for ChildSignalTask {
    type Output = Result<(), RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.send()
    }
}

/// Makes a read end not wait
fn nonblocking(fd: &crate::modules::fd::Fd) -> Result<(), RuntimeError> {
    let flags = unsafe { libc::fcntl(fd.raw(), libc::F_GETFL) }.check()?;

    unsafe { libc::fcntl(fd.raw(), libc::F_SETFL, flags | libc::O_NONBLOCK) }.check()?;

    Ok(())
}
