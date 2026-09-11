//! # Process task
//! The tasks the `Process` constructors return, and everything
//! they do once a thread picks them up

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
        kqueue::{self, Waited},
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

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<ExitStatus, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<ProcessOutput, RuntimeError>>() <= INLINE_PAYLOAD);

/// What a `Child` holds instead of a pid once it has
/// been reaped
///
/// A reaped pid may already be somebody else's process
const NO_CHILD: libc::pid_t = -1;

/// What `poll` is given in place of a descriptor it should
/// leave alone
const IGNORED: libc::c_int = -1;

/// Asks a descriptor not to raise `SIGPIPE` when its reader
/// goes away
///
/// Not in `libc`'s bindings for this platform
const F_SETNOSIGPIPE: libc::c_int = 73;

/// Where a child's standard input comes from when it isn't
/// given any
const DEV_NULL: &CStr = c"/dev/null";

/// The attributes every child is spawned with
///
/// ## Behaviour
/// `CLOEXEC_DEFAULT` stops one task's pipe being inherited by
/// another task's child, which would hold its write end open
///
/// `SETSIGDEF` and `SETSIGMASK` put every signal back to its
/// default, mostly so a child doesn't inherit an ignored
/// `SIGPIPE`
///
/// `SETPGROUP` makes the child the leader of its own group, so
/// a cancel takes the whole tree
const SPAWN_FLAGS: libc::c_short = (libc::POSIX_SPAWN_CLOEXEC_DEFAULT
    | libc::POSIX_SPAWN_SETSIGDEF
    | libc::POSIX_SPAWN_SETSIGMASK
    | libc::POSIX_SPAWN_SETPGROUP) as libc::c_short;

/// A program and its arguments, in the form the kernel takes
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
    /// Per run, since a raw pointer isn't `Send`
    ///
    /// ## Returns
    /// The program, and an argument vector with the program's
    /// own name in front of it and a null on the end
    fn argv(&self, dir: Option<&CStr>) -> Result<(CString, Vec<*mut libc::c_char>), RuntimeError> {
        let file = self.file.as_ref().ok_or(RuntimeError::BadPath)?;
        let args = self.args.as_ref().ok_or(RuntimeError::BadArgument)?;

        let mut argv = Vec::with_capacity(args.len() + 2);

        // Every program is handed its own name as its first argument,
        // exactly as the caller wrote it
        argv.push(file.as_ptr().cast_mut());
        argv.extend(args.iter().map(|arg| arg.as_ptr().cast_mut()));
        argv.push(ptr::null_mut());

        // A relative program is resolved here rather than left to the
        // platform. See `join`
        let spawn_as = match dir {
            Some(dir) if relative(file) => join(dir, file)?,
            _ => file.clone(),
        };

        Ok((spawn_as, argv))
    }
}

/// Everything a child is configured with beyond the program
/// itself
#[derive(Debug, Clone, Default)]
struct Setup {
    /// What to feed the child, or `None` for `/dev/null`
    input: Option<Arc<[u8]>>,

    /// Where it starts
    dir: Dir,

    /// What it is given for an environment
    env: Env,
}

/// Where a child starts
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
/// Entries are kept pre-joined as `NAME=VALUE`
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
/// ## Returns
/// How the child ended, which is a code or a signal. A child
/// that ran and failed is an [`ExitStatus`] saying so rather
/// than an error
#[derive(Debug, Clone)]
pub struct StatusTask {
    /// What to run
    program: Program,

    /// Everything else about how it runs
    setup: Setup,
}

/// Runs a program and collects everything it wrote
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
    /// which is closed once they have all been taken. Without
    /// this a child reads `/dev/null`
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// A child is allowed to stop reading before it has taken
    /// everything, and that is not an error. How much it took
    /// is not reported
    pub fn input(mut self, data: impl Into<Arc<[u8]>>) -> Self {
        self.setup.input = Some(data.into());
        self
    }

    /// Starts the child somewhere else
    ///
    /// ## Behaviour
    /// The directory has to be **absolute**, since the working
    /// directory can change underneath the runtime
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
    /// ## Behaviour
    /// A name already in this process's environment is replaced,
    /// so the child sees it once
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// An equals sign is fine in a value and refused in a name
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
    /// Nothing is inherited, so this is also how a variable is
    /// removed
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// An empty set gives the child an empty environment, which
    /// is not the same as not calling this at all
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
    /// which is closed once they have all been taken. Without
    /// this a child reads `/dev/null`
    ///
    /// The writing happens while both output streams are being
    /// read, so a child that reads a line and writes a line
    /// never deadlocks
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// A child is allowed to stop reading before it has taken
    /// everything, and that is not an error. How much it took
    /// is not reported
    pub fn input(mut self, data: impl Into<Arc<[u8]>>) -> Self {
        self.setup.input = Some(data.into());
        self
    }

    /// Starts the child somewhere else
    ///
    /// ## Behaviour
    /// The directory has to be **absolute**, since the working
    /// directory can change underneath the runtime
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
    /// ## Behaviour
    /// A name already in this process's environment is replaced,
    /// so the child sees it once
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// An equals sign is fine in a value and refused in a name
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
    /// Nothing is inherited, so this is also how a variable is
    /// removed
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// An empty set gives the child an empty environment, which
    /// is not the same as not calling this at all
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
        // Checked before the spawn, so a cancelled task never runs
        // the program at all
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

        let data = self.setup.input.as_deref().unwrap_or(&[]);

        // An empty input is the same as none from the child's side
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
        // goes through its `Drop`
        let mut child = Child::new(pid);

        // Closed now, so a child that doesn't read gives `EPIPE`
        // rather than a pipe that never breaks
        drop(in_read);

        let mut in_write = Some(in_write);
        let queue = kqueue::id().ok();

        // Waits on a queue rather than a plain `write`, so a child
        // that never reads can't hold the thread out of a cancel
        let fed = match queue {
            Some(queue) => exchange(queue, &mut in_write, data, None, None),
            None => poll_exchange(&mut in_write, data, None, None),
        };

        fed?;
        drop(in_write);

        wait_exit(&mut child, queue)
    }

    /// Held for as long as the child runs
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

        // An empty input is the same as none from the child's side
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
        // goes through its `Drop`
        let mut child = Child::new(pid);

        let mut in_write = match feeding {
            Some((in_read, in_write)) => {
                // Closed now, so a child that doesn't read gives `EPIPE`
                // rather than a pipe that never breaks
                drop(in_read);

                Some(in_write)
            }

            None => None,
        };

        // Closed now, or the exchange below waits for an end this
        // thread is holding open
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

/// An open descriptor that closes itself
///
/// #### Note
/// Closing in `Drop` also keeps errno intact
struct Fd(libc::c_int);

impl Drop for Fd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

/// A spawned child that is always reaped
///
/// `Drop` kills before it waits, so an error after the spawn
/// kills the child
struct Child {
    /// The child, or `NO_CHILD` once it has been reaped
    pid: libc::pid_t,

    /// The queue its exit is registered on, if it is
    ///
    /// Carried so `Drop` can take the registration back off
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

    /// Says the child has been waited for, so `Drop` never
    /// signals a pid the kernel has handed back out
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
/// These return the errno directly rather than setting it, so
/// `IntCheck` is wrong for them
fn spawn_check(code: libc::c_int) -> Result<(), RuntimeError> {
    if code == 0 {
        return Ok(());
    }

    Err(RuntimeError::CheckError(Some(code)))
}

/// Converts one argument to the form the kernel takes
///
/// ## Returns
/// `None` when it has a zero byte in it
fn as_c_arg(arg: impl AsRef<OsStr>) -> Option<CString> {
    CString::new(arg.as_ref().as_bytes()).ok()
}

/// The file action that sets a child's working directory
///
/// Looked up rather than declared, so a macOS without it can
/// still start the program
type AddChdir =
    unsafe extern "C" fn(*mut libc::posix_spawn_file_actions_t, *const libc::c_char) -> libc::c_int;

/// What `dlsym` is given to search every image in the process
///
/// Not in `libc`'s bindings for this platform
const RTLD_DEFAULT: *mut libc::c_void = -2isize as *mut libc::c_void;

/// The name the call has had since macOS 10.15
const ADD_CHDIR_NP: &CStr = c"posix_spawn_file_actions_addchdir_np";

/// The name POSIX.1-2024 gave it, which macOS 26 was the first
/// release to declare
const ADD_CHDIR: &CStr = c"posix_spawn_file_actions_addchdir";

/// Finds the file action that sets a working directory, once
///
/// ## Returns
/// `None` on a macOS that has neither name, which is every
/// release before 10.15
fn add_chdir() -> Option<AddChdir> {
    static FOUND: OnceLock<Option<AddChdir>> = OnceLock::new();

    *FOUND.get_or_init(|| {
        for name in [ADD_CHDIR_NP, ADD_CHDIR] {
            let symbol = unsafe { libc::dlsym(RTLD_DEFAULT, name.as_ptr()) };

            if symbol.is_null() {
                continue;
            }

            // Both names have the signature `spawn.h` gives above
            return Some(unsafe { mem::transmute::<*mut libc::c_void, AddChdir>(symbol) });
        }

        None
    })
}

/// Whether a program names a file relative to wherever the
/// child happens to start
fn relative(file: &CStr) -> bool {
    let bytes = file.to_bytes();

    !bytes.starts_with(b"/") && bytes.contains(&b'/')
}

/// Puts a relative program on the end of the directory it will
/// be run from
///
/// macOS launches a relative program spawned alongside a
/// directory change and then reports `ENOENT` anyway, which
/// leaves a child nothing can reap. Joining here means the
/// platform is never asked
fn join(dir: &CStr, file: &CStr) -> Result<CString, RuntimeError> {
    let dir = dir.to_bytes();
    let file = file.to_bytes();

    let mut path = Vec::with_capacity(dir.len() + file.len() + 1);

    path.extend_from_slice(dir);

    if !dir.ends_with(b"/") {
        path.push(b'/');
    }

    path.extend_from_slice(file);

    CString::new(path).map_err(|_| RuntimeError::BadDirectory)
}

/// Converts a working directory to the form the kernel takes
///
/// ## Returns
/// `Dir::Bad` for a directory with a zero byte in it or one
/// that isn't absolute
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
/// `None` for a zero byte in either half, an equals sign in
/// the name, and an empty name
fn as_c_var(name: impl AsRef<OsStr>, value: impl AsRef<OsStr>) -> Option<CString> {
    let name = name.as_ref().as_bytes();
    let value = value.as_ref().as_bytes();

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
/// `into` is which kind of environment they are becoming
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
    /// ## Returns
    /// `None` for an inherited environment, which is handed over
    /// as this process's own array
    ///
    /// #### Note
    /// An overlay reads this process's environment as it goes, so
    /// a `setenv` on another thread at the same time is a data race
    fn envp(&self) -> Result<Option<Vec<*mut libc::c_char>>, RuntimeError> {
        match self {
            Self::Inherited => Ok(None),

            Self::Bad => Err(RuntimeError::BadVariable),

            Self::Only(vars) => {
                let mut envp = Vec::with_capacity(vars.len() + 1);

                envp.extend(vars.iter().map(|var| var.as_ptr().cast_mut()));

                // A null array would mean inherit rather than empty
                envp.push(ptr::null_mut());

                Ok(Some(envp))
            }

            Self::Over(vars) => Ok(Some(merge(&inherited(), vars))),
        }
    }
}

/// Writes an overlay over a base environment
///
/// ## Returns
/// The pointer array, null terminated
fn merge(base: &[*mut libc::c_char], over: &[CString]) -> Vec<*mut libc::c_char> {
    let mut envp = Vec::with_capacity(base.len() + over.len() + 1);

    for entry in base {
        // Every pointer in `base` came from a zero terminated entry
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
/// Everything up to the first equals sign, or the whole entry
/// if there isn't one
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
/// The end kept here is `O_NONBLOCK`, since a write wake only
/// promises some room
///
/// ## Returns
/// The read end for the child, and the write end for here
fn input_pipe() -> Result<(Fd, Fd), RuntimeError> {
    let (read, write) = pipe()?;

    unsafe { libc::fcntl(write.0, libc::F_SETFL, libc::O_NONBLOCK) }.check()?;

    // Best effort, since a Rust host already ignores `SIGPIPE`
    let _ = unsafe { libc::fcntl(write.0, F_SETNOSIGPIPE, 1) };

    Ok((read, write))
}

/// What a child is handed for its three standard descriptors
struct Stdio {
    /// The read end of an input pipe, or `None` for `/dev/null`
    input: Option<libc::c_int>,

    /// The write ends for the child's output and error, or
    /// `None` to leave them this process's own
    capture: Option<(libc::c_int, libc::c_int)>,
}

/// Starts a child
///
/// `posix_spawn` rather than a `fork` and an `exec`, since
/// forking a multi threaded process is unsafe
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

        // Duplicated onto itself to exempt it from `CLOEXEC_DEFAULT`
        None => {
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, 1, 1) })?;
            spawn_check(unsafe { libc::posix_spawn_file_actions_adddup2(&mut actions.0, 2, 2) })?;
        }
    }

    // Last of the file actions, so nothing else resolves against
    // the caller's directory
    if let Some(dir) = dir {
        // Never run somebody's program somewhere other than where
        // they said
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

    // A null envp is an empty environment, not an inherited one
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
struct Feed<'a> {
    /// The write end, closed the moment the last byte lands
    ///
    /// A borrow, so an unwind still closes it
    end: &'a mut Option<Fd>,

    /// Every byte the child is to be given
    data: &'a [u8],

    /// How many of them it has taken
    sent: usize,

    /// The queue the watch sits on
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

    /// Gives the child one chunk at most, so a large input never
    /// holds the loop away from the streams it is also reading
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
    /// The unregister comes first, since a close hands the number
    /// straight back out while the kernel is still taking the
    /// watch down
    fn finish(&mut self) {
        let Some(end) = self.end.as_ref() else {
            return;
        };

        unwatch_write(self.queue, end.0);
        self.end.take();
    }
}

/// Takes the watch off on an unwind
impl Drop for Feed<'_> {
    fn drop(&mut self) {
        self.finish();
    }
}

/// The read watches, taken off however the loop ended
struct Reads<'a> {
    /// The queue the watches sit on
    queue: i32,

    /// The descriptors, `IGNORED` where there is no stream
    ends: &'a [libc::c_int; 2],

    /// Which of them are actually registered
    open: [bool; 2],
}

impl Drop for Reads<'_> {
    fn drop(&mut self) {
        for (slot, end) in self.ends.iter().enumerate() {
            if self.open[slot] {
                unwatch_read(self.queue, *end);
                self.open[slot] = false;
            }
        }
    }
}

/// Reads everything a child writes while giving it everything
/// it was to be given
///
/// One loop over up to three descriptors, which is the only
/// ordering that can't deadlock against the child
///
/// ## Returns
/// What the child wrote to each stream, in the order they were
/// given
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

    let mut reads = Reads {
        queue,
        ends: &ends,
        open: [false, false],
    };

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
            Ok(_) => reads.open[slot] = true,
            Err(error) => {
                outcome = Err(error);
                break;
            }
        }
    }

    if outcome.is_ok() {
        outcome = feed.watch();
    }

    // Recorded around the whole loop so a cancel can reach it
    if outcome.is_ok() {
        outcome = match executor::waiting_on(queue) {
            true => {
                let pumped = pump(queue, &ends, &mut reads.open, &mut found, &mut feed);

                // Also spins out a cancel still part way through its
                // syscalls against this queue
                match executor::stopped_waiting() {
                    true => pumped,
                    false => Err(RuntimeError::Cancelled),
                }
            }

            // Cancelled before the loop started
            false => Err(RuntimeError::Cancelled),
        };
    }

    // Through the guards, so an unwind takes the same route
    drop(reads);

    feed.finish();

    outcome?;

    let [stdout, stderr] = found;

    Ok((stdout, stderr))
}

/// The loop `exchange` runs once everything is watched
///
/// Ends when both streams have reported their last byte and the
/// input has been taken. A finished stream comes off the queue
/// at once, or it would read as ready forever
///
/// #### Note
/// A cancel's wake matches nothing below and falls through to
/// the cancellation check, so a stale one never cuts an
/// exchange short
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

/// The same exchange with `poll`, for when there is no queue
/// to wait on
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
            // Closing the end tells the child its input has finished
            input.take();
        }
    }

    let [stdout, stderr] = found;

    Ok((stdout, stderr))
}

/// Gives a descriptor one chunk at most, from a position
///
/// ## Returns
/// Whether there is anything left to give. `false` means either
/// everything has been taken, or the child stopped reading
fn write_chunk(fd: libc::c_int, data: &[u8], sent: &mut usize) -> Result<bool, RuntimeError> {
    let want = (data.len() - *sent).min(FILE_CHUNK);

    if want == 0 {
        return Ok(false);
    }

    // `sent` never passes `data.len()`
    let from = unsafe { data.as_ptr().add(*sent) }.cast::<libc::c_void>();

    let written = unsafe { libc::write(fd, from, want) }.check();

    let put = match written {
        Ok(put) => put as usize,

        Err(RuntimeError::CheckError(Some(libc::EINTR))) => return Ok(true),

        // The wake said there was room, not that it would still be there
        Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => return Ok(true),

        // The child stopped reading
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
        Err(RuntimeError::CheckError(Some(libc::EINTR))) => return Ok(true),
        Err(error) => return Err(error),
    };

    if got == 0 {
        return Ok(false);
    }

    // The kernel just wrote `got` bytes into the reserved capacity
    unsafe { into.set_len(into.len() + got) };

    Ok(true)
}

/// Waits for a child to end, and reaps it
///
/// The watch goes on before the child is first asked, so an
/// exit can't fall between the two
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

    // Nothing waits on a registration the kernel refused
    if watched.is_err() {
        return poll_exit(child);
    }

    child.watching(queue);

    match try_reap(pid) {
        Ok(Some(status)) => {
            child.reaped();
            unwatch_proc(queue, pid);

            return Ok(status);
        }

        Ok(None) => {}

        // A pid that can't be waited for must not be signalled either
        Err(error) => {
            child.reaped();
            unwatch_proc(queue, pid);

            return Err(error);
        }
    }

    // Cancelled before the wait started, so the watch comes
    // straight back off
    if !executor::waiting_on(queue) {
        unwatch_proc(queue, pid);

        return Err(RuntimeError::Cancelled);
    }

    // Every lap asks the child rather than believing the wake,
    // since a `WAKE_IDENT` can be left over from another task
    let mut outcome = None;

    loop {
        let waited = kqueue::wait_for(queue, pid as usize, libc::EVFILT_PROC);

        match try_reap(pid) {
            Ok(Some(status)) => {
                outcome = Some(Ok(status));
                break;
            }

            Err(error) => {
                outcome = Some(Err(error));
                break;
            }

            Ok(None) => {}
        }

        if waited == Waited::Cancelled || executor::cancelled() {
            outcome = Some(Err(RuntimeError::Cancelled));
            break;
        }

        // Nothing will arrive on this queue again, so this falls
        // back to polling
        if waited == Waited::Failed {
            break;
        }
    }

    // Also spins out a cancel still part way through its
    // syscalls against this queue
    let carry_on = executor::stopped_waiting();

    unwatch_proc(queue, pid);

    let Some(outcome) = outcome else {
        return poll_exit(child);
    };

    if !carry_on {
        return Err(RuntimeError::Cancelled);
    }

    // Reaped either way, so the guard never signals this pid
    child.reaped();

    outcome
}

/// Waits for a child to end without a queue to wait on
fn poll_exit(child: &mut Child) -> Result<ExitStatus, RuntimeError> {
    loop {
        match try_reap(child.pid) {
            Ok(Some(status)) => {
                child.reaped();

                return Ok(status);
            }

            Ok(None) => {}

            Err(error) => {
                child.reaped();

                return Err(error);
            }
        }

        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

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
/// `SIGKILL` to the group first, so a shell takes its children
/// with it, then to the child itself
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
/// Always before the descriptor is closed
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

/// Takes an exit watch back off a queue, along with anything
/// it already queued
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

    /// Both process tasks say they hold their thread
    #[test]
    fn every_process_task_says_it_blocks() {
        assert!(StatusTask::new("a", [""; 0]).blocking(), "run");
        assert!(OutputTask::new("a", [""; 0]).blocking(), "output");

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

    /// An argument with a zero byte in it doesn't convert
    #[test]
    fn an_argument_with_a_zero_byte_does_not_convert() {
        assert!(as_c_arg("a\0b").is_none(), "a zero byte must not convert");
        assert!(as_c_arg("ab").is_some(), "an ordinary argument must convert");
    }

    /// A bad path and a bad argument report as different errors
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

    /// An entry is split at its first equals sign
    #[test]
    fn an_entry_is_split_at_its_first_equals() {
        assert_eq!(key(b"A=B"), b"A", "the ordinary case");
        assert_eq!(key(b"A=B=C"), b"A", "a value may hold more of them");
        assert_eq!(key(b"NOEQUALS"), b"NOEQUALS", "a malformed entry is its own name");
        assert_eq!(key(b"=X"), b"", "an empty name is still where the split is");
    }

    /// A variable the kernel can't be given doesn't convert
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

    /// An overlay replaces a name rather than adding a second
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

        // An absolute program and a bare name are never joined
        let absolute = Program::new("/bin/sh", [""; 0]);
        let (file, _) = absolute.argv(Some(c"/usr")).expect("must convert");
        assert_eq!(file.to_bytes(), b"/bin/sh", "an absolute program is left alone");

        let looked_up = Program::new("sh", [""; 0]);
        let (file, _) = looked_up.argv(Some(c"/usr")).expect("must convert");
        assert_eq!(file.to_bytes(), b"sh", "a PATH lookup is left alone");
    }

    /// A pipe can be asked not to raise `SIGPIPE`
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
