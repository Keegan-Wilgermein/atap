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
    ffi::{CString, OsStr},
    mem,
    os::unix::ffi::OsStrExt,
    ptr,
    sync::Arc,
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

/// Where a child's standard input comes from
///
/// Never inherited. A child that reads stdin would otherwise be
/// competing with the parent for a terminal nobody told it
/// about, and `cat` with no arguments would hang the task
/// forever rather than finishing empty
const DEV_NULL: &std::ffi::CStr = c"/dev/null";

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
    fn argv(&self) -> Result<(&CString, Vec<*mut libc::c_char>), RuntimeError> {
        let file = self.file.as_ref().ok_or(RuntimeError::BadPath)?;
        let args = self.args.as_ref().ok_or(RuntimeError::BadArgument)?;

        let mut argv = Vec::with_capacity(args.len() + 2);

        // Every program is handed its own name as its first
        // argument. A caller passing one itself would find it
        // arriving twice, so the constructors take only the
        // arguments that come after it
        argv.push(file.as_ptr().cast_mut());
        argv.extend(args.iter().map(|arg| arg.as_ptr().cast_mut()));
        argv.push(ptr::null_mut());

        Ok((file, argv))
    }
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
        }
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
        }
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

        let pid = spawn_child(&self.program, None)?;
        let mut child = Child::new(pid);

        wait_exit(&mut child, kqueue::id().ok())
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

        let (out_read, out_write) = pipe()?;
        let (err_read, err_write) = pipe()?;

        let pid = spawn_child(&self.program, Some((out_write.0, err_write.0)))?;

        // Built before anything below can fail, so every way out
        // of here goes through its `Drop`
        let mut child = Child::new(pid);

        // The child has copies of its own now, and these have to
        // go *here* rather than at the end of the scope. A write
        // end still open in this process is one the pipe is
        // still waiting on, so leaving them to fall out of scope
        // would have the drain below waiting for an end that
        // this thread is itself holding back
        drop(out_write);
        drop(err_write);

        let queue = kqueue::id().ok();

        let (stdout, stderr) = match queue {
            Some(queue) => drain(queue, &out_read, &err_read),
            None => poll_drain(&out_read, &err_read),
        }?;

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
    streams: Option<(libc::c_int, libc::c_int)>,
) -> Result<libc::pid_t, RuntimeError> {
    let (file, argv) = program.argv()?;

    let mut actions = FileActions::new()?;
    let mut attr = SpawnAttr::new()?;

    spawn_check(unsafe {
        libc::posix_spawn_file_actions_addopen(
            &mut actions.0,
            0,
            DEV_NULL.as_ptr(),
            libc::O_RDONLY,
            0,
        )
    })?;

    match streams {
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
    let code = unsafe {
        libc::posix_spawnp(
            &mut pid,
            file.as_ptr(),
            &actions.0,
            &attr.0,
            argv.as_ptr(),
            *libc::_NSGetEnviron(),
        )
    };

    spawn_check(code)?;

    Ok(pid)
}

/// Reads everything from both of a child's streams
///
/// ## Behaviour
/// Both ends are watched at once, which is the only ordering
/// that works. Reading one to its end and then the other
/// deadlocks the moment a child fills the pipe this thread
/// isn't looking at — it blocks in `write`, this blocks in
/// `read`, and neither is coming back
///
/// The watches are level triggered, so a wake means the
/// descriptor has something on it and one ordinary blocking
/// read is safe. That is what keeps `O_NONBLOCK` and a loop
/// around `EAGAIN` out of this entirely
///
/// ## Returns
/// What the child wrote to each stream, in the order they were
/// given
fn drain(queue: i32, out: &Fd, err: &Fd) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
    let ends = [out.0, err.0];
    let mut found = [Vec::new(), Vec::new()];
    let mut open = [false, false];

    let mut outcome = Ok(());

    for (slot, end) in ends.iter().enumerate() {
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

    // Recorded once, around the whole loop, rather than once per
    // wake. This is the field a cancel reaches for, and a drain
    // that never wrote it would sit in `listen` with nothing able
    // to bring it back — a child that says nothing and doesn't
    // end would hold the thread for as long as it felt like
    if outcome.is_ok() {
        outcome = match executor::waiting_on(queue) {
            true => {
                let pumped = pump(queue, &ends, &mut open, &mut found);

                // Called whatever `pump` decided, because it also
                // spins out a cancel that is still part way
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

    // Whatever happened, nothing stays registered. A read watch
    // left on a queue this thread keeps is a descriptor number
    // that will be handed out again
    for (slot, end) in ends.iter().enumerate() {
        if open[slot] {
            unwatch_read(queue, *end);
        }
    }

    outcome?;

    let [stdout, stderr] = found;

    Ok((stdout, stderr))
}

/// The loop `drain` runs once both ends are watched
///
/// ## Behaviour
/// Ends when both descriptors have reported their last byte.
/// An end that has is taken off the queue immediately — a level
/// triggered descriptor sitting at its end reads as *ready*
/// every time, so leaving it on would turn the wait for the
/// other one into a spin
///
/// #### Note
/// The wake a cancel sends arrives here as an `EVFILT_USER`
/// event, which nothing below matches, so it falls through to
/// the cancellation check at the bottom of the batch. That is
/// deliberate: a wake left over from an earlier cancel would
/// otherwise cut a live drain short and lose output that had
/// already been written
fn pump(
    queue: i32,
    ends: &[libc::c_int; 2],
    open: &mut [bool; 2],
    found: &mut [Vec<u8>; 2],
) -> Result<(), RuntimeError> {
    let mut events = eventlist();

    while open[0] || open[1] {
        let count = match unsafe { KEvent::listen(queue, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        };

        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
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

/// Reads everything from both streams without a queue
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
fn poll_drain(out: &Fd, err: &Fd) -> Result<(Vec<u8>, Vec<u8>), RuntimeError> {
    let ends = [out.0, err.0];
    let mut found = [Vec::new(), Vec::new()];
    let mut open = [true, true];

    while open[0] || open[1] {
        if executor::cancelled() {
            return Err(RuntimeError::Cancelled);
        }

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
    }

    let [stdout, stderr] = found;

    Ok((stdout, stderr))
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
            bad_program.argv().unwrap_err(),
            RuntimeError::BadPath,
            "a zero byte in the program is a bad path"
        );

        assert_eq!(
            bad_argument.argv().unwrap_err(),
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
        let (file, argv) = program.argv().expect("an ordinary program must convert");

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
        let pid = spawn_child(&program, None).expect("sleep must spawn");

        let child = Child::new(pid);
        let started = Instant::now();

        drop(child);

        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the guard must kill rather than wait, took {:?}",
            started.elapsed()
        );
    }
}
