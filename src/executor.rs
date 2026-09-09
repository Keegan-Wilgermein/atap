//! # Executor
//! Owns every task slot in the process and manages the pool
//! of workers that run them
//!
//! The `Executor` owns every task slot in the process. A
//! `TaskHandle` holds nothing but an id and comes back
//! through here for everything, so there is exactly one
//! thing in the crate that can see a slot, and it is the
//! one thing that knows whether that slot is still alive
//!
//! It doesn't run anything itself. Spawning queues a task and
//! the workers come for it, so the manager is never in the way
//! of a task reaching a thread. What it does instead is the
//! part no worker can do for itself: decide the pool is too
//! small, decide it is too big, lift the oldest task out of the
//! way of everything overtaking it, and clear up after a worker
//! that went down
//!
//! #### Note
//! None of that is required for tasks to run. A pool with no
//! manager finds its own work, reverses its own queue and
//! clears up after its own dead. It stops adapting, it does not
//! stop working, and that is the whole reason a manager is
//! allowed to die

use crate::{
    Runtime, RuntimeError,
    constants::{
        DEAD_KQUEUE_ID, MANAGER_TICK, MANAGER_TICK_IDENT, MAX_TASK_ID, NO_TASK, RESTART_BACKOFF,
        RESTART_LIMIT, RESTART_WINDOW, SCHEDULE_IDENT_BASE, SHUTDOWN_POLL, WAKE_IDENT,
    },
    futures::task::Task,
    modules::{
        address_lock,
        erased_task::ErasedTask,
        event_desc::EventDesc,
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        series::SeriesTask,
        task_data::TaskData,
        task_handle::TaskHandle,
        task_setup::TaskSetup,
        task_state::TaskState,
        task_table::TaskTable,
        worker_pool::POOL,
    },
};
use libc::c_void;
use std::{
    cell::Cell,
    mem,
    panic::{self, AssertUnwindSafe},
    ptr,
    sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, Ordering},
    thread,
    time::{Duration, Instant},
};

/// Every task in the process, addressed by id
///
/// A plain static rather than anything thread local, because
/// the thread that spawns a task, the thread that runs it and
/// the thread that reads its result are three different
/// threads and all of them have to find the same slot
static DATA: TaskTable = TaskTable::new();

/// The kqueue the manager takes its tick from
///
/// Only `Relaxed` reads for speed, with a single `SeqCst`
/// write at initialisation and another if the manager
/// ever gives up, so that everything sees both
static EXECUTOR_KQUEUE_ID: AtomicI32 = AtomicI32::new(DEAD_KQUEUE_ID);

/// The order tasks have been spawned in
///
/// Stamped into a task at creation and never touched again.
/// The difference between this and a task's own stamp is how
/// many tasks have overtaken it, which is what aging measures
/// and what makes it free: no clock is read and nothing has to
/// walk a queue rewriting anything
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// The spawned task this thread is running, or `NO_TASK`
    ///
    /// Set around the run and nowhere else, so a task can be
    /// asked about from inside itself without every `Task`
    /// method having to carry the id down to wherever it is
    /// finally needed
    ///
    /// #### Note
    /// This is what tells a spawned task apart from a blocking
    /// one. `Runtime::block` never goes through `run`, so its
    /// thread reads `NO_TASK` and none of the cancellation
    /// machinery applies to it, which is the promise blocking
    /// calls make
    static CURRENT: Cell<usize> = const { Cell::new(NO_TASK) };
}

/// Whether the runtime has been shut down
///
/// One way. Nothing clears this, because a shutdown gives back
/// every slot in the table and starting again would hand those
/// ids to new tasks while old handles still hold them
///
/// `SeqCst` throughout — it is read once per manager loop and
/// written once in the life of a process, so nothing here is
/// worth being clever about
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Whether somebody has asked the runtime to stop
#[inline(always)]
pub(crate) fn shutting_down() -> bool {
    SHUTDOWN.load(Ordering::SeqCst)
}

/// Stops the runtime for good
///
/// ## Behaviour
/// Drains rather than aborts. Nothing new gets in from the
/// moment this starts, and everything already queued still
/// runs — workers pull their own work and stop only between
/// tasks, so a task in flight is never interrupted and a task
/// waiting its turn still gets one
///
/// Blocks until the pool has nothing left to do, so a caller
/// that comes back from this knows the work is finished rather
/// than merely asked to finish
///
/// The manager's queue is closed first, before the drain rather
/// than after it. Both re-arm paths check it, so a repeating
/// task that publishes part way through the drain ends its
/// series there instead of putting itself back on a pool that
/// is trying to empty — which is what stops the drain being a
/// wait for something that keeps renewing itself
///
/// ## Returns
/// Nothing, and it cannot fail. A second caller finds the flag
/// already set and comes straight back, rather than tearing
/// down a runtime somebody else is already tearing down
///
/// #### Note
/// The one way this doesn't come back is a task that never
/// finishes. Draining means waiting for the work, and a task
/// that runs forever is work that never ends — the same task
/// would have held a worker for the life of the process
/// anyway, this is just where it becomes visible
///
/// The same goes for calling this from inside a spawned task.
/// The caller is one of the things keeping the pool busy, so
/// the drain would be waiting on the thread doing the waiting
pub(crate) fn shutdown_now() {
    // Claimed and checked in one operation, so two threads
    // arriving together can't both go through it
    if SHUTDOWN.swap(true, Ordering::SeqCst) {
        return;
    }

    // Nothing new gets in past here. A spawn after this settles
    // `Failed` with its slot given straight back, down the same
    // path a spawn onto a dead pool has always taken
    POOL.stop_permanently();

    let manager = EXECUTOR_KQUEUE_ID.swap(DEAD_KQUEUE_ID, Ordering::SeqCst);

    // Woken rather than closed. The manager may be sitting in
    // `kevent` on this descriptor at this very moment, so the
    // supervisor closes it once that thread has actually gone
    if manager != DEAD_KQUEUE_ID {
        let _ = unsafe {
            KEvent::register(
                manager,
                WAKE_IDENT,
                0,
                ptr::null_mut(),
                EventDesc::new_user_trigger(),
            )
        }
        .check();
    }

    // Everything already queued still runs
    while POOL.stats().has_any_task() {
        thread::sleep(SHUTDOWN_POLL);
    }

    POOL.stop_all();
    POOL.abandon();

    // Both halves. A blocking task stranded in its own queue
    // has listeners waiting on it exactly like any other, and
    // nothing else is going to come for it now
    for task in POOL
        .injector()
        .drain()
        .into_iter()
        .chain(POOL.blocking().drain())
    {
        fail(task);
    }

    // Everything the runtime is still holding a reference on:
    // a schedule whose queue has closed, a repeat between runs,
    // a task in the ring of a worker that went down
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        let state = data.state();

        // A thread is inside this one and gives the reference
        // back itself on the way out. A series is the exception,
        // because the thread inside one is a run publishing into
        // it rather than the series itself, and that run holds a
        // claim of its own — so the count can't reach zero while
        // it is still writing
        if state == TaskState::Running && !data.kind().schedules() {
            continue;
        }

        if !state.terminal() {
            data.set_state(TaskState::Failed);
            wake(data);
        }

        release(task);
    }
}

/// Manager deaths still owed
///
/// The restart path has no other way to be reached. A manager
/// only ever dies of a kernel refusing it a syscall or of a bug
/// in this crate, and a test can ask for neither — so the one
/// piece of machinery whose whole job is surviving a failure
/// would otherwise be the one piece nothing ever exercises
///
/// Zero in every run that hasn't asked for otherwise, so what
/// it costs a healthy manager is a single relaxed read on a
/// loop that goes round ten times a second
static INJECTED_FAULTS: AtomicU32 = AtomicU32::new(0);

/// Makes the manager come apart the next `count` times it goes
/// round its loop
///
/// Fewer than `RESTART_LIMIT` and the supervisor brings it back
/// every time. More and it gives up, closes its queue, and
/// everything that was depending on that queue has to be
/// written off — which is the half worth testing, because it is
/// the half that strands tasks if it is wrong
pub(crate) fn inject_manager_faults(count: u32) {
    INJECTED_FAULTS.store(count, Ordering::SeqCst);
}

/// Takes one of the owed deaths, if any are owed
fn injected_fault() -> bool {
    INJECTED_FAULTS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| match left {
            0 => None,
            _ => Some(left - 1),
        })
        .is_ok()
}

/// Async task executor and handler
pub(crate) struct Executor;

impl Executor {
    /// Initialises a new `Executor`
    ///
    /// The kqueue and the first workers are both created here,
    /// on the calling thread, so that a task spawned the
    /// instant `Runtime::init()` returns has somewhere to be
    /// delivered and something to run it. Only the supervisor
    /// is put on a thread of its own
    pub(crate) fn init() -> Option<RuntimeError> {
        let id = match unsafe { libc::kqueue() }.check() {
            Ok(id) => id,
            Err(error) => return Some(error),
        };

        EXECUTOR_KQUEUE_ID.store(id, Ordering::SeqCst);

        POOL.ensure_floor();
        supervise(id);

        None
    }

    /// Adds a new `Task` to be processed
    ///
    /// Publishing the slot before queueing it matters. A worker
    /// can be looking the id up the moment it is queued, so the
    /// task has to be findable before anything is told to go
    /// looking for it
    pub(crate) fn new_task<F>(task: F, setup: TaskSetup) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        create(task, setup).0
    }

    /// Adds a schedule that starts a fresh copy of a task on
    /// the interval, whether the last one has finished or not
    ///
    /// ## Behaviour
    /// The slot this makes is not a task. It holds no
    /// `ErasedTask`, is never queued and is never run — what it
    /// holds is the prototype the runs are cloned from, and a
    /// place for whichever run finished most recently to leave
    /// its output. The handle points at it for the life of the
    /// series, which is what makes one handle mean the whole
    /// schedule rather than one run of it
    ///
    /// The first run goes now rather than an interval from now,
    /// the same way every other spawn starts as soon as it can.
    /// It is launched from this thread, before the timer exists
    /// at all, so the prototype is handed over to the manager
    /// rather than shared with it
    pub(crate) fn new_series<F>(task: F, setup: TaskSetup) -> TaskHandle<F::Output>
    where
        F: Task + Clone,
    {
        let boxed: Box<dyn SeriesTask> = Box::new(task);
        let prototype = Box::into_raw(Box::new(boxed)).cast::<c_void>();

        let Some(id) = DATA.alloc() else {
            return abandoned(prototype);
        };

        let Some(entry) = DATA.slot(id) else {
            DATA.free(id);
            return abandoned(prototype);
        };

        let ready = unsafe {
            TaskData::init::<F::Output>(
                entry as *const TaskData as *mut TaskData,
                ptr::null_mut(),
                TaskState::Pending,
                setup,
                SEQUENCE.fetch_add(1, Ordering::Relaxed),
            )
        };

        if !ready {
            DATA.free(id);
            return abandoned(prototype);
        }

        // After `init`, which writes the whole header over the
        // top of everything, and before anything else can reach
        // the slot. No timer is armed and no handle exists yet,
        // so this thread is still alone with it
        entry.set_prototype(prototype);

        let handle = TaskHandle::new(id);

        // A schedule with a delay on it doesn't start now. The
        // first run and the repeating timer both wait on the
        // one shot armed here, and the first tick puts the
        // period on the queue once the delay has been served
        let started = match setup.start_delay.as_nanos() as u64 {
            0 => launch(id, entry) && schedule(id, setup.interval.as_nanos() as u64),
            delay => wait_for(entry, id, delay),
        };

        if started {
            return handle;
        }

        // Either nothing could run it or the kernel wouldn't
        // take the schedule, and a series with no schedule is a
        // slot that will never do anything again
        unschedule(id);

        if entry.try_state(TaskState::Pending, TaskState::Failed) {
            wake(entry);
        }

        release(id);

        handle
    }

    /// Adds 1 to the listener count on a piece of data
    ///
    /// This is so multiple listeners can be on the same object
    /// while preventing the data from getting cleaned up early
    pub(crate) fn add_listener(id: usize) {
        let Some(data) = slot(id) else {
            return;
        };

        data.add_listener();
    }

    /// Takes 1 off the listener count on a piece of data
    ///
    /// The thread that takes the count to zero is by
    /// definition the only one that can still see the slot,
    /// so it is the one that does the freeing, alone and
    /// without needing to exclude anybody
    pub(crate) fn drop_listener(id: usize) {
        let Some(data) = slot(id) else {
            return;
        };

        if !data.drop_listener() {
            return;
        }

        // Drops the task, the output and any mapping the
        // output needed, and leaves the slot reading `Free`
        unsafe { data.destroy() };

        // Only once it is empty, since the id and the memory
        // behind it are both live again the moment this lands
        DATA.free(id);
    }

    /// Whether a task has settled and will not run again
    ///
    /// ## Behaviour
    /// The two halves matter separately. A repeat between runs
    /// has settled and *will* run again; a bounded one that has
    /// reached its ending has settled and won't. Both read
    /// `Ready`, so the state alone can't tell them apart — the
    /// kind is what does, because a series that ran out has had
    /// its kind flipped to `Once`
    ///
    /// #### Note
    /// An id with no slot behind it answers `true`. Whatever it
    /// was, it is certainly not going to run again
    pub(crate) fn finished(id: usize) -> bool {
        let Some(data) = slot(id) else {
            return true;
        };

        let state = data.state();

        match state {
            // Ends for everything, whatever kind it was. A
            // cancelled series is over even though its kind
            // still says it repeats
            TaskState::Cancelled | TaskState::Failed => true,

            // Ends only for something that wasn't going round
            // again. This is the pair a repeat sits in between
            // runs, and the pair a bounded one is left in when
            // it runs out
            _ => state.terminal() && !data.kind().repeats(),
        }
    }

    /// The state a task is currently in
    pub(crate) fn state(id: usize) -> TaskState {
        match slot(id) {
            Some(data) => data.state(),
            None => TaskState::Failed,
        }
    }

    /// Blocks until a task settles, and says how it settled
    ///
    /// Waits for as long as it takes. `wait_until` is the same
    /// wait with somewhere to stop
    pub(crate) fn wait(id: usize) -> Result<TaskState, RuntimeError> {
        Self::wait_until(id, None)
    }

    /// Blocks until a task settles or a deadline passes
    ///
    /// ## Returns
    /// How it settled, or `NotReady` if the deadline came
    /// first. `None` for the deadline is a wait with nowhere to
    /// stop, which is what `wait` asks for
    ///
    /// ## Behaviour
    /// The wait sleeps only while the state word still reads
    /// the value it was given, so a state that has already
    /// moved on doesn't sleep at all and the loop simply looks
    /// again
    ///
    /// What is left of the deadline is worked out fresh on
    /// every pass rather than handed to the kernel once. A
    /// signal, a spurious wake and a state that moved without
    /// settling all send this round again, and giving each of
    /// them the whole timeout over again would let a stream of
    /// them hold a caller long past the moment it asked to stop
    /// waiting
    pub(crate) fn wait_until(
        id: usize,
        deadline: Option<Instant>,
    ) -> Result<TaskState, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::NoSuchTask);
        };

        loop {
            let state = data.state();

            if state.terminal() {
                return Ok(state);
            }

            let Some(deadline) = deadline else {
                address_lock::wait(data.wait_address(), state as u32)?;
                continue;
            };

            let left = deadline.saturating_duration_since(Instant::now());

            // Out of time. Asked after the state read above, so
            // a task that settled on the way round here still
            // comes back with its answer rather than a timeout
            if left.is_zero() {
                return Err(RuntimeError::NotReady);
            }

            if address_lock::wait_until(data.wait_address(), state as u32, left)? {
                continue;
            }

            // The time ran out, so the word is read once more
            // before giving up on it. A settle landing in the
            // same moment as the timeout is still a settle, and
            // the caller would rather have it than not
            let state = data.state();

            if state.terminal() {
                return Ok(state);
            }

            return Err(RuntimeError::NotReady);
        }
    }

    /// Waits for a task and clones its output
    ///
    /// The output stays owned by the slot, which is what lets
    /// every listener have one
    pub(crate) fn clone_result<T>(id: usize) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Self::clone_result_until(id, None)
    }

    /// The same read, with somewhere to stop waiting
    ///
    /// ## Returns
    /// The output, or `NotReady` if the deadline passed before
    /// there was one. Any other error is the task's own and
    /// waiting longer wouldn't have helped
    pub(crate) fn clone_result_until<T>(
        id: usize,
        deadline: Option<Instant>,
    ) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::NoSuchTask);
        };

        loop {
            settled(data, Self::wait_until(id, deadline)?)?;

            // Held for as long as the clone takes, so that a
            // `take` on another thread waits rather than moving
            // the output away part way through reading it
            if data.enter_read() {
                break;
            }

            // A repeating task can have started, finished, and
            // published all over again between the wait and
            // here, so `terminal` is the wrong question — being
            // settled isn't the same as being over. `lost`
            // knows the difference: everything it calls
            // `NotReady` is a race worth going back round for
            let error = lost(data, data.state());

            if error != RuntimeError::NotReady {
                return Err(error);
            }

            // Losing that race costs another go round, and a
            // caller that put a deadline on this has only as
            // many goes as the deadline leaves room for
            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(RuntimeError::NotReady);
            }
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        let value = unsafe { (*data.payload().cast::<T>()).clone() };
        data.leave_read();

        Ok(value)
    }

    /// Reads the output if it is there, without waiting
    ///
    /// The same read `clone_result` does, minus the waiting. A
    /// task that hasn't settled says so rather than blocking,
    /// which is the whole difference between polling a handle
    /// and joining one
    pub(crate) fn poll_result<T>(id: usize) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::NoSuchTask);
        };

        if !data.enter_read() {
            return Err(lost(data, data.state()));
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        let value = unsafe { (*data.payload().cast::<T>()).clone() };
        data.leave_read();

        Ok(value)
    }

    /// Moves the output out if it is there, without waiting
    ///
    /// The same move `take_result` does, minus the waiting. A
    /// task that hasn't settled says so rather than blocking,
    /// which is the difference between polling a handle and
    /// taking from one
    ///
    /// #### Note
    /// `claim_result` only wins against `Ready`, so a task
    /// still running turns this away on its own and nothing
    /// here has to ask whether it settled first
    pub(crate) fn poll_take<T>(id: usize) -> Result<T, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::NoSuchTask);
        };

        if !data.claim_result() {
            return Err(lost(data, data.state()));
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        data.empty();

        Ok(unsafe { ptr::read(data.payload().cast::<T>()) })
    }

    /// Waits for a task and moves its output out
    ///
    /// Winning the move to `Taken` is what makes this the one
    /// caller that owns the output, and what makes every
    /// later read fail rather than hand out a second owner of
    /// the same value
    pub(crate) fn take_result<T>(id: usize) -> Result<T, RuntimeError> {
        Self::take_result_until(id, None)
    }

    /// The same move, with somewhere to stop waiting
    ///
    /// ## Returns
    /// The output, or `NotReady` if the deadline passed before
    /// there was one
    ///
    /// #### Note
    /// A deadline that passes leaves the output where it is.
    /// Nothing was claimed, so a later read still finds it and
    /// running out of patience costs the caller nothing but the
    /// wait
    pub(crate) fn take_result_until<T>(
        id: usize,
        deadline: Option<Instant>,
    ) -> Result<T, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::NoSuchTask);
        };

        loop {
            settled(data, Self::wait_until(id, deadline)?)?;

            if data.claim_result() {
                break;
            }

            // Same race as `clone_result`, and the same answer
            let error = lost(data, data.state());

            if error != RuntimeError::NotReady {
                return Err(error);
            }

            if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                return Err(RuntimeError::NotReady);
            }
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        data.empty();

        Ok(unsafe { ptr::read(data.payload().cast::<T>()) })
    }

    /// Abandons a task
    ///
    /// ## Behaviour
    /// A task that hasn't started never will. One already
    /// sitting in a kernel wait is taken back out of it, so the
    /// thread it was holding comes back rather than waiting out
    /// a timer nobody is interested in any more. Either way its
    /// output is dropped instead of published
    ///
    /// A repeating task ends the whole series. The run in
    /// flight finishes and its output goes nowhere, and there
    /// is no run after it
    ///
    /// #### Note
    /// Only spawned tasks. `Runtime::block` runs on the
    /// caller's own thread and has no id to be cancelled
    /// through, which is the promise it makes
    ///
    /// #### Note
    /// Cancelling never touches the output, even when it has
    /// already landed. Dropping it here would be dropping it
    /// underneath a listener that is part way through reading
    /// it, so it is left to the last listener out
    pub(crate) fn cancel(id: usize) {
        let Some(data) = slot(id) else {
            return;
        };

        let repeats = data.kind().repeats();

        loop {
            let state = data.state();

            match state {
                // Over already, one way or another
                TaskState::Cancelled | TaskState::Failed | TaskState::Free => return,

                // A one shot whose output has been moved out is
                // finished and has nothing left to abandon. A
                // repeating one in the same state is only
                // between runs, and the series is still going
                TaskState::Taken if !repeats => return,

                _ => {}
            }

            if data.try_state(state, TaskState::Cancelled) {
                wake(data);
                interrupt(data, id);

                return;
            }
        }
    }
}

/// Puts a task in the table and hands it to the pool
///
/// ## Returns
/// The handle, and whether anything is ever going to pick the
/// task up. A `false` is a task already settled `Failed` with
/// its slot given back, so the handle reads an error rather
/// than blocking on a result that isn't coming
///
/// #### Note
/// Split out from `new_task` rather than folded into it because
/// a series has to know whether its run actually got away. The
/// handle alone can't say: a run that finished and failed on
/// its own reads exactly like one that was never queued
fn create<F>(task: F, setup: TaskSetup) -> (TaskHandle<F::Output>, bool)
where
    F: Task,
{
    // Asked here, where the task is still itself, rather
    // than by the worker that picks it up. A re-arm has no
    // concrete type left to ask, so the answer is kept
    let setup = setup.blocking(task.blocking());

    let boxed: Box<dyn ErasedTask> = Box::new(task);
    let erased = Box::into_raw(Box::new(boxed)).cast::<c_void>();

    let Some(id) = DATA.alloc() else {
        return (failed(erased), false);
    };

    let Some(entry) = DATA.slot(id) else {
        DATA.free(id);
        return (failed(erased), false);
    };

    let ready = unsafe {
        TaskData::init::<F::Output>(
            entry as *const TaskData as *mut TaskData,
            erased,
            TaskState::Pending,
            setup,
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        )
    };

    if !ready {
        DATA.free(id);
        return (failed(erased), false);
    }

    let handle = TaskHandle::new(id);

    // Armed rather than queued when there is a delay on it, so
    // the first run happens when the delay is up instead of
    // now. Either way somebody is coming for it, which is every
    // case but a pool that is gone and won't restart, or a
    // kernel that wouldn't take the timer
    //
    // Read from the setup rather than passed in, because a
    // delay is no longer only a one shot's business — a repeat
    // can be given one too, and then it waits this out before
    // the first run and `interval` between the ones after it
    let started = match setup.start_delay.as_nanos() as u64 {
        0 => queue(id, setup.blocking),
        delay => wait_for(entry, id, delay),
    };

    if started {
        return (handle, true);
    }

    // Nothing is ever going to pick this up, so it is
    // settled here rather than left for a listener to block
    // on forever, and the reference every task holds for
    // the `Executor` is given back by the run that will
    // never happen
    if let Some(published) = slot(id) {
        published.set_state(TaskState::Failed);
        wake(published);
    }

    release(id);

    (handle, false)
}

/// Spawns one run of a series
///
/// The handle is dropped on the way out, because a run has
/// nobody waiting on it — whatever it produces goes to the
/// series slot rather than to a listener. The `Executor`'s own
/// reference is what keeps the run's slot alive until it
/// finishes, exactly as it does for any other task
///
/// ## Returns
/// Whether a run is on its way
pub(crate) fn spawn_run<F>(task: F, priority: u8) -> bool
where
    F: Task,
{
    create(task, TaskSetup::once(priority)).1
}

/// Leaves an output in a series slot for its listeners
///
/// ## Behaviour
/// The same ending `run` gives an ordinary task, from the other
/// side of the erasure. Winning the move into `Running` is what
/// stops any further read from starting, which is what makes it
/// safe to throw the last run's output away and write this one
/// over the top of it
///
/// A value that can't be published is dropped here. That covers
/// a cancelled series, which has nothing left to publish into,
/// and two runs finishing together, where the one that loses
/// the race is simply a run whose output nobody sees. Runs of a
/// series overlap by design, so which of them is "latest" was
/// never going to be more precise than this
pub(crate) fn publish<T>(id: usize, value: T) {
    let Some(data) = slot(id) else {
        return;
    };

    if !data.begin() {
        return;
    }

    debug_assert_eq!(data.size(), mem::size_of::<T>());

    unsafe { data.payload().cast::<T>().write(value) };
    data.fill();

    // Cancelled part way through the write, so the value is
    // left for the last listener out to drop rather than
    // published to listeners that have already given up
    if !data.try_state(TaskState::Running, TaskState::Ready) {
        return;
    }

    wake(data);
}

/// The slot for an id, if there is a task in it
///
/// A slot that has never been used, or whose last listener
/// has gone, reads as `Free`. Filtering it out here is what
/// stops a stale id from finding the task that took its place
#[inline(always)]
pub(crate) fn slot(id: usize) -> Option<&'static TaskData> {
    let data = DATA.slot(id)?;

    if data.state() == TaskState::Free {
        return None;
    }

    Some(data)
}

/// Whether the manager still has a queue to work from
///
/// False once its supervisor has given up on it, and false the
/// moment a shutdown starts
#[inline(always)]
pub(crate) fn manager_alive() -> bool {
    EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed) != DEAD_KQUEUE_ID
}

/// Task slots the table has ever handed out
///
/// Climbs only when no retired slot could be reused, so it is
/// the peak number of tasks alive at once rather than the
/// number ever spawned
#[inline(always)]
pub(crate) fn slots() -> usize {
    DATA.high_water()
}

/// Tasks holding a slot right now
#[inline(always)]
pub(crate) fn live() -> usize {
    DATA.live()
}

/// Gives back the pages behind the unused top of the table
#[inline(always)]
pub(crate) fn trim() -> Result<usize, RuntimeError> {
    DATA.trim()
}

/// How many tasks have been spawned so far
///
/// A task's age is the difference between this and the stamp
/// it was created with
#[inline(always)]
pub(crate) fn sequence() -> u64 {
    SEQUENCE.load(Ordering::Relaxed)
}

/// Runs one task and publishes what comes back
///
/// ## Behaviour
/// Claiming is what makes a task run at most once. A second
/// caller for the same id finds nothing, and leaves the
/// `Executor`'s reference alone for the run that did claim it
/// to give back
///
/// A task that panics is contained here. It settles as
/// `Failed`, its listeners are let go, and the thread that was
/// running it carries straight on — rerunning it isn't possible
/// anyway, since claiming took the `Task` out of the slot and
/// the unwind dropped it
///
/// #### Note
/// This is not what makes the pool survive a panic. The drop
/// guards on the worker and sleep loops still do that, for a
/// panic somewhere in the pool's own machinery rather than in
/// a task. This is what stops the far more likely of the two
/// from costing a thread
pub(crate) fn run(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    let raw = data.claim();

    if raw.is_null() {
        return;
    }

    let mut task = unsafe { Box::from_raw(raw.cast::<Box<dyn ErasedTask>>()) };

    // Cancelled, failed, or already taken by somebody else, so
    // it is dropped rather than run and nothing is published
    if !data.begin() {
        drop(task);
        release(id);

        return;
    }

    // A run has started, so no delay is owed before one any
    // more. Written on every run rather than only the first,
    // because storing zero over zero is cheaper than the branch
    // that would avoid it — and it is what tells a restarted
    // manager that the wait it is putting back is a gap rather
    // than a start delay
    data.clear_start_delay();

    let reactor = Runtime::reactor_id();
    let payload = data.payload();

    // Caught rather than allowed to take the thread with it. A
    // task that goes down is one task going down; the worker
    // carries on with the next one, and nothing has to be
    // recovered, replaced or spawned to make up for it
    //
    // `AssertUnwindSafe` because nothing survives to see a
    // half finished task. The slot is settled below, its output
    // is never read, and the box is dropped on the way out
    // without ever being run again
    CURRENT.with(|current| current.set(id));

    let finished = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        task.run(reactor, id, payload)
    }))
    .is_ok();

    CURRENT.with(|current| current.set(NO_TASK));

    if !finished {
        // Writing the output is the last thing a task does, so
        // a panic means it was never written and there is no
        // value in the payload to drop. `filled` stays false
        // and the last listener out leaves it alone
        //
        // A series ends here too. Whatever the task was, it
        // came apart part way through, and running it again is
        // not a way of finding out whether it would do it twice
        drop(task);

        if data.try_state(TaskState::Running, TaskState::Failed) {
            wake(data);
        }

        release(id);

        return;
    }

    data.fill();

    // A listener that cancelled part way through isn't coming
    // back for this, so the output is left for the last one
    // out to drop rather than published
    if !data.try_state(TaskState::Running, TaskState::Ready) {
        drop(task);
        release(id);

        return;
    }

    wake(data);

    if !data.kind().repeats() {
        drop(task);
        release(id);

        return;
    }

    // The bound is asked here, before the task goes back in
    // its slot, so a run that isn't wanted is never queued and
    // no timer is ever armed for it
    //
    // The count first, because it is two loads against a clock
    // read — and a series ended by its count doesn't need to
    // know what time it is
    let gap = match data.kind().waits() {
        true => Duration::from_nanos(data.interval()),
        false => Duration::ZERO,
    };

    if data.count_run() || data.past_deadline(gap) {
        drop(task);
        data.finish_series();
        release(id);

        return;
    }

    // Round again, in the same slot, with the same box. The
    // state is left at `Ready` so listeners can read the run
    // that just finished while the next one is queued, and the
    // `Executor`'s reference is held rather than given back,
    // because it stands for the series and not for one run
    data.rearm(Box::into_raw(task).cast::<c_void>());

    // A timed one waits on the kernel rather than on a thread,
    // so nothing of the pool's is tied up for the interval
    let armed = match data.kind().waits() {
        true => wait_out(data, id),
        false => queue(id, data.blocking()),
    };

    if armed {
        return;
    }

    // Nothing is left to run it again, so the series ends the
    // way a task spawned onto a dead pool does
    if data.try_state(TaskState::Ready, TaskState::Failed) {
        wake(data);
    }

    release(id);
}

/// Hands a task to whichever half of the pool should have it
///
/// ## Returns
/// Whether anything will come for it
#[inline(always)]
fn queue(id: usize, blocking: bool) -> bool {
    match blocking {
        true => POOL.offload(id),
        false => POOL.submit(id),
    }
}

/// Puts a task down until its interval is up
///
/// ## Returns
/// Whether the kernel took the timer. A refusal ends the
/// series, because a task waiting on a timer that was never
/// armed waits for good
///
/// ## Behaviour
/// The wait costs nothing at all. No worker is held, no sleep
/// thread is held, and the pool sees the task as gone rather
/// than as one of its threads sitting still — which matters,
/// because a worker that stops finishing tasks is exactly what
/// the pool reads as stuck and grows itself to make up for
///
/// The slot is marked before the timer exists, so that a
/// manager which dies between here and the wake arriving can
/// find the wait again and put it back. Marking afterwards
/// would leave a window where the timer is out there and
/// nothing in the table says so
fn wait_out(data: &TaskData, id: usize) -> bool {
    wait_for(data, id, data.interval())
}

/// Puts a task down for a given number of nanoseconds
///
/// Split out from `wait_out` because the two waits a task can
/// be put down for are different durations. A gap between runs
/// is `interval`; a delay before the first run is
/// `start_delay`, and the slot has to hold both at once for a
/// repeat that was given a delay
///
/// ## Returns
/// Whether the kernel took the timer. A refusal ends the
/// series, because a task waiting on a timer that was never
/// armed waits for good
fn wait_for(data: &TaskData, id: usize, nanos: u64) -> bool {
    data.arm();

    if arm_timer(id, nanos) {
        return true;
    }

    data.disarm();

    false
}

/// Puts a one shot timer on the manager's queue for a task
///
/// ## Behaviour
/// The ident is shifted clear of the ones this crate keeps for
/// itself, since a kqueue keys an event on its ident and filter
/// together and the manager's own tick is a timer on this very
/// queue
///
/// #### Note
/// `EV_ADD` replaces whatever was on the ident rather than
/// adding beside it, which is what makes re-arming a wait that
/// may still be armed safe to do
fn arm_timer(id: usize, interval: u64) -> bool {
    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return false;
    }

    unsafe {
        KEvent::register(
            manager,
            id + SCHEDULE_IDENT_BASE,
            interval as libc::intptr_t,
            ptr::null_mut(),
            EventDesc::new_timer(),
        )
    }
    .check()
    .is_ok()
}

/// Puts a task whose interval is up back on the queue
///
/// ## Behaviour
/// Nothing is checked for a repeating task. A series cancelled
/// while it waited still comes back through this, and `run`
/// turns it away the same way it turns away anything else that
/// was cancelled before it started — which is one path rather
/// than two
///
/// A schedule is the other thing that lands here, and it is the
/// opposite case: its own slot never goes anywhere near a
/// worker, so what the tick does is start a run of it
fn fire(ident: usize) {
    let Some(id) = ident.checked_sub(SCHEDULE_IDENT_BASE) else {
        return;
    };

    let Some(data) = slot(id) else {
        return;
    };

    if data.kind().schedules() {
        // A delayed schedule's first wake is an armed one shot,
        // so it is claimed the same way every other wait is. A
        // manager that put the wake back after losing it would
        // otherwise start the schedule twice
        if data.armed() && !data.claim_armed() {
            return;
        }

        tick(id, data);

        return;
    }

    // Only the caller that takes the wake puts the task back. A
    // manager that went down holding this one may have left a
    // replacement timer behind it, and one task can only be in
    // one queue once
    if !data.claim_armed() {
        return;
    }

    if queue(id, data.blocking()) {
        return;
    }

    // Nothing is left to run it, so this ends the way it would
    // have if the timer itself had been refused
    //
    // Two states can be sitting here. A repeat between runs is
    // `Ready` or `Taken`, having published at least once. A
    // delayed one shot is still `Pending`, because the wake it
    // was waiting on was to be its first run — and a `Pending`
    // task left alone is a listener blocked on something
    // nothing is going to pick up
    if data.try_state(TaskState::Ready, TaskState::Failed)
        || data.try_state(TaskState::Pending, TaskState::Failed)
    {
        wake(data);
    }

    release(id);
}

/// Starts the next run of a series, or clears the series up
///
/// ## Behaviour
/// A schedule is taken off the queue here rather than at the
/// moment somebody cancels it. The manager is the only thread
/// that touches this queue's timers, so doing it from in here
/// is the difference between one thread owning them and a
/// canceller racing a tick for the same ident
///
/// The cost of that is a slot held until the next tick would
/// have come round anyway, which is the same deal `repeat_every`
/// makes and worth knowing about for a long interval
///
/// #### Note
/// A run failing to get away ends the schedule. It means the
/// table had no slot to give or nothing is left to run
/// anything, and a schedule that can't produce runs is a slot
/// waking the manager forever to do nothing
fn tick(id: usize, data: &TaskData) {
    // The wake that brought a delayed schedule here was its
    // start delay, not its period. The repeating timer that
    // keeps it going has never been armed, so it is armed now —
    // and the delay is marked spent so this only happens once
    if data.start_delay() != 0 {
        data.clear_start_delay();

        if !schedule(id, data.interval()) {
            end_schedule(id, data);

            return;
        }
    }

    // Both bounds, before the launch rather than after it. A
    // run that would begin past the deadline is never begun,
    // and a schedule that has used every run it was allowed has
    // nothing left to start
    if data.past_deadline(Duration::from_nanos(data.interval())) || !data.runs_remain() {
        end_schedule(id, data);

        return;
    }

    if !over(data.state()) && launch(id, data) {
        // That launch spent one of them. A schedule counts what
        // it *starts* rather than what finishes, because its
        // runs overlap and the last to start is not the last to
        // end
        if data.count_run() {
            end_schedule(id, data);
        }

        return;
    }

    unschedule(id);

    // Left alone if it has already settled, so a cancelled
    // series stays cancelled and one that published a last
    // output keeps it readable. Only a series that never got
    // anywhere is written off
    if !data.state().terminal() {
        data.set_state(TaskState::Failed);
        wake(data);
    }

    release(id);
}

/// Takes a schedule off the clock because it is finished
///
/// ## Behaviour
/// Distinct from the failure path in `tick`, and the difference
/// matters. This is a schedule that did exactly what it was
/// asked and stopped, so its state is left alone and its last
/// output stays readable — only the kind is flipped, which is
/// what stops anything treating it as a schedule again
fn end_schedule(id: usize, data: &TaskData) {
    unschedule(id);
    data.finish_series();
    release(id);
}

/// Whether a series has come to an end
///
/// `Ready` and `Taken` are not endings here. They are what a
/// series looks like between runs, which is most of its life
#[inline(always)]
fn over(state: TaskState) -> bool {
    matches!(state, TaskState::Cancelled | TaskState::Failed)
}

/// Spawns one run of the series in a slot
///
/// ## Returns
/// Whether a run is on its way. A slot with no prototype in it
/// is not a series at all, and says no
fn launch(id: usize, data: &TaskData) -> bool {
    let prototype = data.prototype();

    if prototype.is_null() {
        return false;
    }

    // Only ever the manager, or the thread that made the series
    // before the manager could see it, so the prototype is
    // never actually shared with anybody
    let task = unsafe { &**prototype.cast::<Box<dyn SeriesTask>>() };

    task.launch(id, data.priority_class())
}

/// Puts a series on the clock
///
/// A repeating timer rather than a chain of one shots, so the
/// kernel keeps the cadence itself and a run that takes longer
/// than the interval costs the schedule nothing. Re-arming from
/// userspace would add the cost of getting the manager onto a
/// thread to every single period
///
/// ## Returns
/// Whether the kernel took it
fn schedule(id: usize, interval: u64) -> bool {
    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return false;
    }

    unsafe {
        KEvent::register(
            manager,
            id + SCHEDULE_IDENT_BASE,
            interval as libc::intptr_t,
            ptr::null_mut(),
            EventDesc::new_interval(),
        )
    }
    .check()
    .is_ok()
}

/// Takes a series back off the clock
///
/// Unlike a one shot, a repeating timer stays armed until it is
/// asked to go, so a series that ended without this would carry
/// on waking the manager for an id that has been handed to
/// somebody else
fn unschedule(id: usize) {
    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return;
    }

    let _ = unsafe {
        KEvent::register(
            manager,
            id + SCHEDULE_IDENT_BASE,
            0,
            ptr::null_mut(),
            EventDesc::new_timer_delete(),
        )
    }
    .check();
}

/// Says this thread is now sitting in a wait on `queue`
///
/// ## Returns
/// Whether to go ahead and wait at all. `false` means the task
/// was cancelled before it got here, so there is no sense
/// starting a wait that would only have to be interrupted
///
/// Blocking calls always get `true`. They have no id, nothing
/// can cancel them, and they don't want any of this
pub(crate) fn waiting_on(queue: i32) -> bool {
    let id = CURRENT.with(|current| current.get());

    if id == NO_TASK {
        return true;
    }

    let Some(data) = slot(id) else {
        return true;
    };

    if data.state() == TaskState::Cancelled {
        return false;
    }

    data.set_waiting(queue);

    // Looked at again, because a cancel that landed between the
    // check above and the record would have found nothing to
    // interrupt and left this waiting for the full duration
    if data.state() == TaskState::Cancelled {
        data.clear_waiting();
        return false;
    }

    true
}

/// Says this thread is out of its wait
///
/// ## Returns
/// Whether the task is still worth carrying on with. A wait
/// that came back because somebody cancelled it has nothing
/// left to do, and should not go on to sit out the rest of the
/// duration it was asked for
///
/// Blocks while a cancel is in flight, so the thread can't
/// finish and have its queue closed underneath a canceller that
/// is part way through a syscall against it
pub(crate) fn stopped_waiting() -> bool {
    let id = CURRENT.with(|current| current.get());

    if id == NO_TASK {
        return true;
    }

    let Some(data) = slot(id) else {
        return true;
    };

    data.clear_waiting();

    data.state() != TaskState::Cancelled
}

/// Whether the task on this thread has been cancelled
///
/// ## Behaviour
/// The question a task that can't be taken out of the kernel
/// asks itself between chunks. A sleep is interrupted mid wait
/// and finds out on the way back; a file read is inside a
/// syscall nothing can reach into, so the only place it can
/// find out is between two of them
///
/// ## Returns
/// Whether there is any point carrying on. `true` means the
/// output is going to be thrown away whatever it turns out to
/// be, so the rest of the work is worth skipping
///
/// Blocking calls always get `false`. They have no id and
/// nothing can cancel them, which is the promise `block` makes
///
/// #### Note
/// Reads the same slot `waiting_on` does, and works from a
/// sleep thread for the same reason: both go through `run`,
/// which is what sets `CURRENT`
pub(crate) fn cancelled() -> bool {
    let id = CURRENT.with(|current| current.get());

    if id == NO_TASK {
        return false;
    }

    let Some(data) = slot(id) else {
        return false;
    };

    data.state() == TaskState::Cancelled
}

/// Takes a cancelled task back out of the kernel
///
/// ## Behaviour
/// The timer comes off the queue first, so it doesn't go off
/// later into a queue nobody is waiting on it in. Then a
/// trigger goes on, which is what actually brings the thread
/// back out of its `kevent` call
///
/// Doing nothing is the right answer whenever the task isn't in
/// a wait. It either hasn't started, in which case the state
/// alone stops it, or it has already come back on its own
fn interrupt(data: &TaskData, id: usize) {
    let Some(queue) = data.claim_waiting() else {
        return;
    };

    let _ =
        unsafe { KEvent::register(queue, id, 0, ptr::null_mut(), EventDesc::new_timer_delete()) }
            .check();

    let _ = unsafe {
        KEvent::register(
            queue,
            WAKE_IDENT,
            0,
            ptr::null_mut(),
            EventDesc::new_user_trigger(),
        )
    }
    .check();

    data.release_waiting();
}

/// Settles a task nothing is left to finish
///
/// ## Behaviour
/// Only ever called for a task whose thread died holding it.
/// Its `Task` was swapped out of the slot before it started
/// and went down with that thread, so there is nothing left to
/// run again and no amount of requeueing would help. A
/// listener blocked on it is let go rather than left waiting
/// on a result that isn't coming
pub(crate) fn fail(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    if !data.state().terminal() {
        data.set_state(TaskState::Failed);
        wake(data);
    }

    // The reference the run that died was holding, which it is
    // no longer around to give back
    release(id);
}

/// Turns a settled state into the error it stands for
#[inline(always)]
fn settled(data: &TaskData, state: TaskState) -> Result<(), RuntimeError> {
    if state == TaskState::Ready {
        return Ok(());
    }

    Err(lost(data, state))
}

/// Why a task that isn't `Ready` has nothing to hand out
#[inline(always)]
fn lost(data: &TaskData, state: TaskState) -> RuntimeError {
    match state {
        // Two very different endings wearing one state. A
        // repeat between runs has another output coming and a
        // reader should try again; a bounded one that reached
        // its ending has none, and telling a caller to try
        // again would be telling it to loop forever
        //
        // A one shot is neither and says `AlreadyTaken`, since
        // "somebody beat you to it" is the useful thing there
        // and there was never a second run for this to rule out
        TaskState::Taken => match !data.kind().repeats() && data.spent() {
            true => RuntimeError::Finished,
            false => RuntimeError::AlreadyTaken,
        },
        TaskState::Cancelled => RuntimeError::Cancelled,
        TaskState::Failed => RuntimeError::TaskFailed,

        // Not an answer, just a race lost. A repeating task is
        // the only thing that gets here: it started its next run
        // while somebody was reading the last one, or finished
        // another one before they looked again. Either way there
        // is an output coming and trying again finds it
        TaskState::Pending | TaskState::Running | TaskState::Ready => RuntimeError::NotReady,

        // The slot is empty, so whatever id reached here belongs
        // to nothing at all
        TaskState::Free => RuntimeError::NoSuchTask,
    }
}

/// Wakes every listener blocked on a slot
#[inline(always)]
fn wake(data: &TaskData) {
    address_lock::wake(data.wait_address());
}

/// A handle for a task that never made it into the table
///
/// The task is dropped here rather than run, and the id is
/// one no slot will ever answer to, so every read on the
/// handle comes back `NoSuchTask` instead of blocking
fn failed<T>(erased: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(erased.cast::<Box<dyn ErasedTask>>()) });

    TaskHandle::new(MAX_TASK_ID)
}

/// A handle for a series that never made it into the table
///
/// The same dead end `failed` is, for the other kind of task a
/// slot can hold. The prototype is dropped here rather than
/// kept, since nothing is ever going to make a copy of it
fn abandoned<T>(prototype: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(prototype.cast::<Box<dyn SeriesTask>>()) });

    TaskHandle::new(MAX_TASK_ID)
}

/// Gives up the `Executor`'s reference on a task
///
/// Taken at creation and held until the task is finished
/// with, so that a handle dropped the instant it is handed
/// out can't free the slot underneath the thread that is
/// about to run it
///
/// ## Behaviour
/// Idempotent, and that is the point of it. Several things can
/// each have a fair claim to be the last to finish with a task
/// — the run that ends it, the tick that finds its series
/// cancelled, the sweep after a dead worker, the teardown that
/// writes off what nothing is left to run — and which of them
/// gets there is not knowable from any one of their positions
///
/// The first one to arrive gives the reference back and the
/// rest do nothing, so none of them has to know about the
/// others. A second release would take the listener count below
/// zero and free a slot with a live task in it
#[inline(always)]
fn release(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    if !data.claim_release() {
        return;
    }

    Executor::drop_listener(id);
}

/// Keeps the manager alive
///
/// The same backoff, window and limit the `Reactor` gets,
/// but without the channel. `join` comes back for a clean
/// exit and a panic alike, so there is nothing the loop has
/// to remember to report on its way out
///
/// #### Note
/// Nothing is requeued on a restart, because nothing was lost.
/// Every task is either in the shared queue, in a worker's
/// ring, or claimed by a thread that is still running it, and
/// none of those live on the manager's stack. A manager comes
/// back to a pool that has been working the whole time it was
/// away
fn supervise(id: i32) {
    thread::spawn(move || {
        let mut failures = 0;
        let mut started = Instant::now();

        loop {
            let _ = thread::spawn(move || executor_loop(id)).join();

            // Asked to stop rather than fell over, so this is
            // not a failure and there is nothing to bring back.
            // The queue is closed here rather than by the
            // caller, because this is the one place that knows
            // the manager thread has actually gone and isn't
            // still sitting in a `kevent` on it
            if shutting_down() {
                let _ = unsafe { libc::close(id) };
                break;
            }

            if started.elapsed() >= RESTART_WINDOW {
                failures = 0;
            }

            failures += 1;

            if failures > RESTART_LIMIT {
                shutdown(id);
                break;
            }

            thread::sleep(RESTART_BACKOFF * failures);

            started = Instant::now();
        }
    });
}

/// The loop the manager runs on
///
/// Woken by its own timer rather than by spawning. A task
/// reaches a worker without passing through here at all, so
/// there is nothing for a spawn to tell the manager that the
/// next tick won't see for itself
fn executor_loop(id: i32) {
    let mut events = eventlist();

    let armed = unsafe {
        KEvent::register(
            id,
            MANAGER_TICK_IDENT,
            MANAGER_TICK.as_nanos() as libc::intptr_t,
            ptr::null_mut(),
            EventDesc::new_interval(),
        )
    }
    .check();

    // A manager with no tick has nothing to wake it, and
    // policy that never runs is worse than a restart
    if armed.is_err() {
        return;
    }

    recover_waits();

    loop {
        // Checked before the policy pass as well as after the
        // wait, so a manager asked to stop while it was already
        // inside `kevent` doesn't go round and grow a pool that
        // is being taken down
        if shutting_down() {
            return;
        }

        POOL.tick();

        let count = match unsafe { KEvent::listen(id, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => break,
        };

        // The poke that woke this is the whole of its message,
        // and the events it came back with belong to a queue
        // nothing is going to read again
        if shutting_down() {
            return;
        }

        // Nothing at all unless a test has asked for it, and
        // deliberately here rather than at the top of the loop.
        // This is the one place a manager can die and take
        // something with it: the kernel has handed these wakes
        // over and no copy of them exists anywhere else
        if injected_fault() {
            panic!("injected manager fault");
        }

        // The tick carries nothing — being woken is the whole
        // of its message. Everything else on this queue is a
        // scheduled task whose interval is up
        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 || event.ident == MANAGER_TICK_IDENT {
                continue;
            }

            fire(event.ident);
        }
    }
}

/// Writes off every schedule in the process
///
/// ## Behaviour
/// A schedule is the one thing that cannot carry on without a
/// manager. A `Repeating` task puts itself back on a pool that
/// is still running and never needed the queue at all, and a
/// `RepeatEvery` finds the queue closed when it goes to wait
/// and ends its own series on the spot — but a `Series` is
/// driven entirely by a timer on the queue that has just gone,
/// and its slot is held for the life of the series rather than
/// the life of a run. Left alone it would hold that slot for as
/// long as the process lives, with nothing anywhere that would
/// ever look at it again
///
/// #### Note
/// Done before the pool is asked whether it can carry on,
/// because it makes no difference to the answer. A pool that
/// recovers still has no manager to tick these, and one that
/// doesn't was going to write them off anyway
///
/// #### Note
/// Safe against a run publishing into a series at this moment.
/// A run holds a claim of its own for as long as it intends to
/// publish, so giving the `Executor`'s back here can't take the
/// count to zero underneath one
fn orphaned() {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        let kind = data.kind();

        // Only what the queue was driving. A `Repeating` task
        // never touched it and carries on regardless
        //
        // An armed slot is on that list whatever its kind says.
        // A delayed one shot is a `Once` waiting on a timer,
        // and a timer on a queue that has closed is a task that
        // will never start
        if !kind.waits() && !kind.schedules() && !data.armed() {
            continue;
        }

        let state = data.state();

        // A thread is inside this one and will find the queue
        // closed the moment it goes to wait, and end its own
        // series there. A schedule has no such thread — the one
        // inside a schedule is a run publishing into it, which
        // knows nothing about any of this
        if state == TaskState::Running && !kind.schedules() {
            continue;
        }

        // Left alone if it has already settled, so a last output
        // stays readable rather than being turned into a failure
        // after the fact. Only the reference has to go
        if !state.terminal() {
            data.set_state(TaskState::Failed);
            wake(data);
        }

        release(task);
    }
}

/// Puts back the wakes a dead manager took down with it
///
/// ## Why this is needed at all
/// A wake is gone from the kernel the moment `kevent` hands it
/// over. A manager that comes apart while holding a batch of
/// them — after the syscall returned and before the task was
/// queued — takes those wakes with it, and a `RepeatEvery`
/// whose wake was in that batch is never put back on the pool.
/// It stops where it stands, holding a slot, with a handle that
/// settles for nobody
///
/// Every wait the table still says is owed is therefore armed
/// again here, before the loop starts reading. Most restarts
/// find nothing, since the common case is a manager that died
/// between batches with every wake still sitting in the kernel
///
/// ## Behaviour
/// Re-arming a wait that was never actually lost is harmless.
/// `EV_ADD` replaces rather than adds, so the ident carries one
/// timer either way, and the wake it eventually delivers is
/// claimed by exactly one caller — so a task is queued once
/// however many times its wait was armed
///
/// A cancelled task gets its wake back too, and should. The run
/// it wakes finds the series cancelled, drops the task and
/// gives the slot up, which is the same route every other
/// cancelled repeat takes and the only one that ends in the
/// slot coming back
///
/// #### Note
/// A schedule needs none of this. Its timer repeats, so a lost
/// tick costs it one run and the next period wakes it again —
/// which is the skipping already written down on
/// `TaskBuilder::at_rate` rather than anything to be recovered
///
/// #### Note
/// A walk of the whole table on every manager start, which on a
/// table that has held millions of tasks is not free. It is
/// bought deliberately: a manager restart is rare and a lost
/// task is forever
fn recover_waits() {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        // The armed flag is the whole question. It is set only
        // by `wait_out` and cleared by whoever takes the wake,
        // so it means "a timer is owed on the manager's queue"
        // whatever kind of task is underneath it — a repeat
        // between runs, or a delayed one shot that hasn't had
        // its first
        if !data.armed() {
            continue;
        }

        // Whichever wait it is actually sitting in. A task that
        // hasn't run yet is owed its start delay, and re-arming
        // it with the gap between runs would start it at the
        // wrong moment entirely
        let owed = match data.start_delay() {
            0 => data.interval(),
            delay => delay,
        };

        arm_timer(task, owed);
    }
}

/// Stops managing the pool for good
///
/// ## Behaviour
/// Deliberately does not fail the backlog. Workers pull their
/// own work, reverse their own queue and clear up after their
/// own dead, so a pool whose manager has given up carries on
/// running everything queued — it simply stops growing,
/// shrinking and rebalancing while it does
///
/// Tasks are only written off when there is genuinely nothing
/// left that could run them, because a listener blocked on a
/// task with no thread anywhere behind it would otherwise
/// block for the life of the process
fn shutdown(id: i32) {
    EXECUTOR_KQUEUE_ID.store(DEAD_KQUEUE_ID, Ordering::SeqCst);
    let _ = unsafe { libc::close(id) };

    // Before the pool is even asked, because this is true
    // whether or not it survives
    orphaned();

    // Every chance to carry on before anything is written off
    POOL.ensure_floor();

    if POOL.live() > 0 {
        return;
    }

    // Nothing is left running, so the pool is shut rather than
    // left able to come back. Every task below is about to be
    // failed and its slot handed back, and a worker starting
    // afterwards would find those ids still sitting in the
    // rings of the workers that died holding them
    POOL.stop_permanently();
    POOL.abandon();

    for task in POOL.injector().drain() {
        fail(task);
    }

    // Everything the runtime is still holding a reference on,
    // which is no longer only the tasks nothing has started
    //
    // A repeating task holds its slot for the life of the
    // series rather than the life of a run, and both of the
    // timed ones wait for a timer on the queue that was just
    // closed. Between runs they sit in `Ready` or `Taken` with
    // nothing anywhere that will ever look at them again, so a
    // sweep that only wrote off `Pending` left every schedule
    // in the process holding a slot for good
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        let state = data.state();

        // A thread is inside this one and will give the
        // reference back itself on the way out. Both re-arm
        // paths fail against a closed queue and a stopped pool,
        // so a repeating task in here ends its series rather
        // than going round again
        //
        // A series is the exception, because the thread inside
        // one is a run publishing into it rather than the
        // series itself, and a run gives back its own claim and
        // never the `Executor`'s. Reaching in is safe precisely
        // because that run is holding a claim of its own, so
        // the count can't reach zero while it is still writing
        if state == TaskState::Running && !data.kind().schedules() {
            continue;
        }

        if !state.terminal() {
            data.set_state(TaskState::Failed);
            wake(data);
        }

        // The reference the `Executor` took at creation, which
        // nothing is going to be around to give back otherwise.
        // A one shot that already finished gave it back on its
        // own, and this is why that is now safe to say twice
        release(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Sleep, futures::task::sealed};
    use std::time::Duration;

    /// A task that goes down and takes its thread with it
    ///
    /// Lives in here rather than in the integration tests
    /// because `Task` is sealed, so nothing outside the crate
    /// can write a task at all, let alone one that panics
    struct Panics;

    impl sealed::Sealed for Panics {}

    impl Task for Panics {
        type Output = usize;

        fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
            panic!("this task is meant to go down");
        }
    }

    /// A task that holds its thread without admitting it
    ///
    /// `blocking()` is a hint and this one gets it wrong on
    /// purpose, so nothing offloads it and it sits on a worker
    /// for its whole duration. That is the case worker growth
    /// exists for, and the only way to reach it deliberately
    struct Liar;

    impl sealed::Sealed for Liar {}

    impl Task for Liar {
        type Output = usize;

        fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
            thread::sleep(Duration::from_millis(200));

            0
        }
    }

    /// The pool grows when it stops getting anywhere
    ///
    /// Being busy is not on its own a reason to add threads.
    /// A pool getting through thousands of tasks a second looks
    /// exactly as busy as one stuck in a handful of long ones,
    /// and adding threads to the first makes it slower. What
    /// separates them is whether anything finished, which is
    /// what this checks by making sure nothing does
    #[test]
    fn pool_grows_when_tasks_hold_their_workers() {
        crate::Runtime::init();

        let cores = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);

        let handles: Vec<_> = (0..cores * 2)
            .map(|_| crate::Runtime::task(Liar).spawn())
            .collect();

        // Long enough for the manager to have seen several
        // ticks pass with nothing finishing, and well short of
        // the tasks themselves ending
        thread::sleep(Duration::from_millis(150));

        let grown = crate::Runtime::workers().workers.len();

        for handle in handles {
            handle.join().expect("every task finishes");
        }

        assert!(
            grown > cores,
            "pool stayed at {} workers with {} tasks holding threads and a floor of {}",
            grown,
            cores * 2,
            cores,
        );
    }

    /// A task going down costs that task and nothing else
    ///
    /// It comes back as an error, because claiming took its
    /// `Task` out of the slot and the unwind dropped it, so
    /// there is nothing left to run again. Everything queued
    /// around it finishes normally, and the worker that was
    /// running it doesn't even stop
    ///
    /// #### Note
    /// The panic is still printed as it unwinds, because that
    /// is Rust's default hook and a task dying is worth
    /// knowing about. A line about a thread going down in the
    /// middle of this test is the test working
    #[test]
    fn panicking_task_does_not_lose_its_queue() {
        crate::Runtime::init();

        let quick = || Sleep::sleep(Duration::from_micros(50), true);

        let before: Vec<_> = (0..256).map(|_| crate::Runtime::task(quick()).spawn()).collect();
        let doomed = crate::Runtime::task(Panics).spawn();
        let after: Vec<_> = (0..256).map(|_| crate::Runtime::task(quick()).spawn()).collect();

        assert_eq!(
            doomed.join(),
            Err(RuntimeError::TaskFailed),
            "the task that went down comes back as an error rather than blocking forever",
        );

        for handle in before.into_iter().chain(after) {
            handle
                .join()
                .expect("every task the dead worker was holding still finishes");
        }
    }
}
