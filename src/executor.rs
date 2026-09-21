//! # Executor
//! Owns every task slot in the process and manages the pool of
//! workers that run them

use crate::modules::input::token;
use crate::{
    Runtime, RuntimeError,
    constants::{
        DEAD_KQUEUE_ID, MANAGER_TICK, MANAGER_TICK_IDENT, MAX_TASK_ID, NO_SELECT, NO_TASK,
        PARK_TIMER, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW, SCHEDULE_IDENT_BASE,
        SELECT_IDENT, SELECT_POLL, SHUTDOWN_POLL, TIMEOUT_IDENT_BASE, UNSTARTED_TASK_ID,
        WAKE_IDENT,
    },
    futures::task::{Task, sealed::Park},
    modules::{
        address_lock,
        erased_task::ErasedTask,
        event_desc::EventDesc,
        extras::Extras,
        faults,
        forward::Forward,
        gate::{AfterRun, Gate, Trigger},
        gated::Gated,
        gather::{Gather, access},
        handle_kind::Waiting,
        handle_set::HandleSet,
        help::{self, Patience},
        input::Receives,
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        kqueue,
        mailbox::Mailbox,
        merge_set::MergeSet,
        series::SeriesTask,
        task_data::TaskData,
        task_data::deadline_epoch,
        task_handle::TaskHandle,
        task_setup::TaskSetup,
        task_state::TaskState,
        task_table::TaskTable,
        tuning::{self, Tuning},
        worker_pool::POOL,
    },
};
use libc::c_void;
use std::{
    cell::Cell,
    mem,
    panic::{self, AssertUnwindSafe},
    ptr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI32, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Every task in the process, addressed by id
static DATA: TaskTable = TaskTable::new();

/// The kqueue the manager takes its tick from
///
/// `Relaxed` reads, with `SeqCst` writes when the runtime starts,
/// shuts down, or the manager gives up
static EXECUTOR_KQUEUE_ID: AtomicI32 = AtomicI32::new(DEAD_KQUEUE_ID);

/// How many tasks have been spawned
///
/// A task's age is this less the stamp it was created with
static SEQUENCE: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// The spawned task this thread is running, or `NO_TASK`
    ///
    /// `Runtime::block` never sets it, which is what keeps blocking
    /// calls out of cancellation
    static CURRENT: Cell<usize> = const { Cell::new(NO_TASK) };
}

/// No manager and a closed pool, before the first `init` and
/// after every `shutdown`
const STOPPED: u8 = 0;

/// Between `STOPPED` and `RUNNING`, while one `init` brings the
/// runtime up
const STARTING: u8 = 1;

/// Started, and not yet asked to stop
const RUNNING: u8 = 2;

/// Between `RUNNING` and `STOPPED`, while one `shutdown` drains
/// the pool
const STOPPING: u8 = 3;

/// Where the runtime is between an `init` and a `shutdown`
///
/// `SeqCst` throughout
static LIFECYCLE: AtomicU8 = AtomicU8::new(STOPPED);

/// The thread supervising the manager
static SUPERVISOR: Mutex<Option<JoinHandle<()>>> = Mutex::new(None);

/// Whether the runtime is stopping or stopped
#[inline(always)]
pub(crate) fn shutting_down() -> bool {
    matches!(LIFECYCLE.load(Ordering::SeqCst), STOPPING | STOPPED)
}

/// Stops the runtime until the next `init`
///
/// ## Behaviour
/// Nothing new gets in, and everything already queued still
/// runs. Blocks until the pool is empty and every thread it
/// started has gone, then writes off whatever is left, such as
/// schedules and repeats between runs
///
/// A second caller waits for the first one to finish
///
/// #### Note
/// Never returns while a task that never finishes is running,
/// including when called from inside a spawned task
pub(crate) fn shutdown_now() {
    loop {
        match LIFECYCLE.compare_exchange(RUNNING, STOPPING, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => break,
            Err(STOPPED) => return,

            // Somebody else's shutdown, which is this one too
            Err(STOPPING) => {
                while LIFECYCLE.load(Ordering::SeqCst) == STOPPING {
                    thread::sleep(SHUTDOWN_POLL);
                }

                return;
            }

            // A start that is nearly done, and then gets stopped
            Err(_) => thread::sleep(SHUTDOWN_POLL),
        }
    }

    // A spawn from here on settles `Failed`
    POOL.close();

    let manager = EXECUTOR_KQUEUE_ID.swap(DEAD_KQUEUE_ID, Ordering::SeqCst);

    // Woken rather than closed, since the manager may be waiting
    // on it. The supervisor closes it once the manager has gone
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

    // Gone before the pool is stopped, so no tick can start a
    // thread behind it
    let supervisor = SUPERVISOR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .take();

    if let Some(supervisor) = supervisor {
        let _ = supervisor.join();
    }

    // Everything already queued still runs
    while POOL.stats().has_any_task() {
        thread::sleep(SHUTDOWN_POLL);
    }

    // Asked again every time round, for a thread that started
    // just as the pool closed
    loop {
        POOL.stop_all();
        POOL.abandon();

        if POOL.live() == 0 && POOL.sleeps_live() == 0 {
            break;
        }

        thread::sleep(SHUTDOWN_POLL);
    }

    // Blocking tasks too
    for task in POOL
        .injector()
        .drain()
        .into_iter()
        .chain(POOL.blocking().drain())
    {
        fail(task);
    }

    // Everything still holding a reference: schedules, repeats
    // between runs, and tasks in a dead worker's ring
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        // Its timer went with the manager's queue, and the next
        // manager would otherwise arm it again
        data.disarm();

        // A parked task holds no thread to give its reference back
        if data.parked() {
            unpark(task, data);
            continue;
        }

        let state = data.state();

        // The running thread gives the reference back itself. A
        // series is the exception, since what is running is one of its
        // runs, which holds a claim of its own
        if state == TaskState::Running && !data.kind().schedules() {
            continue;
        }

        if !state.terminal() {
            data.set_state(TaskState::Failed);
            wake(data);
        }

        // A task between gives takes no more, and its handles say so
        close_extras(data);
        release(task);
    }

    LIFECYCLE.store(STOPPED, Ordering::SeqCst);
}

/// Manager deaths still owed
static INJECTED_FAULTS: AtomicU32 = AtomicU32::new(0);

/// Makes the manager come apart the next `count` times it goes
/// round its loop
///
/// Past `RESTART_LIMIT` in one window, the supervisor gives up
/// until the runtime is shut down and started again
#[cfg(feature = "fault-injection")]
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
    /// Starts the manager and opens the pool with these sizes
    ///
    /// Used by the first `Runtime::init` and by every one after a
    /// shutdown
    ///
    /// ## Returns
    /// `AlreadyInit` when the runtime is already running, with the
    /// sizes left as they are. A shutdown or a start still in
    /// progress is waited out first
    pub(crate) fn init(tuning: Tuning) -> Result<(), RuntimeError> {
        loop {
            match LIFECYCLE.compare_exchange(STOPPED, STARTING, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => break,
                Err(RUNNING) => return Err(RuntimeError::AlreadyInit),
                Err(_) => thread::sleep(SHUTDOWN_POLL),
            }
        }

        let id = match unsafe { libc::kqueue() }.check() {
            Ok(id) => id,
            Err(error) => {
                LIFECYCLE.store(STOPPED, Ordering::SeqCst);
                return Err(error);
            }
        };

        EXECUTOR_KQUEUE_ID.store(id, Ordering::SeqCst);

        tuning::apply(tuning);

        // Started before any deadline could be stored against it
        let _ = deadline_epoch();

        POOL.open();
        POOL.ensure_floor();
        supervise(id);

        LIFECYCLE.store(RUNNING, Ordering::SeqCst);

        Ok(())
    }

    /// Adds a new `Task` to be processed
    pub(crate) fn new_task<F>(task: F, setup: TaskSetup) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        create(task, setup, None).0
    }

    /// Adds a schedule that starts a fresh copy of a task every
    /// interval, whether the last has finished or not
    ///
    /// ## Behaviour
    /// The handle points at a slot that is never run. It holds the
    /// prototype the runs are cloned from, and the latest run's
    /// output. The first run goes now
    pub(crate) fn new_series<F>(task: F, setup: TaskSetup) -> TaskHandle<F::Output>
    where
        F: Task + Clone,
    {
        create_series(task, setup, None)
    }

    /// Adds a task that waits for a give before each run, or each
    /// series of runs
    ///
    /// ## Behaviour
    /// Nothing runs until the handle gives it a value. What a give
    /// starts is decided by the task's kind, and every give waits
    /// out the start delay before it
    pub(crate) fn new_waiting<F, T, M>(
        task: F,
        setup: TaskSetup,
    ) -> TaskHandle<F::Output, Waiting<T>>
    where
        F: Task,
        F::Input: Receives<T, M>,
        T: Send + 'static,
        M: 'static,
    {
        let (gate, mailbox, setup) = waiting_parts::<T>(setup);
        let gated = Gated::<F, T, M>::new(task, Arc::clone(&mailbox));

        create(gated, setup, Some(gate)).0.into_kind(mailbox)
    }

    /// Adds a schedule that waits for a give before each series
    pub(crate) fn new_waiting_series<F, T, M>(
        task: F,
        setup: TaskSetup,
    ) -> TaskHandle<F::Output, Waiting<T>>
    where
        F: Task + Clone,
        F::Input: Receives<T, M>,
        T: Send + 'static,
        M: 'static,
    {
        let (gate, mailbox, setup) = waiting_parts::<T>(setup);
        let gated = Gated::<F, T, M>::new(task, Arc::clone(&mailbox));

        create_series(gated, setup, Some(gate)).into_kind(mailbox)
    }

    /// Gives a waiting task the value its next run is handed
    ///
    /// ## Returns
    /// Why the give was refused, if it was. A refused value is dropped
    pub(crate) fn give<T>(id: usize, mailbox: &Mailbox<T>, value: T) -> Result<(), RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(missing(id));
        };

        let gate = mailbox.gate();

        if gate.finished()
            || matches!(
                data.state(),
                TaskState::Cancelled | TaskState::TimedOut | TaskState::Failed
            )
        {
            return Err(closed(data));
        }

        // Left before the gate is asked, so a run the give starts sees it.
        // What it replaces is dropped here, outside the lock
        drop(mailbox.replace(value));

        match gate.trigger() {
            Trigger::Replaced => Ok(()),
            Trigger::Closed => Err(closed(data)),

            Trigger::Start => match start_waiting(id, data, gate) {
                true => Ok(()),
                false => Err(RuntimeError::TaskFailed),
            },
        }
    }

    /// Registers `forward` on every output of a task
    ///
    /// An output already there is handed over at once. A task that has
    /// gone lets the registration go, and its far end with it
    pub(crate) fn forward(upstream: usize, forward: Box<dyn Forward>) {
        let Some(data) = slot(upstream) else {
            drop(forward);
            return;
        };

        data.extras_or_attach().receivers().register(forward, data);
    }

    /// Links a waiting task to the set it gathers from, and hands back
    /// a plain handle to it, since only the set gives to it
    pub(crate) fn receive_all<O, H>(
        waiting: TaskHandle<O, Waiting<H::Output>>,
        set: H,
    ) -> TaskHandle<O>
    where
        H: HandleSet,
    {
        let gather = Arc::new(Gather::<H>::new(set.slots(), waiting.retyped()));
        let mut held = Vec::new();

        set.link(&gather, access(|root: &mut H::Slots| root), &mut held);
        hold(waiting.id(), held);

        // An empty set is full already, and can never fill again
        gather.settle();

        waiting.into_plain()
    }

    /// Links a waiting task to the set it merges from, and hands back
    /// a plain handle to it
    pub(crate) fn receive_any<O, V, M, H>(
        waiting: TaskHandle<O, Waiting<H::Given>>,
        set: H,
    ) -> TaskHandle<O>
    where
        H: MergeSet<V, M>,
    {
        let target = waiting.retyped();
        let mut held = Vec::new();

        set.link(&target, &mut held);
        hold(waiting.id(), held);

        // Only the tasks in the set give to it from here
        drop(target);

        waiting.into_plain()
    }

    /// Adds 1 to the listener count on a piece of data
    ///
    /// So multiple listeners can be on the same object without the
    /// data being cleaned up early
    pub(crate) fn add_listener(id: usize) {
        let Some(data) = slot(id) else {
            return;
        };

        data.add_listener();
    }

    /// Takes 1 off the listener count on a piece of data
    ///
    /// Whoever takes it to zero frees the slot
    pub(crate) fn drop_listener(id: usize) {
        let Some(data) = slot(id) else {
            return;
        };

        if !data.drop_listener() {
            return;
        }

        // Drops the task, the output and any mapping, and leaves the
        // slot `Free`
        unsafe { data.destroy() };

        // Only once empty, since the id is live again the moment this
        // lands
        DATA.free(id);
    }

    /// Whether a task has settled and will not run again
    ///
    /// An id with no slot behind it answers `true`
    pub(crate) fn finished(id: usize) -> bool {
        let Some(data) = slot(id) else {
            return true;
        };

        let state = data.state();

        match state {
            // Over, whatever the kind
            TaskState::Cancelled | TaskState::TimedOut | TaskState::Failed => true,

            // Over only if it isn't going round again
            _ => state.terminal() && !data.kind().repeats() && !data.open_for_gives(),
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
    pub(crate) fn wait(id: usize) -> Result<TaskState, RuntimeError> {
        Self::wait_until(id, None)
    }

    /// Blocks until a task settles or a deadline passes
    ///
    /// ## Returns
    /// How it settled, or `NotReady` if the deadline came first.
    /// `None` waits forever
    pub(crate) fn wait_until(
        id: usize,
        deadline: Option<Instant>,
    ) -> Result<TaskState, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(missing(id));
        };

        let mut patience = Patience::new();

        loop {
            let state = data.state();

            if state.terminal() {
                return Ok(state);
            }

            // A worker waiting inside a task runs the task it waits on itself
            // if nobody has started it, and other queued work if not
            if help::run_awaited(id) || help::help_once() {
                patience.helped();
                continue;
            }

            let slice = patience.slice();

            let sleep = match deadline {
                None => slice,

                Some(deadline) => {
                    let left = deadline.saturating_duration_since(Instant::now());

                    // Checked after the state, so a task that has just
                    // settled still answers
                    if left.is_zero() {
                        return Err(RuntimeError::NotReady);
                    }

                    Some(slice.map_or(left, |slice| slice.min(left)))
                }
            };

            let Some(sleep) = sleep else {
                address_lock::wait(data.wait_address(), state as u32)?;
                continue;
            };

            if address_lock::wait_until(data.wait_address(), state as u32, sleep)? {
                continue;
            }

            // Only the caller's own deadline ends the wait. A slice that ran
            // out just looks again
            if deadline.is_none_or(|deadline| Instant::now() < deadline) {
                continue;
            }

            // Read once more, so a settle landing with the timeout still
            // counts
            let state = data.state();

            if state.terminal() {
                return Ok(state);
            }

            return Err(RuntimeError::NotReady);
        }
    }

    /// Waits for a task and clones its output, which stays in the
    /// slot for every other listener
    pub(crate) fn clone_result<T>(id: usize) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        Self::clone_result_until(id, None)
    }

    /// The same read, with somewhere to stop waiting
    ///
    /// ## Returns
    /// The output, or `NotReady` if the deadline passed first
    pub(crate) fn clone_result_until<T>(
        id: usize,
        deadline: Option<Instant>,
    ) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        let Some(data) = slot(id) else {
            return Err(missing(id));
        };

        loop {
            settled(data, Self::wait_until(id, deadline)?)?;

            // Held for the length of the clone, so a `take` can't move the
            // output away mid read
            if data.enter_read() {
                break;
            }

            // A repeat can publish again between the wait and here. `lost`
            // says `NotReady` for exactly those races
            let error = lost(data, data.state());

            if error != RuntimeError::NotReady {
                return Err(error);
            }

            // Each retry still honours the deadline
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
    pub(crate) fn poll_result<T>(id: usize) -> Result<T, RuntimeError>
    where
        T: Clone,
    {
        let Some(data) = slot(id) else {
            return Err(missing(id));
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
    /// `claim_result` only wins against `Ready`, so an unsettled
    /// task turns this away by itself
    pub(crate) fn poll_take<T>(id: usize) -> Result<T, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(missing(id));
        };

        if !data.claim_result() {
            return Err(lost(data, data.state()));
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        data.empty();

        Ok(unsafe { ptr::read(data.payload().cast::<T>()) })
    }

    /// Waits for a task and moves its output out, making this the
    /// only caller that owns it
    pub(crate) fn take_result<T>(id: usize) -> Result<T, RuntimeError> {
        Self::take_result_until(id, None)
    }

    /// The same move, with somewhere to stop waiting
    ///
    /// ## Returns
    /// The output, or `NotReady` if the deadline passed first,
    /// leaving the output where it was
    pub(crate) fn take_result_until<T>(
        id: usize,
        deadline: Option<Instant>,
    ) -> Result<T, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(missing(id));
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
    /// A task that hasn't started never will, and one in a kernel
    /// wait is woken out of it. Its output is dropped rather than
    /// published. On a repeat, this ends the whole series
    ///
    /// #### Note
    /// Never touches an output that has already landed, since a
    /// listener may be reading it
    pub(crate) fn cancel(id: usize) {
        if let Some(data) = slot(id) {
            stop(id, data, TaskState::Cancelled);
        }
    }
}

/// Ends a task early as `to`, from wherever it is
///
/// A task already over, or a one shot already taken, is left alone
fn stop(id: usize, data: &TaskData, to: TaskState) {
    loop {
        let state = data.state();

        // Re-read each time round, since a bounded series can finish
        // meanwhile and change its kind
        let repeats = data.kind().repeats();

        match state {
            // Over already, one way or another
            TaskState::Cancelled | TaskState::TimedOut | TaskState::Failed | TaskState::Free => {
                return;
            }

            // A one shot that has been taken is finished. A repeat in the
            // same state is only between runs
            TaskState::Taken if !repeats => return,

            _ => {}
        }

        if data.try_state(state, to) {
            stopped(id, data);
            return;
        }
    }
}

/// Brings a task that has just been stopped out of wherever it waits
fn stopped(id: usize, data: &TaskData) {
    wake(data);
    interrupt(data, id);

    // A parked task has no thread to notice, so it comes down here
    // rather than waiting on its socket
    unpark(id, data);

    // Nor does a task between gives, which is let go here
    if data.takes_input() {
        let_go_waiting(id, data);
    }
}

/// Puts a schedule in the table
///
/// One that waits sits until a give starts its first series. Any
/// other starts now, or once its start delay is up
fn create_series<F>(task: F, setup: TaskSetup, gate: Option<Arc<Gate>>) -> TaskHandle<F::Output>
where
    F: Task + Clone,
{
    let boxed: Box<dyn SeriesTask> = Box::new(task);
    let prototype = Box::into_raw(Box::new(boxed)).cast::<c_void>();

    if !Runtime::initialised() {
        drop(unsafe { Box::from_raw(prototype.cast::<Box<dyn SeriesTask>>()) });

        return TaskHandle::new(UNSTARTED_TASK_ID);
    }

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

    // Still alone with the slot: no timer is armed and no handle
    // exists yet
    let waits = gate.is_some();

    entry.attach_extras(Box::new(Extras::new(gate).timed(setup.timeout, NO_TASK)));
    entry.set_prototype(prototype);

    let handle = TaskHandle::new(id);

    // A schedule that waits starts at its first give
    if waits {
        return handle;
    }

    // A delayed schedule arms a one shot here, and its first tick
    // arms the repeating timer
    let started = match setup.start_delay.as_nanos() as u64 {
        0 => start_series(id, entry),
        delay => wait_for(entry, id, delay),
    };

    if started {
        return handle;
    }

    // Nothing could run it, or the kernel wouldn't take the timer
    unschedule(id);

    if entry.try_state(TaskState::Pending, TaskState::Failed) {
        wake(entry);
    }

    release(id);

    handle
}

/// Puts a task in the table and hands it to the pool
///
/// ## Returns
/// The handle, and whether anything will pick the task up. On
/// `false` the task has already settled `Failed`
fn create<F>(task: F, setup: TaskSetup, gate: Option<Arc<Gate>>) -> (TaskHandle<F::Output>, bool)
where
    F: Task,
{
    // Asked while the concrete type is still here, since a re-arm
    // can't ask
    let setup = setup.blocking(task.blocking(token()));

    let boxed: Box<dyn ErasedTask> = Box::new(task);
    let erased = Box::into_raw(Box::new(boxed)).cast::<c_void>();

    if !Runtime::initialised() {
        return (unstarted(erased), false);
    }

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

    // Still alone with the slot, since no handle has been handed out
    if gate.is_some() || setup.timeout.is_some() {
        let waits = gate.is_some();

        entry.attach_extras(Box::new(
            Extras::new(gate).timed(setup.timeout, setup.parent),
        ));

        // A task that waits sits in its slot until a give starts it
        if waits {
            return (handle, true);
        }
    }

    // Armed instead of queued when there is a start delay
    let started = match setup.start_delay.as_nanos() as u64 {
        0 => queue_spawned(id, setup.blocking),
        delay => wait_for(entry, id, delay),
    };

    if started {
        return (handle, true);
    }

    // Nothing will ever pick this up, so it settles now
    if let Some(published) = slot(id) {
        published.set_state(TaskState::Failed);
        wake(published);
    }

    release(id);

    (handle, false)
}

/// Spawns one run of a series, dropping its handle
///
/// ## Returns
/// Whether a run is on its way
pub(crate) fn spawn_run<F>(task: F, priority: u8, timeout: Option<Duration>, series: usize) -> bool
where
    F: Task,
{
    let setup = TaskSetup {
        timeout,
        parent: series,
        ..TaskSetup::once(priority)
    };

    create(task, setup, None).1
}

/// Leaves an output in a series slot for its listeners
///
/// A value that can't be published is dropped: the series was
/// cancelled, or another run published first
pub(crate) fn publish<T>(id: usize, value: T) {
    let Some(data) = slot(id) else {
        return;
    };

    if !data.begin() {
        close_finished_series(data);

        return;
    }

    debug_assert_eq!(data.size(), mem::size_of::<T>());

    unsafe { data.payload().cast::<T>().write(value) };
    data.fill();

    note_output(data);

    // Cancelled mid write, so the value is left for the last
    // listener out to drop
    if !data.try_state(TaskState::Running, TaskState::Ready) {
        return;
    }

    wake(data);
    forward_output(data);

    close_finished_series(data);
}

/// The slot for an id, if there is a task in it
///
/// A `Free` slot is filtered out, so a stale id can't find the
/// task that took its place
#[inline(always)]
pub(crate) fn slot(id: usize) -> Option<&'static TaskData> {
    let data = DATA.slot(id)?;

    if data.state() == TaskState::Free {
        return None;
    }

    Some(data)
}

/// The queue link in a slot, whether or not a task is in it
///
/// For the injector, which has to step over a retired node to
/// reach the rest of its chain. `None` means no memory is
/// behind the id at all
#[inline(always)]
pub(crate) fn queue_link(id: usize) -> Option<usize> {
    Some(DATA.slot(id)?.queue_next())
}

/// Sets the queue link in a slot, whether or not a task is in it
#[inline(always)]
pub(crate) fn set_queue_link(id: usize, next: usize) {
    if let Some(data) = DATA.slot(id) {
        data.set_queue_next(next);
    }
}

/// Whether the manager still has a queue to work from
///
/// False once its supervisor gives up, or a shutdown starts
#[inline(always)]
pub(crate) fn manager_alive() -> bool {
    EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed) != DEAD_KQUEUE_ID
}

/// The most task slots the table has ever spanned at once
#[inline(always)]
pub(crate) fn peak_slots() -> usize {
    DATA.peak()
}

/// Task slots the table spans right now
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
#[inline(always)]
pub(crate) fn sequence() -> u64 {
    SEQUENCE.load(Ordering::Relaxed)
}

/// Runs one task and publishes what comes back
///
/// ## Behaviour
/// Claiming makes a task run at most once. A task that panics
/// settles `Failed`, and the thread carries on
pub(crate) fn run(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    let raw = data.claim();

    if raw.is_null() {
        return;
    }

    let mut task = unsafe { Box::from_raw(raw.cast::<Box<dyn ErasedTask>>()) };

    let resumed = match data.begin() {
        true => false,

        // Only a task coming back from a park is turned away while
        // still `Running`, with its task in the slot. It carries on
        // where it was
        false if data.state() == TaskState::Running => true,

        // Cancelled or failed, so it is dropped unrun
        false => {
            close_extras(data);
            drop(task);
            release(id);

            return;
        }
    };

    // Cleared on every run, so a restarted manager knows the wait
    // it re-arms is a gap, not a start delay
    data.clear_start_delay();

    if !resumed {
        time_run(id, data);
    }

    let reactor = Runtime::reactor_id();
    let payload = data.payload();

    // Put back afterwards rather than cleared, since a worker helping
    // inside a task runs this inside that task's own run
    let outer = CURRENT.with(|current| current.replace(id));

    let stepped = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        task.run(reactor, id, payload, resumed)
    }));

    CURRENT.with(|current| current.set(outer));

    let finished = match stepped {
        // Parked on something the kernel will report, so the thread
        // goes back to the pool
        Ok(Some(park)) => {
            park_task(id, data, task, park);

            return;
        }

        Ok(None) => true,

        // The thread going down rather than the task. Carried on down
        // through every run the thread is inside
        Err(payload) if faults::is_thread_death(payload.as_ref()) => {
            drop(task);
            panic::resume_unwind(payload);
        }

        Err(_) => false,
    };

    untime_run(id, data);

    if !finished {
        // A panic means the output was never written, so there is
        // nothing to drop. A series ends here too, and so does a task
        // that waits for gives
        close_extras(data);
        drop(task);

        if data.try_state(TaskState::Running, TaskState::Failed) {
            wake(data);
        }

        release(id);

        return;
    }

    data.fill();

    // Counted before it can be read, so a registration that finds it
    // readable also finds it counted
    note_output(data);

    // Cancelled mid run, so the output is left for the last
    // listener out to drop
    if !data.try_state(TaskState::Running, TaskState::Ready) {
        close_extras(data);
        drop(task);
        release(id);

        return;
    }

    wake(data);

    // Whatever this output is forwarded to gets it now
    forward_output(data);

    if !data.kind().repeats() {
        // A task that waits goes back for its next give
        if data.takes_input() {
            rewait(id, data, task);

            return;
        }

        close_extras(data);
        drop(task);
        release(id);

        return;
    }

    // Bounds checked before the task goes back, so an unwanted run
    // is never queued
    let gap = match data.kind().waits() {
        true => Duration::from_nanos(data.interval()),
        false => Duration::ZERO,
    };

    if data.count_run() || data.past_deadline(gap) {
        // A series a give started is over, and the task goes back for
        // the next one
        if data.takes_input() {
            rewait(id, data, task);

            return;
        }

        // The kind moves on before the registrations close, so one made
        // meanwhile sees the series is over
        data.finish_series();
        close_extras(data);
        drop(task);
        release(id);

        return;
    }

    // Round again in the same slot. The state stays `Ready`, so
    // the last output can be read while the next run is queued
    data.rearm(Box::into_raw(task).cast::<c_void>());

    // A timed repeat waits on a timer, not a thread
    let armed = match data.kind().waits() {
        true => wait_out(data, id),
        false => queue(id, data.blocking()),
    };

    if armed {
        return;
    }

    // Nothing is left to run it again
    if data.try_state(TaskState::Ready, TaskState::Failed) {
        wake(data);
    }

    close_extras(data);
    release(id);
}

/// Puts a timer on the manager's queue for a run with a timeout
fn time_run(id: usize, data: &TaskData) {
    let Some(timeout) = data.extras().and_then(Extras::timeout) else {
        return;
    };

    let word = data.time_run(timeout);

    // With no manager to fire it, the run goes unlimited
    let _ = arm_timeout(id, timeout, word);
}

/// Takes a finished run's timer back off the manager's queue
#[inline(always)]
fn untime_run(id: usize, data: &TaskData) {
    if !data.untime_run() {
        return;
    }

    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return;
    }

    let _ = unsafe {
        KEvent::register(
            manager,
            id + TIMEOUT_IDENT_BASE,
            0,
            ptr::null_mut(),
            EventDesc::new_timer_delete(),
        )
    }
    .check();
}

/// Arms the timer that ends a run at its deadline
///
/// `word` is the deadline's word, carried so a timer left from an
/// earlier run does nothing
fn arm_timeout(id: usize, left: Duration, word: u64) -> bool {
    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return false;
    }

    // A timer of zero is not one the kernel fires
    let nanos = left.as_nanos().clamp(1, libc::intptr_t::MAX as u128);

    unsafe {
        KEvent::register(
            manager,
            id + TIMEOUT_IDENT_BASE,
            nanos as libc::intptr_t,
            word as *mut c_void,
            EventDesc::new_timer(),
        )
    }
    .check()
    .is_ok()
}

/// Ends a run that went past its timeout, and the series it
/// belongs to if it is one run of a schedule
fn timed_out(id: usize, word: u64) {
    let Some(data) = slot(id) else {
        return;
    };

    // Left from a run that has already ended
    if word == 0 || data.run_deadline() != word {
        return;
    }

    // A parked run is taken down before anyone hears it timed out, so
    // whatever it gives back on the way is there for the next reader
    if let Some(parked) = data.claim_parked() {
        unwatch_park(id, parked, Fired::Neither);

        let raw = data.claim();

        if !raw.is_null() {
            drop(unsafe { Box::from_raw(raw.cast::<Box<dyn ErasedTask>>()) });
        }

        if data.try_state(TaskState::Running, TaskState::TimedOut) {
            close_extras(data);
            wake(data);
        }

        release(id);
    } else if data.try_state(TaskState::Running, TaskState::TimedOut) {
        stopped(id, data);
    } else {
        return;
    }

    let parent = data.extras().map_or(NO_TASK, Extras::parent);

    if let Some(series) = slot(parent) {
        stop(parent, series, TaskState::TimedOut);
    }
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

/// Hands a task just spawned to the pool, beside the task that spawned
/// it when that task is running on a worker
///
/// ## Returns
/// Whether anything will come for it
#[inline(always)]
fn queue_spawned(id: usize, blocking: bool) -> bool {
    if !blocking && CURRENT.with(|current| current.get()) != NO_TASK {
        if let Some(worker) = help::worker() {
            return POOL.submit_local(worker, id);
        }
    }

    queue(id, blocking)
}

/// Which half of a park went off, and so took itself down
///
/// Both halves are one shot, so only the other one is left for
/// whoever claims the park to take off the queue
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fired {
    /// What it was watching happened: a socket became ready, or a
    /// signal arrived
    Event,

    /// The deadline came
    Deadline,

    /// Neither, because a cancel, a restart or a shutdown claimed
    /// the park
    Neither,
}

/// Puts a task down until what it is watching happens, giving its
/// thread back to the pool
///
/// ## Behaviour
/// The watch and the deadline go on the manager's queue, and
/// whichever fires first queues the task again
///
/// A listener is held across the registration, so a cancel that
/// takes the park down meanwhile can't free the slot under it
fn park_task(id: usize, data: &TaskData, task: Box<Box<dyn ErasedTask>>, park: Park) {
    data.add_listener();

    data.rearm(Box::into_raw(task).cast::<c_void>());
    data.park(park.ident, park.filter, park.deadline.is_some());

    let watched = watch_park(id, park);

    // Nothing will ever wake it, or a cancel landed before the park
    // was there for it to find. Whoever else may have claimed the
    // park meanwhile deals with it instead
    if !watched || data.state() != TaskState::Running {
        unpark(id, data);
    }

    Executor::drop_listener(id);
}

/// Puts a parked task's deadline and watch on the manager's
/// queue
///
/// ## Returns
/// Whether both went on. `false` means the manager has gone or
/// the kernel refused one
fn watch_park(id: usize, park: Park) -> bool {
    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return false;
    }

    // First, so a watch that fires at once finds the timer already
    // there to take down
    if let Some(deadline) = park.deadline {
        // A timer of zero is not one the kernel fires
        let left = deadline
            .saturating_duration_since(Instant::now())
            .as_nanos()
            .clamp(1, libc::intptr_t::MAX as u128);

        let timed = unsafe {
            KEvent::register(
                manager,
                id + SCHEDULE_IDENT_BASE,
                left as libc::intptr_t,
                PARK_TIMER as *mut c_void,
                EventDesc::new_timer(),
            )
        }
        .check();

        if timed.is_err() {
            return false;
        }
    }

    unsafe {
        KEvent::register(
            manager,
            park.ident as usize,
            0,
            id as *mut c_void,
            EventDesc::new_park(park.filter, park.notes),
        )
    }
    .check()
    .is_ok()
}

/// Takes whatever is left of a park off the manager's queue
///
/// `parked` is what `claim_parked` gave back. Nothing is taken off
/// once the manager has gone, since its queue went with it
fn unwatch_park(id: usize, parked: (i32, i16, bool), fired: Fired) {
    let manager = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);

    if manager == DEAD_KQUEUE_ID {
        return;
    }

    let (ident, filter, timed) = parked;

    if timed && fired != Fired::Deadline {
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

    if fired != Fired::Event {
        let _ = unsafe {
            KEvent::register(
                manager,
                ident as usize,
                0,
                id as *mut c_void,
                EventDesc::new_park_delete(filter),
            )
        }
        .check();
    }
}

/// Queues a parked task again, because what it was watching
/// happened or its deadline has come
///
/// A wake for a task that is no longer parked is left over from
/// an earlier park, and does nothing. One for a task parked since
/// only costs it a second look at what it is watching
fn wake_parked(id: usize, fired: Fired) {
    let Some(data) = slot(id) else {
        return;
    };

    let Some(parked) = data.claim_parked() else {
        return;
    };

    unwatch_park(id, parked, fired);

    if queue(id, data.blocking()) {
        return;
    }

    // Nothing is left to run it
    take_down(id, data);
}

/// Takes a parked task down without running it again, if nobody
/// else has claimed it first
fn unpark(id: usize, data: &TaskData) {
    let Some(parked) = data.claim_parked() else {
        return;
    };

    unwatch_park(id, parked, Fired::Neither);
    take_down(id, data);
}

/// Drops a task whose park has been claimed, and gives its
/// reference back
///
/// A task still `Running` settles `Failed`. A cancelled one keeps
/// its state
fn take_down(id: usize, data: &TaskData) {
    let raw = data.claim();

    if !raw.is_null() {
        drop(unsafe { Box::from_raw(raw.cast::<Box<dyn ErasedTask>>()) });
    }

    if data.try_state(TaskState::Running, TaskState::Failed) {
        wake(data);
    }

    release(id);
}

/// Puts a task down until its interval is up
///
/// ## Returns
/// Whether the kernel took the timer. A refusal ends the series
fn wait_out(data: &TaskData, id: usize) -> bool {
    wait_for(data, id, data.interval())
}

/// Puts a task down for `nanos` nanoseconds
///
/// The slot is marked armed before the timer exists, so a
/// restarted manager can always find the wait
///
/// ## Returns
/// Whether the kernel took the timer
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
/// The ident is shifted clear of the manager's own. `EV_ADD`
/// replaces a timer already on the ident, so re-arming is safe
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

/// Acts on a timer that has gone off
///
/// A waiting task goes back on the queue, and `run` turns away
/// any cancelled meanwhile. A schedule starts its next run
fn fire(ident: usize) {
    let Some(id) = ident.checked_sub(SCHEDULE_IDENT_BASE) else {
        return;
    };

    let Some(data) = slot(id) else {
        return;
    };

    if data.kind().schedules() {
        // A delayed schedule's first wake is claimed like any other,
        // so a restarted manager can't start it twice
        if data.armed() && !data.claim_armed() {
            return;
        }

        tick(id, data);

        return;
    }

    // Only whoever takes the wake queues the task, so a timer put
    // back by a restarted manager can't queue it twice
    if !data.claim_armed() {
        return;
    }

    if queue(id, data.blocking()) {
        return;
    }

    // Nothing is left to run it. A repeat between runs is `Ready`
    // or `Taken`; a delayed one shot is still `Pending`
    if data.try_state(TaskState::Ready, TaskState::Failed)
        || data.try_state(TaskState::Taken, TaskState::Failed)
        || data.try_state(TaskState::Pending, TaskState::Failed)
    {
        wake(data);
    }

    close_extras(data);
    release(id);
}

/// Starts a schedule whose first run goes now
///
/// The first run counts against `count` like any other
///
/// ## Returns
/// Whether the series is under way
fn start_series(id: usize, data: &TaskData) -> bool {
    if !data.runs_remain() {
        return false;
    }

    if !launch(id, data) {
        return false;
    }

    // A schedule counts the runs it starts, since its runs overlap
    if data.count_run() {
        // The only run allowed is away, so the reference goes back
        // now. One that waits goes back for its next give instead
        match data.takes_input() {
            true => rewait_series(id, data),

            false => {
                data.finish_series();
                close_finished_series(data);
                release(id);
            }
        }

        return true;
    }

    schedule(id, data.interval())
}

/// Starts the next run of a series, or clears the series up
///
/// A cancelled schedule comes off the clock here, at its next
/// tick. Its slot is held until then
fn tick(id: usize, data: &TaskData) {
    // A delayed schedule's first wake was its delay, so the
    // repeating timer is armed now, once
    if data.start_delay() != 0 {
        data.clear_start_delay();

        if !schedule(id, data.interval()) {
            end_schedule(id, data);

            return;
        }
    }

    // Both bounds, before the launch
    if data.past_deadline(Duration::from_nanos(data.interval())) || !data.runs_remain() {
        end_schedule(id, data);

        return;
    }

    if !over(data.state()) && launch(id, data) {
        // A schedule counts the runs it starts, since its runs overlap
        if data.count_run() {
            end_schedule(id, data);
        }

        return;
    }

    unschedule(id);

    // A series that already settled keeps its state. Only one that
    // never got anywhere is written off
    if !data.state().terminal() {
        data.set_state(TaskState::Failed);
        wake(data);
    }

    close_extras(data);
    release(id);
}

/// Takes a schedule off the clock because it is finished
///
/// Its state is left alone, so its last output stays readable
fn end_schedule(id: usize, data: &TaskData) {
    unschedule(id);

    // One that waits for gives goes back for the next one
    if data.takes_input() {
        rewait_series(id, data);

        return;
    }

    data.finish_series();
    close_finished_series(data);
    release(id);
}

/// Whether a series has come to an end, which `Ready` and
/// `Taken` are not
#[inline(always)]
fn over(state: TaskState) -> bool {
    matches!(
        state,
        TaskState::Cancelled | TaskState::TimedOut | TaskState::Failed
    )
}

/// Spawns one run of the series in a slot
///
/// ## Returns
/// Whether a run is on its way. `false` for a slot with no
/// prototype
fn launch(id: usize, data: &TaskData) -> bool {
    let prototype = data.prototype();

    if prototype.is_null() {
        return false;
    }

    // Only the manager, or the spawning thread before the manager
    // can see it, ever touches the prototype
    let task = unsafe { &**prototype.cast::<Box<dyn SeriesTask>>() };

    let timeout = data.extras().and_then(Extras::timeout);

    task.launch(id, data.priority_class(), timeout)
}

/// Puts a series on the clock with a repeating timer, which
/// keeps the cadence itself
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
/// A repeating timer stays armed until removed, so a series
/// must come off before its id can be reused
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

/// Says this thread is now waiting on `queue`
///
/// ## Returns
/// Whether to wait at all. `false` means the task was already
/// cancelled. Blocking calls always get `true`
pub(crate) fn waiting_on(queue: i32) -> bool {
    let id = CURRENT.with(|current| current.get());

    if id == NO_TASK {
        return true;
    }

    let Some(data) = slot(id) else {
        return true;
    };

    if data.state().stopped() {
        return false;
    }

    data.set_waiting(queue);

    // Checked again, since a cancel between the check above and
    // the record would have found nothing to interrupt
    if data.state().stopped() {
        data.clear_waiting();
        return false;
    }

    true
}

/// Says this thread is out of its wait
///
/// ## Returns
/// Whether the task is still worth carrying on with
///
/// Blocks while a cancel is in flight, so the queue can't be
/// closed under the canceller
pub(crate) fn stopped_waiting() -> bool {
    let id = CURRENT.with(|current| current.get());

    if id == NO_TASK {
        return true;
    }

    let Some(data) = slot(id) else {
        return true;
    };

    data.clear_waiting();

    !data.state().stopped()
}

/// Whether the task on this thread has been cancelled
///
/// For work that can't be interrupted mid syscall, to check
/// between steps. Blocking calls always get `false`
pub(crate) fn cancelled() -> bool {
    let id = CURRENT.with(|current| current.get());

    if id == NO_TASK {
        return false;
    }

    let Some(data) = slot(id) else {
        return false;
    };

    data.state().stopped()
}

/// Takes a cancelled task back out of the kernel
///
/// Deletes its timer, then fires a trigger to wake the thread.
/// Does nothing if the task isn't waiting
///
/// #### Note
/// Only knows how to delete a timer at the task's id. A wait on
/// any other filter has to unregister itself
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

/// Settles a task whose thread died holding it
///
/// Its `Task` went down with the thread, so it can't be run
/// again, and its listeners are let go
pub(crate) fn fail(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    if !data.state().terminal() {
        data.set_state(TaskState::Failed);
        wake(data);
    }

    close_extras(data);

    // The reference the dead run was holding
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
        // A bounded repeat that ran out has nothing more coming. A
        // repeat between runs, or a one shot, says somebody got there
        // first
        TaskState::Taken => {
            match !data.kind().repeats() && data.spent() && !data.open_for_gives() {
                true => RuntimeError::Finished,
                false => RuntimeError::AlreadyTaken,
            }
        }
        TaskState::Cancelled => RuntimeError::Cancelled,
        TaskState::TimedOut => RuntimeError::TimedOut,
        TaskState::Failed => RuntimeError::TaskFailed,

        // A race lost to a repeat's next run. Trying again finds the
        // output
        TaskState::Pending | TaskState::Running | TaskState::Ready => RuntimeError::NotReady,

        // An empty slot
        TaskState::Free => RuntimeError::NoSuchTask,
    }
}

/// Wakes every listener blocked on a slot
#[inline(always)]
fn wake(data: &TaskData) {
    address_lock::wake(data.wait_address());

    // A `join_first` watching this task is poked on its own queue
    // instead. Best effort, since it re-reads the states anyway
    let queue = data.select_queue();

    if queue == NO_SELECT {
        return;
    }

    let _ = unsafe {
        KEvent::register(
            queue,
            SELECT_IDENT,
            0,
            ptr::null_mut(),
            EventDesc::new_user_trigger(),
        )
    }
    .check();
}

/// Waits until one of `ids` has settled, and says which
///
/// A missed wake only costs `SELECT_POLL`
///
/// ## Returns
/// The first id found settled, or `None` for an empty set. An
/// id whose slot has gone counts as settled
pub(crate) fn join_first(ids: &[usize]) -> Option<usize> {
    if ids.is_empty() {
        return None;
    }

    // Often one is already done, and none of the below is needed
    if let Some(done) = settled_any(ids) {
        return Some(done);
    }

    let Ok(queue) = kqueue::id() else {
        // No queue to be poked on, so this just polls
        loop {
            if let Some(done) = settled_any(ids) {
                return Some(done);
            }

            if help::help_once() {
                continue;
            }

            thread::sleep(SELECT_POLL);
        }
    };

    let registered: Vec<usize> = ids
        .iter()
        .filter(|id| slot(**id).is_some_and(|data| data.set_select(queue)))
        .copied()
        .collect();

    // Looked at again after registering, to catch a task that
    // settled before its poke had anywhere to go
    let mut patience = Patience::new();

    let winner = loop {
        if let Some(done) = settled_any(ids) {
            break Some(done);
        }

        // A worker racing tasks inside a task helps with queued work, the
        // same as any other wait
        if help::help_once() {
            patience.helped();
            continue;
        }

        let slice = patience
            .slice()
            .map_or(SELECT_POLL, |slice| slice.min(SELECT_POLL));

        kqueue::wait_any(queue, slice);
    };

    for id in registered {
        if let Some(data) = slot(id) {
            data.clear_select(queue);
        }
    }

    winner
}

/// The first of `ids` with nothing left to wait for
fn settled_any(ids: &[usize]) -> Option<usize> {
    ids.iter().copied().find(|id| match slot(*id) {
        Some(data) => data.state().terminal(),
        None => true,
    })
}

/// A handle for a task that never made it into the table,
/// which reads `NoSuchTask`
fn failed<T>(erased: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(erased.cast::<Box<dyn ErasedTask>>()) });

    TaskHandle::new(MAX_TASK_ID)
}

/// Why a handle with no slot behind it has nothing to hand out
#[inline(always)]
fn missing(id: usize) -> RuntimeError {
    match id == UNSTARTED_TASK_ID {
        true => RuntimeError::NotInitialised,
        false => RuntimeError::NoSuchTask,
    }
}

/// A handle for a task there was no runtime to take, which reads
/// `NotInitialised`
fn unstarted<T>(erased: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(erased.cast::<Box<dyn ErasedTask>>()) });

    TaskHandle::new(UNSTARTED_TASK_ID)
}

/// A handle for a series that never made it into the table,
/// which reads `NoSuchTask`
fn abandoned<T>(prototype: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(prototype.cast::<Box<dyn SeriesTask>>()) });

    TaskHandle::new(MAX_TASK_ID)
}

/// Gives up the `Executor`'s reference on a task
///
/// Idempotent. Several paths can each be the last to finish
/// with a task, and only the first to arrive releases
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

/// The gate and mailbox a waiting task is given, and the setup its
/// slot is made with
///
/// The start delay moves into the gate, since it is waited out after
/// each give rather than after the spawn
fn waiting_parts<T>(setup: TaskSetup) -> (Arc<Gate>, Arc<Mailbox<T>>, TaskSetup) {
    let gate = Arc::new(Gate::new(
        setup.gives,
        setup.runs,
        setup.deadline,
        setup.start_delay,
        setup.kind.repeats(),
    ));

    let mailbox = Arc::new(Mailbox::new(Arc::clone(&gate)));

    let setup = TaskSetup {
        start_delay: Duration::ZERO,
        waits: true,
        ..setup
    };

    (gate, mailbox, setup)
}

/// Why a waiting task takes no more gives
fn closed(data: &TaskData) -> RuntimeError {
    match data.state() {
        TaskState::Cancelled => RuntimeError::Cancelled,
        TaskState::TimedOut => RuntimeError::TimedOut,
        TaskState::Failed | TaskState::Free => RuntimeError::TaskFailed,
        _ => RuntimeError::Finished,
    }
}

/// Starts what a give owes a waiting task: a run, or a series
///
/// The series starts from the top, with its runs and deadline back
/// to full, and waits out the delay first if there is one
///
/// ## Returns
/// Whether anything will run it. On `false` the task has been
/// written off and let go
fn start_waiting(id: usize, data: &TaskData, gate: &Gate) -> bool {
    data.reset_series(gate.runs(), gate.until());

    let delay = gate.delay();

    if delay != 0 {
        data.set_start_delay(delay);
    }

    let started = match (data.kind().schedules(), delay) {
        (true, 0) => start_series(id, data),
        (false, 0) => queue(id, data.blocking()),
        (_, delay) => wait_for(data, id, delay),
    };

    if started {
        return true;
    }

    // Nothing will ever run it, so it takes no more gives
    gate.finish();

    if data.try_state(TaskState::Pending, TaskState::Failed)
        || data.try_state(TaskState::Ready, TaskState::Failed)
        || data.try_state(TaskState::Taken, TaskState::Failed)
    {
        wake(data);
    }

    finish_waiting(id, data);

    false
}

/// Puts a waiting task back once its run, or its series, is over
///
/// The task is back in its slot before the gate is asked, so a give
/// that starts the next run always finds it there
fn rewait(id: usize, data: &TaskData, task: Box<Box<dyn ErasedTask>>) {
    data.rearm(Box::into_raw(task).cast::<c_void>());

    rewait_series(id, data);
}

/// Puts a waiting schedule back once its series is over
///
/// Its prototype stays in the slot for the next series. The slot is
/// held throughout, since the last giver can let the task go as soon
/// as the gate reads waiting
fn rewait_series(id: usize, data: &TaskData) {
    data.add_listener();

    match data.gate().map(Gate::after_run) {
        Some(AfterRun::Wait) => {}

        Some(AfterRun::Again) => {
            if let Some(gate) = data.gate() {
                let _ = start_waiting(id, data, gate);
            }
        }

        Some(AfterRun::Finish) | None => finish_waiting(id, data),
    }

    Executor::drop_listener(id);
}

/// Lets go of a waiting task that takes no more gives
///
/// Its state is left alone, so its last output stays readable
fn finish_waiting(id: usize, data: &TaskData) {
    close_extras(data);

    let task = data.claim();

    if !task.is_null() {
        drop(unsafe { Box::from_raw(task.cast::<Box<dyn ErasedTask>>()) });
    }

    // What it held onto, like the tasks it received from, goes with it
    if let Some(extras) = data.extras() {
        extras.release_held();
    }

    data.finish_series();
    release(id);
}

/// Lets go of a waiting task nothing is owed on, for a cancel or the
/// last handle that could give going
///
/// A task with a run or series under way is let go when it ends
fn let_go_waiting(id: usize, data: &TaskData) {
    let Some(gate) = data.gate() else {
        return;
    };

    if !gate.close_waiting() {
        return;
    }

    // Never given anything, so nothing will ever be read from it
    if data.try_state(TaskState::Pending, TaskState::Failed) {
        wake(data);
    }

    finish_waiting(id, data);
}

/// Lets a waiting task go once no handle is left that could give to
/// it
pub(crate) fn abandon(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    let_go_waiting(id, data);
}

/// Ends what a task's extras keep going, for every way a task ends
///
/// A waiting task takes no more gives, and a task whose outputs are
/// forwarded hands the last one to anything that hasn't had it, then
/// lets every registration go
#[inline(always)]
fn close_extras(data: &TaskData) {
    let Some(extras) = data.extras() else {
        return;
    };

    if let Some(gate) = extras.gate() {
        gate.finish();
    }

    extras.receivers().close(data);
}

/// Lets go of what a finished schedule's outputs are forwarded to,
/// once none of its runs can publish again
///
/// A schedule still waiting on its first output is let go when that
/// run publishes
fn close_finished_series(data: &TaskData) {
    if data.kind().repeats() || data.takes_input() || data.state() == TaskState::Pending {
        return;
    }

    close_extras(data);
}

/// Counts an output about to become readable, for a task whose
/// outputs are forwarded
#[inline(always)]
fn note_output(data: &TaskData) {
    if let Some(extras) = data.extras() {
        extras.receivers().note_output();
    }
}

/// Hands a readable output to everything it is forwarded to
#[inline(always)]
fn forward_output(data: &TaskData) {
    if let Some(extras) = data.extras() {
        extras.receivers().walk(data);
    }
}

/// Keeps `claims` until a task is finished, or drops them if the task
/// has already gone
fn hold(id: usize, claims: Vec<Box<dyn Send>>) {
    match slot(id) {
        Some(data) => data.extras_or_attach().hold(claims),
        None => drop(claims),
    }
}

/// Writes a task off because something it was owed could never
/// reach it, like a copy into it that panicked
///
/// A task between gives is let go at once. One mid run is let go
/// when the run ends
pub(crate) fn write_off(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    loop {
        let state = data.state();

        if matches!(
            state,
            TaskState::Cancelled | TaskState::TimedOut | TaskState::Failed | TaskState::Free
        ) {
            break;
        }

        if data.try_state(state, TaskState::Failed) {
            wake(data);
            break;
        }
    }

    let Some(gate) = data.gate() else {
        return;
    };

    if gate.close_waiting() {
        finish_waiting(id, data);
        return;
    }

    gate.finish();
}

/// Keeps the manager alive, restarting it with the same
/// backoff, window and limit as the `Reactor`
fn supervise(id: i32) {
    let supervisor = thread::spawn(move || {
        let mut failures = 0;
        let mut started = Instant::now();

        loop {
            let _ = thread::spawn(move || executor_loop(id)).join();

            // Stopped rather than fell over. The queue is closed here, once
            // the manager has definitely gone
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

    *SUPERVISOR
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(supervisor);
}

/// The loop the manager runs on, woken by its own tick
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

    // With no tick, nothing would ever wake it
    if armed.is_err() {
        return;
    }

    recover_waits();

    loop {
        // Checked before the policy pass too, so a stopping manager
        // doesn't grow the pool
        if shutting_down() {
            return;
        }

        POOL.tick();

        let count = match unsafe { KEvent::listen(id, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => break,
        };

        // The events belong to a queue nothing will read again
        if shutting_down() {
            return;
        }

        // Only when a test asks
        if injected_fault() {
            panic!("injected manager fault");
        }

        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
                continue;
            }

            match event.filter {
                // What a parked task was watching happened: a socket is
                // ready, a signal arrived, or a watched path moved
                libc::EVFILT_READ
                | libc::EVFILT_WRITE
                | libc::EVFILT_SIGNAL
                | libc::EVFILT_VNODE => {
                    wake_parked(event.udata as usize, Fired::Event);
                }

                // The tick carries nothing
                libc::EVFILT_TIMER if event.ident == MANAGER_TICK_IDENT => {}

                // A run that went past its timeout
                libc::EVFILT_TIMER if event.ident >= TIMEOUT_IDENT_BASE => {
                    timed_out(event.ident - TIMEOUT_IDENT_BASE, event.udata as u64);
                }

                // A parked task's deadline
                libc::EVFILT_TIMER if event.udata as usize == PARK_TIMER => {
                    if let Some(id) = event.ident.checked_sub(SCHEDULE_IDENT_BASE) {
                        wake_parked(id, Fired::Deadline);
                    }
                }

                // A task whose interval is up
                libc::EVFILT_TIMER => fire(event.ident),

                // The wake a shutdown sends, which the check above the
                // loop deals with
                _ => {}
            }
        }
    }
}

/// Writes off everything the manager's queue was driving, once
/// that queue has closed
///
/// A run publishing into a series holds its own claim, so
/// releasing here is safe
fn orphaned() {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        // Its watch went with the queue, and nothing else will wake it
        if data.parked() {
            unpark(task, data);
            continue;
        }

        let kind = data.kind();

        // Only what the queue was driving, including a delayed one
        // shot waiting on its timer
        if !kind.waits() && !kind.schedules() && !data.armed() {
            continue;
        }

        let state = data.state();

        // A running repeat ends itself when it finds the queue closed.
        // A running schedule is only one of its runs, so it is handled
        // here
        if state == TaskState::Running && !kind.schedules() {
            continue;
        }

        // A settled task keeps its state and its last output. Only the
        // reference goes
        if !state.terminal() {
            data.set_state(TaskState::Failed);
            wake(data);
        }

        close_extras(data);
        release(task);
    }
}

/// Puts back the wakes a dead manager took down with it
///
/// Every wait still marked armed is armed again. Re-arming one
/// that wasn't lost is harmless
fn recover_waits() {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        // A run's timeout went with the old queue too
        if let Some(deadline) = data.run_deadline_at() {
            let left = deadline.saturating_duration_since(Instant::now());
            let _ = arm_timeout(task, left, data.run_deadline());
        }

        // A park's wake can be lost the same way. Queued again, the task
        // looks at its socket and parks again if it still has to
        if let Some(parked) = data.claim_parked() {
            unwatch_park(task, parked, Fired::Neither);

            if !queue(task, data.blocking()) {
                take_down(task, data);
            }

            continue;
        }

        // Armed means a timer is owed on the manager's queue, whatever
        // the task's kind
        if !data.armed() {
            continue;
        }

        // A task that hasn't run yet is owed its start delay, not its
        // gap
        let owed = match data.start_delay() {
            0 => data.interval(),
            delay => delay,
        };

        arm_timer(task, owed);
    }
}

/// Stops managing the pool until the runtime is shut down and
/// started again
///
/// Doesn't fail the backlog, since the pool carries on without
/// a manager
fn shutdown(id: i32) {
    EXECUTOR_KQUEUE_ID.store(DEAD_KQUEUE_ID, Ordering::SeqCst);
    let _ = unsafe { libc::close(id) };

    // True whether or not the pool survives
    orphaned();

    // Every chance to carry on before anything is written off. Dead
    // threads are swept first, since they still count as live
    POOL.sweep_all();
    POOL.ensure_floor();

    if POOL.live() > 0 {
        return;
    }

    write_off_pool();
}

/// Writes off everything the pool was going to run, once nothing is
/// left that could
///
/// Used when the manager gives up with no thread left, and when the
/// pool can't bring a thread back on its own
pub(crate) fn write_off_pool() {
    // Nothing is left running. The pool is shut until a shutdown
    // and an `init` start it again
    POOL.close();
    POOL.abandon();

    for task in POOL
        .injector()
        .drain()
        .into_iter()
        .chain(POOL.blocking().drain())
    {
        fail(task);
    }

    // Everything still holding a reference, including repeats and
    // schedules between runs
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        let state = data.state();

        // The running thread gives the reference back itself, and a
        // repeat can't go round again against a stopped pool. A series
        // is the exception, since what is running is one of its runs,
        // which holds a claim of its own
        if state == TaskState::Running && !data.kind().schedules() {
            continue;
        }

        if !state.terminal() {
            data.set_state(TaskState::Failed);
            wake(data);
        }

        close_extras(data);

        // Safe to repeat, since `release` is idempotent
        release(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::input::Token;
    use crate::{Nothing, futures::task::sealed, sleep::Sleep};
    use std::time::Duration;

    /// A task that panics
    struct Panics;

    impl sealed::Sealed for Panics {}

    impl Task for Panics {
        type Output = usize;
        type Input = Nothing;

        fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
            panic!("this task is meant to go down");
        }
    }

    /// A task that blocks its thread without saying so, so it
    /// stays on a worker
    struct Liar;

    impl sealed::Sealed for Liar {}

    impl Task for Liar {
        type Output = usize;
        type Input = Nothing;

        fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
            thread::sleep(Duration::from_millis(200));

            0
        }
    }

    /// The pool grows when tasks hold their workers and nothing
    /// finishes
    #[test]
    fn pool_grows_when_tasks_hold_their_workers() {
        let _ = crate::Runtime::init();

        let cores = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);

        let handles: Vec<_> = (0..cores * 2)
            .map(|_| crate::Runtime::task(Liar).spawn())
            .collect();

        // Long enough for several manager ticks, well short of the
        // tasks ending
        thread::sleep(Duration::from_millis(150));

        let grown = crate::Runtime::pool().workers().len();

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

    /// A wake left over from a parked task that has gone can't
    /// start a delayed task that has since taken its id
    #[test]
    fn a_stale_park_wake_cannot_start_a_delayed_task() {
        let _ = crate::Runtime::init();

        let handle = crate::Runtime::task(Sleep::sleep(Duration::from_micros(1)))
            .after(Duration::from_millis(300))
            .spawn();

        // Both kinds of park wake, aimed at a task that isn't parked
        wake_parked(handle.id(), Fired::Event);
        wake_parked(handle.id(), Fired::Deadline);

        thread::sleep(Duration::from_millis(50));

        assert!(
            handle.is_pending(),
            "a stale park wake started a delayed task early, which is now {:?}",
            handle.state(),
        );

        assert!(
            slot(handle.id()).is_some_and(|data| data.armed()),
            "a stale park wake took the delay's own wake",
        );

        handle
            .join()
            .expect("the delayed task still runs at its own time");
    }

    /// A task that panics fails alone, and everything queued
    /// around it still finishes
    #[test]
    fn panicking_task_does_not_lose_its_queue() {
        let _ = crate::Runtime::init();

        let quick = || Sleep::sleep(Duration::from_micros(50));

        let before: Vec<_> = (0..256)
            .map(|_| crate::Runtime::task(quick()).spawn())
            .collect();
        let doomed = crate::Runtime::task(Panics).spawn();
        let after: Vec<_> = (0..256)
            .map(|_| crate::Runtime::task(quick()).spawn())
            .collect();

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
