//! # Executor
//! Owns every task slot in the process and manages the pool of
//! workers that run them
//!
//! A `TaskHandle` is only an id, so every operation on a task
//! comes through here. The manager never sits between a task
//! and a worker; it only adapts the pool

use crate::{
    Runtime, RuntimeError,
    constants::{
        DEAD_KQUEUE_ID, MANAGER_TICK, MANAGER_TICK_IDENT, MAX_TASK_ID, NO_SELECT, NO_TASK,
        RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW, SCHEDULE_IDENT_BASE, SELECT_IDENT,
        SELECT_POLL, SHUTDOWN_POLL, WAKE_IDENT,
    },
    futures::task::Task,
    modules::{
        address_lock,
        erased_task::ErasedTask,
        event_desc::EventDesc,
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        kqueue,
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
    sync::{
        Mutex,
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
/// Only a `STOPPED` runtime is started and only a `RUNNING` one
/// is stopped, so a start and a stop never overlap. `SeqCst`
/// throughout
static LIFECYCLE: AtomicU8 = AtomicU8::new(STOPPED);

/// The thread supervising the manager, so a shutdown can wait
/// for it to be gone before the runtime can start again
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
    let supervisor = SUPERVISOR.lock().unwrap_or_else(|poisoned| poisoned.into_inner()).take();

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

        release(task);
    }

    LIFECYCLE.store(STOPPED, Ordering::SeqCst);
}

/// Manager deaths still owed, so tests can exercise the restart
/// path
static INJECTED_FAULTS: AtomicU32 = AtomicU32::new(0);

/// Makes the manager come apart the next `count` times it goes
/// round its loop
///
/// Past `RESTART_LIMIT` in one window, the supervisor gives up
/// until the runtime is shut down and started again
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
    /// Starts the manager and opens the pool
    ///
    /// Used by the first `Runtime::init` and by every one after a
    /// shutdown. The kqueue and the first workers are created on
    /// the calling thread, so a task spawned the moment `init`
    /// returns has somewhere to go
    ///
    /// ## Returns
    /// `AlreadyInit` when the runtime is already running. A
    /// shutdown or a start still in progress is waited out first
    pub(crate) fn init() -> Option<RuntimeError> {
        loop {
            match LIFECYCLE.compare_exchange(STOPPED, STARTING, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(RUNNING) => return Some(RuntimeError::AlreadyInit),
                Err(_) => thread::sleep(SHUTDOWN_POLL),
            }
        }

        let id = match unsafe { libc::kqueue() }.check() {
            Ok(id) => id,
            Err(error) => {
                LIFECYCLE.store(STOPPED, Ordering::SeqCst);
                return Some(error);
            }
        };

        EXECUTOR_KQUEUE_ID.store(id, Ordering::SeqCst);

        POOL.open();
        POOL.ensure_floor();
        supervise(id);

        LIFECYCLE.store(RUNNING, Ordering::SeqCst);

        None
    }

    /// Adds a new `Task` to be processed
    pub(crate) fn new_task<F>(task: F, setup: TaskSetup) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        create(task, setup).0
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

        // Still alone with the slot: no timer is armed and no handle
        // exists yet
        entry.set_prototype(prototype);

        let handle = TaskHandle::new(id);

        // A delayed schedule arms a one shot here, and its first tick
        // arms the repeating timer
        let started = match setup.start_delay.as_nanos() as u64 {
            0 => start_series(id, entry, setup),
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
    /// Whoever takes it to zero frees the slot, since nobody else
    /// can still see it
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
            TaskState::Cancelled | TaskState::Failed => true,

            // Over only if it isn't going round again
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
    pub(crate) fn wait(id: usize) -> Result<TaskState, RuntimeError> {
        Self::wait_until(id, None)
    }

    /// Blocks until a task settles or a deadline passes
    ///
    /// ## Returns
    /// How it settled, or `NotReady` if the deadline came first.
    /// `None` waits forever
    ///
    /// The time left is worked out afresh each time round, so
    /// spurious wakes can't stretch the wait
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

            // Checked after the state, so a task that has just settled
            // still answers
            if left.is_zero() {
                return Err(RuntimeError::NotReady);
            }

            if address_lock::wait_until(data.wait_address(), state as u32, left)? {
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
            return Err(RuntimeError::NoSuchTask);
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
    /// `claim_result` only wins against `Ready`, so an unsettled
    /// task turns this away by itself
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
    /// A task that hasn't started never will, and one in a kernel
    /// wait is woken out of it. Its output is dropped rather than
    /// published. On a repeat, this ends the whole series
    ///
    /// #### Note
    /// Never touches an output that has already landed, since a
    /// listener may be reading it
    pub(crate) fn cancel(id: usize) {
        let Some(data) = slot(id) else {
            return;
        };

        loop {
            let state = data.state();

            // Re-read each time round, since a bounded series can finish
            // meanwhile and change its kind
            let repeats = data.kind().repeats();

            match state {
                // Over already, one way or another
                TaskState::Cancelled | TaskState::Failed | TaskState::Free => return,

                // A one shot that has been taken is finished. A repeat in the
                // same state is only between runs
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
/// The handle, and whether anything will pick the task up. On
/// `false` the task has already settled `Failed`
fn create<F>(task: F, setup: TaskSetup) -> (TaskHandle<F::Output>, bool)
where
    F: Task,
{
    // Asked while the concrete type is still here, since a re-arm
    // can't ask
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

    // Armed instead of queued when there is a start delay
    let started = match setup.start_delay.as_nanos() as u64 {
        0 => queue(id, setup.blocking),
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

/// Spawns one run of a series, dropping its handle, since the
/// run's output goes to the series slot
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
/// A value that can't be published is dropped: the series was
/// cancelled, or another run published first
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

    // Cancelled mid write, so the value is left for the last
    // listener out to drop
    if !data.try_state(TaskState::Running, TaskState::Ready) {
        return;
    }

    wake(data);
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

    // Cancelled or failed, so it is dropped unrun
    if !data.begin() {
        drop(task);
        release(id);

        return;
    }

    // Cleared on every run, so a restarted manager knows the wait
    // it re-arms is a gap, not a start delay
    data.clear_start_delay();

    let reactor = Runtime::reactor_id();
    let payload = data.payload();

    // A panic costs this task, not the thread. `AssertUnwindSafe`
    // because the task is dropped unrun afterwards and its output
    // is never read
    CURRENT.with(|current| current.set(id));

    let finished = panic::catch_unwind(AssertUnwindSafe(|| unsafe {
        task.run(reactor, id, payload)
    }))
    .is_ok();

    CURRENT.with(|current| current.set(NO_TASK));

    if !finished {
        // A panic means the output was never written, so there is
        // nothing to drop. A series ends here too
        drop(task);

        if data.try_state(TaskState::Running, TaskState::Failed) {
            wake(data);
        }

        release(id);

        return;
    }

    data.fill();

    // Cancelled mid run, so the output is left for the last
    // listener out to drop
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

    // Bounds checked before the task goes back, so an unwanted run
    // is never queued. The count first, since it needs no clock
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
        || data.try_state(TaskState::Pending, TaskState::Failed)
    {
        wake(data);
    }

    release(id);
}

/// Starts a schedule whose first run goes now
///
/// The first run counts against `count` like any other. The
/// deadline isn't checked, since a window measured from now
/// always has room for the run at its start
///
/// ## Returns
/// Whether the series is under way
fn start_series(id: usize, data: &TaskData, setup: TaskSetup) -> bool {
    if !data.runs_remain() {
        return false;
    }

    if !launch(id, data) {
        return false;
    }

    // A schedule counts the runs it starts, since its runs overlap
    if data.count_run() {
        // The only run allowed is away, so there is no timer to arm.
        // The reference goes back now, since no tick will come for
        // it, and the run holds a claim of its own
        data.finish_series();
        release(id);

        return true;
    }

    schedule(id, setup.interval.as_nanos() as u64)
}

/// Starts the next run of a series, or clears the series up
///
/// A cancelled schedule comes off the clock here, at its next
/// tick, so only the manager ever touches its timer. Its slot
/// is held until then
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

    release(id);
}

/// Takes a schedule off the clock because it is finished
///
/// Its state is left alone, so its last output stays readable
fn end_schedule(id: usize, data: &TaskData) {
    unschedule(id);
    data.finish_series();
    release(id);
}

/// Whether a series has come to an end, which `Ready` and
/// `Taken` are not
#[inline(always)]
fn over(state: TaskState) -> bool {
    matches!(state, TaskState::Cancelled | TaskState::Failed)
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

    task.launch(id, data.priority_class())
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

    if data.state() == TaskState::Cancelled {
        return false;
    }

    data.set_waiting(queue);

    // Checked again, since a cancel between the check above and
    // the record would have found nothing to interrupt
    if data.state() == TaskState::Cancelled {
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

    data.state() != TaskState::Cancelled
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

    data.state() == TaskState::Cancelled
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
        TaskState::Taken => match !data.kind().repeats() && data.spent() {
            true => RuntimeError::Finished,
            false => RuntimeError::AlreadyTaken,
        },
        TaskState::Cancelled => RuntimeError::Cancelled,
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
/// Each slot is asked to poke this thread's queue when it
/// settles, which makes the answer prompt. The states are
/// re-read on every wake, which makes it right, so a missed
/// poke only costs `SELECT_POLL`
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
    let winner = loop {
        if let Some(done) = settled_any(ids) {
            break Some(done);
        }

        kqueue::wait_any(queue, SELECT_POLL);
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

/// Keeps the manager alive, restarting it with the same
/// backoff, window and limit as the `Reactor`
///
/// Nothing is lost on a restart, since no task lives on the
/// manager's stack
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

    *SUPERVISOR.lock().unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(supervisor);
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

        // Only when a test asks. Here, after the wakes have been
        // handed over, since that is the one place a death can lose
        // something
        if injected_fault() {
            panic!("injected manager fault");
        }

        // The tick carries nothing. Everything else is a task whose
        // interval is up
        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 || event.ident == MANAGER_TICK_IDENT {
                continue;
            }

            fire(event.ident);
        }
    }
}

/// Writes off everything the manager's queue was driving, once
/// that queue has closed
///
/// A schedule's slot would otherwise be held for the life of
/// the process. A run publishing into a series holds its own
/// claim, so releasing here is safe
fn orphaned() {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

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

        release(task);
    }
}

/// Puts back the wakes a dead manager took down with it
///
/// A manager that dies holding a batch of wakes loses them, so
/// every wait still marked armed is armed again. Re-arming one
/// that wasn't lost is harmless: `EV_ADD` replaces the timer,
/// and the wake is claimed once
///
/// A schedule needs none of this, since its timer repeats
fn recover_waits() {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

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
/// a manager. Tasks are only written off when nothing is left
/// that could run them
fn shutdown(id: i32) {
    EXECUTOR_KQUEUE_ID.store(DEAD_KQUEUE_ID, Ordering::SeqCst);
    let _ = unsafe { libc::close(id) };

    // True whether or not the pool survives
    orphaned();

    // Every chance to carry on before anything is written off
    POOL.ensure_floor();

    if POOL.live() > 0 {
        return;
    }

    // Nothing is left running. The pool is shut until a shutdown
    // and an `init` start it again, since everything below is about
    // to be failed and its slot given back
    POOL.close();
    POOL.abandon();

    for task in POOL.injector().drain() {
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

        // Safe to repeat, since `release` is idempotent
        release(task);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Sleep, futures::task::sealed};
    use std::time::Duration;

    /// A task that panics
    struct Panics;

    impl sealed::Sealed for Panics {}

    impl Task for Panics {
        type Output = usize;

        fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
            panic!("this task is meant to go down");
        }
    }

    /// A task that blocks its thread without saying so, so it
    /// stays on a worker
    struct Liar;

    impl sealed::Sealed for Liar {}

    impl Task for Liar {
        type Output = usize;

        fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
            thread::sleep(Duration::from_millis(200));

            0
        }
    }

    /// The pool grows when tasks hold their workers and nothing
    /// finishes
    #[test]
    fn pool_grows_when_tasks_hold_their_workers() {
        crate::Runtime::init();

        let cores = thread::available_parallelism()
            .map(|count| count.get())
            .unwrap_or(1);

        let handles: Vec<_> = (0..cores * 2)
            .map(|_| crate::Runtime::task(Liar).spawn())
            .collect();

        // Long enough for several manager ticks, well short of the
        // tasks ending
        thread::sleep(Duration::from_millis(150));

        let grown = crate::Runtime::workers().workers().len();

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

    /// A task that panics fails alone, and everything queued
    /// around it still finishes
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
