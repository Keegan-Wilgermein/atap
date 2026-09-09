//! # Executor
//! Executes async tasks as they
//! are ready, delegates IDs
//! across threads, and communicates
//! with `TaskHandle`s
//!
//! The `Executor` owns every task slot in the process. A
//! `TaskHandle` holds nothing but an id and comes back
//! through here for everything, so there is exactly one
//! thing in the crate that can see a slot, and it is the
//! one thing that knows whether that slot is still alive
//!
//! Work arrives as `EVFILT_USER` events on the `Executor`'s
//! own kqueue. Spawning raises one, the loop blocks on the
//! same `kevent` call the `Reactor` blocks on, and no lock
//! or channel sits between the two

use crate::{
    Runtime, RuntimeError,
    constants::{DEAD_KQUEUE_ID, MAX_TASK_ID, RESTART_BACKOFF, RESTART_LIMIT, RESTART_WINDOW},
    futures::task::Task,
    modules::{
        erased_task::ErasedTask,
        event_desc::EventDesc,
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        task_data::TaskData,
        task_handle::TaskHandle,
        task_state::TaskState,
        task_table::TaskTable,
    },
};
use libc::c_void;
use std::{
    io::Error,
    mem, ptr,
    sync::atomic::{AtomicI32, Ordering},
    thread,
    time::Instant,
};

/// Every task in the process, addressed by id
///
/// A plain static rather than anything thread local, because
/// the thread that spawns a task, the thread that runs it and
/// the thread that reads its result are three different
/// threads and all of them have to find the same slot
static DATA: TaskTable = TaskTable::new();

/// The kqueue the `Executor` takes its work from
///
/// Only `Relaxed` reads for speed, with a single `SeqCst`
/// write at initialisation and another if the `Executor`
/// ever gives up, so that everything sees both
static EXECUTOR_KQUEUE_ID: AtomicI32 = AtomicI32::new(DEAD_KQUEUE_ID);

/// Async task executor and handler
pub(crate) struct Executor;

impl Executor {
    /// Initialises a new `Executor`
    ///
    /// The kqueue is created here, on the calling thread, so
    /// that a task spawned the instant `Runtime::init()`
    /// returns has somewhere to be delivered. Only the
    /// supervisor is put on a thread of its own
    pub(crate) fn init() -> Option<RuntimeError> {
        let id = match unsafe { libc::kqueue() }.check() {
            Ok(id) => id,
            Err(error) => return Some(error),
        };

        EXECUTOR_KQUEUE_ID.store(id, Ordering::SeqCst);
        supervise(id);

        None
    }

    /// Adds a new `Task` to be processed
    ///
    /// Publishing the slot before raising the event matters.
    /// The loop looks the id up the moment the event lands,
    /// so the task has to be findable before anything is
    /// told to go looking for it
    pub(crate) fn new_task<F>(task: F) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        let boxed: Box<dyn ErasedTask> = Box::new(task);
        let erased = Box::into_raw(Box::new(boxed)).cast::<c_void>();

        let queue = EXECUTOR_KQUEUE_ID.load(Ordering::Relaxed);
        let alive = queue != DEAD_KQUEUE_ID;

        // A task handed to a dead `Executor` is never going
        // to run, so it is born finished rather than left for
        // a listener to block on forever
        let state = match alive {
            true => TaskState::Pending,
            false => TaskState::Failed,
        };

        let Some(id) = DATA.alloc() else {
            return failed(erased);
        };

        let Some(entry) = DATA.slot(id) else {
            DATA.free(id);
            return failed(erased);
        };

        let data = TaskData::create::<F::Output>(erased, state);

        if data.is_null() {
            DATA.free(id);
            return failed(erased);
        }

        entry.publish(data);

        let handle = TaskHandle::new(id);

        // Nothing is ever going to pick this up, so the
        // reference every task holds for the `Executor` is
        // given back here rather than by the run that will
        // never happen. Without it the slot would sit in the
        // table for the life of the process
        if !alive {
            release(id);
            return handle;
        }

        let raised = unsafe {
            KEvent::register(
                queue,
                id,                          // The id comes back as the event's ident
                0,                           // Nothing to tell the kernel about it
                ptr::null_mut(),
                EventDesc::new_user_trigger(),
            )
        }
        .check();

        // The task is in the table but nothing has been told
        // to come and get it, which is the same dead end as
        // spawning onto an `Executor` that has already gone
        if raised.is_err() {
            if let Some(published) = slot(id) {
                published.set_state(TaskState::Failed);
                wake(published);
            }

            release(id);
        }

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

        // Emptied before it is unmapped so that nothing can
        // find the slot in the window between the two
        if let Some(entry) = DATA.slot(id) {
            entry.clear();
        }

        unsafe { TaskData::destroy(data as *const TaskData as *mut TaskData) };

        // Only once the memory is gone, since the id is live
        // again the moment it lands on the free list
        DATA.free(id);
    }

    /// The state a task is currently in
    pub(crate) fn state(id: usize) -> TaskState {
        return match slot(id) {
            Some(data) => data.state(),
            None => TaskState::Failed,
        };
    }

    /// Blocks until a task settles, and says how it settled
    ///
    /// ## Behaviour
    /// `os_sync_wait_on_address` sleeps only while the word
    /// still reads the value it was given, so a state that
    /// has already moved on doesn't sleep at all and the
    /// loop simply looks again
    pub(crate) fn wait(id: usize) -> Result<TaskState, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::ExecutorDead);
        };

        loop {
            let state = data.state();

            if state.terminal() {
                return Ok(state);
            }

            let status = unsafe {
                libc::os_sync_wait_on_address(
                    data.wait_address(),
                    state as u64,                        // Sleep only while it still reads this
                    mem::size_of::<u32>(),               // The state word, not the output
                    libc::OS_SYNC_WAIT_ON_ADDRESS_NONE,  // Single process waiting
                )
            };

            if status >= 0 {
                continue;
            }

            let error = Error::last_os_error().raw_os_error();

            // A signal, or a state that moved between the
            // read and the wait, both just mean go round again
            if error != Some(libc::EINTR) && error != Some(libc::EAGAIN) {
                return Err(RuntimeError::AddressLock);
            }
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
        let state = Self::wait(id)?;
        settled(state)?;

        let Some(data) = slot(id) else {
            return Err(RuntimeError::ExecutorDead);
        };

        // Held for as long as the clone takes, so that a
        // `take` on another thread waits rather than moving
        // the output away part way through reading it
        if !data.enter_read() {
            return Err(lost(data.state()));
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        let value = unsafe { (*data.payload().cast::<T>()).clone() };
        data.leave_read();

        Ok(value)
    }

    /// Waits for a task and moves its output out
    ///
    /// Winning the move to `Taken` is what makes this the one
    /// caller that owns the output, and what makes every
    /// later read fail rather than hand out a second owner of
    /// the same value
    pub(crate) fn take_result<T>(id: usize) -> Result<T, RuntimeError> {
        let state = Self::wait(id)?;
        settled(state)?;

        let Some(data) = slot(id) else {
            return Err(RuntimeError::ExecutorDead);
        };

        // Losing this means another listener got there first,
        // or a cancel landed between the wait and the move
        if !data.claim_result() {
            return Err(lost(data.state()));
        }

        debug_assert_eq!(data.size(), mem::size_of::<T>());

        data.empty();

        Ok(unsafe { ptr::read(data.payload().cast::<T>()) })
    }

    /// Abandons a task
    ///
    /// ## Behaviour
    /// A task that hasn't started never will. One already in
    /// flight runs to the end, because a `SleepTask` sitting
    /// in `kevent` has no way to be taken back off the
    /// kernel, and its output is dropped instead of published
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

        loop {
            let state = data.state();

            if state.terminal() && state != TaskState::Ready {
                return;
            }

            if data.try_state(state, TaskState::Cancelled) {
                wake(data);
                return;
            }
        }
    }
}

/// The slot for an id, if it is still live
#[inline(always)]
fn slot(id: usize) -> Option<&'static TaskData> {
    let entry = DATA.slot(id)?;
    let data = entry.data();

    if data.is_null() {
        return None;
    }

    return Some(unsafe { &*data });
}

/// Turns a settled state into the error it stands for
#[inline(always)]
fn settled(state: TaskState) -> Result<(), RuntimeError> {
    if state == TaskState::Ready {
        return Ok(());
    }

    return Err(lost(state));
}

/// Why a task that isn't `Ready` has nothing to hand out
#[inline(always)]
fn lost(state: TaskState) -> RuntimeError {
    return match state {
        TaskState::Taken => RuntimeError::AlreadyTaken,
        TaskState::Cancelled => RuntimeError::Cancelled,
        _ => RuntimeError::ExecutorDead,
    };
}

/// Wakes every listener blocked on a slot
#[inline(always)]
fn wake(data: &TaskData) {
    let _ = unsafe {
        libc::os_sync_wake_by_address_all(
            data.wait_address(),
            mem::size_of::<u32>(),                  // The state word, not the output
            libc::OS_SYNC_WAKE_BY_ADDRESS_NONE,     // Single process waiting
        )
    };
}

/// A handle for a task that never made it into the table
///
/// The task is dropped here rather than run, and the id is
/// one no slot will ever answer to, so every read on the
/// handle comes back `ExecutorDead` instead of blocking
fn failed<T>(erased: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(erased.cast::<Box<dyn ErasedTask>>()) });

    return TaskHandle::new(MAX_TASK_ID);
}

/// Keeps the executor loop alive
///
/// The same backoff, window and limit the `Reactor` gets,
/// but without the channel. `join` comes back for a clean
/// exit and a panic alike, so there is nothing the loop has
/// to remember to report on its way out
fn supervise(id: i32) {
    thread::spawn(move || {
        let mut failures = 0;
        let mut started = Instant::now();

        loop {
            let _ = thread::spawn(move || executor_loop(id)).join();

            if started.elapsed() >= RESTART_WINDOW {
                failures = 0;
            }

            failures += 1;

            if failures > RESTART_LIMIT {
                shutdown(id);
                break;
            }

            thread::sleep(RESTART_BACKOFF * failures);
            requeue(id);

            started = Instant::now();
        }
    });
}

/// The loop the executor runs on
fn executor_loop(id: i32) {
    let mut events = eventlist();

    loop {
        let count = match unsafe { KEvent::listen(id, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => break,
        };

        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
                continue;
            }

            run(event.ident);
        }
    }
}

/// Runs one task and publishes what comes back
fn run(id: usize) {
    let Some(data) = slot(id) else {
        return;
    };

    // Claiming is what makes a task run at most once, so a
    // second event for the same id finds nothing and leaves
    // the reference alone for the run that did claim it
    let task = data.claim();

    if task.is_null() {
        return;
    }

    let task = unsafe { Box::from_raw(task.cast::<Box<dyn ErasedTask>>()) };

    // Cancelled before it ever got going, so it is dropped
    // rather than run and nothing is ever published
    if !data.try_state(TaskState::Pending, TaskState::Running) {
        drop(task);
        release(id);
        return;
    }

    let task: Box<dyn ErasedTask> = *task;
    unsafe { task.run(Runtime::reactor_id(), id, data.payload()) };

    data.fill();

    // A listener that cancelled part way through isn't coming
    // back for this, so the output is left for the last one
    // out to drop rather than published
    if data.try_state(TaskState::Running, TaskState::Ready) {
        wake(data);
    }

    release(id);
}

/// Gives up the `Executor`'s reference on a task
///
/// Taken at creation and held until the task is finished
/// with, so that a handle dropped the instant it is handed
/// out can't free the slot underneath the thread that is
/// about to run it
#[inline(always)]
fn release(id: usize) {
    Executor::drop_listener(id);
}

/// Picks up after a loop that died part way through
///
/// ## Behaviour
/// `EV_ONESHOT` means the kernel drops an event once it has
/// handed it over, so a task the old loop was told about but
/// never got to needs telling about again
///
/// A task the old loop had already claimed is a different
/// problem. Its `Task` went down with the thread, so nothing
/// is left that could finish it and no amount of retriggering
/// would help. Those are failed here instead, so that a
/// listener blocked on one is let go rather than left waiting
/// on a result that is never coming
fn requeue(id: i32) {
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        match (data.state(), data.unclaimed()) {
            // Still sitting there, so ask for it again
            (TaskState::Pending, true) => {
                let _ = unsafe {
                    KEvent::register(id, task, 0, ptr::null_mut(), EventDesc::new_user_trigger())
                }
                .check();
            }

            // Taken by the loop that died, either side of it
            // having marked the task as running
            (TaskState::Pending, false) | (TaskState::Running, _) => {
                data.set_state(TaskState::Failed);
                wake(data);

                // The reference the run that died was holding,
                // which it is no longer around to give back
                release(task);
            }

            _ => {}
        }
    }
}

/// Closes the `Executor` down for good
///
/// Every task still waiting is failed and its listeners are
/// woken, because a listener blocked on a task that nothing
/// is left to run would otherwise block for the life of the
/// process
fn shutdown(id: i32) {
    EXECUTOR_KQUEUE_ID.store(DEAD_KQUEUE_ID, Ordering::SeqCst);
    let _ = unsafe { libc::close(id) };

    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        if data.state().terminal() {
            continue;
        }

        data.set_state(TaskState::Failed);
        wake(data);

        // The reference the `Executor` took at creation, which
        // nothing is going to be around to give back otherwise
        release(task);
    }
}
