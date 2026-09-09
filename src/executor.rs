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
        RESTART_LIMIT, RESTART_WINDOW, WAKE_IDENT,
    },
    futures::task::Task,
    modules::{
        address_lock,
        erased_task::ErasedTask,
        event_desc::EventDesc,
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        task_data::TaskData,
        task_handle::TaskHandle,
        task_kind::TaskKind,
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
    sync::atomic::{AtomicI32, AtomicU64, Ordering},
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
    pub(crate) fn new_task<F>(task: F, priority: u8, kind: TaskKind) -> TaskHandle<F::Output>
    where
        F: Task,
    {
        // Asked here, where the task is still itself, rather
        // than by the worker that picks it up. A blocking task
        // wants a thread that can be blocked, not a core, so
        // sending it to the workers' queue would have it wait
        // behind every bit of unrelated work in the process for
        // a resource it never wanted
        let blocking = task.blocking();

        let boxed: Box<dyn ErasedTask> = Box::new(task);
        let erased = Box::into_raw(Box::new(boxed)).cast::<c_void>();

        let Some(id) = DATA.alloc() else {
            return failed(erased);
        };

        let Some(entry) = DATA.slot(id) else {
            DATA.free(id);
            return failed(erased);
        };

        let ready = unsafe {
            TaskData::init::<F::Output>(
                entry as *const TaskData as *mut TaskData,
                erased,
                TaskState::Pending,
                kind,
                blocking,
                priority,
                SEQUENCE.fetch_add(1, Ordering::Relaxed),
            )
        };

        if !ready {
            DATA.free(id);
            return failed(erased);
        }

        let handle = TaskHandle::new(id);

        // Queued and somebody will come for it, which is every
        // case but a pool that is gone and won't restart
        if queue(id, blocking) {
            return handle;
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

    /// The state a task is currently in
    pub(crate) fn state(id: usize) -> TaskState {
        match slot(id) {
            Some(data) => data.state(),
            None => TaskState::Failed,
        }
    }

    /// Blocks until a task settles, and says how it settled
    ///
    /// ## Behaviour
    /// The wait sleeps only while the state word still reads
    /// the value it was given, so a state that has already
    /// moved on doesn't sleep at all and the loop simply looks
    /// again
    pub(crate) fn wait(id: usize) -> Result<TaskState, RuntimeError> {
        let Some(data) = slot(id) else {
            return Err(RuntimeError::ExecutorDead);
        };

        loop {
            let state = data.state();

            if state.terminal() {
                return Ok(state);
            }

            address_lock::wait(data.wait_address(), state as u32)?;
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
        let Some(data) = slot(id) else {
            return Err(RuntimeError::ExecutorDead);
        };

        loop {
            settled(Self::wait(id)?)?;

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
            let error = lost(data.state());

            if error != RuntimeError::NotReady {
                return Err(error);
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
            return Err(RuntimeError::ExecutorDead);
        };

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
        let Some(data) = slot(id) else {
            return Err(RuntimeError::ExecutorDead);
        };

        loop {
            settled(Self::wait(id)?)?;

            if data.claim_result() {
                break;
            }

            // Same race as `clone_result`, and the same answer
            let error = lost(data.state());

            if error != RuntimeError::NotReady {
                return Err(error);
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

    // Round again, in the same slot, with the same box. The
    // state is left at `Ready` so listeners can read the run
    // that just finished while the next one is queued, and the
    // `Executor`'s reference is held rather than given back,
    // because it stands for the series and not for one run
    data.rearm(Box::into_raw(task).cast::<c_void>());

    if queue(id, data.blocking()) {
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
fn settled(state: TaskState) -> Result<(), RuntimeError> {
    if state == TaskState::Ready {
        return Ok(());
    }

    Err(lost(state))
}

/// Why a task that isn't `Ready` has nothing to hand out
#[inline(always)]
fn lost(state: TaskState) -> RuntimeError {
    match state {
        TaskState::Taken => RuntimeError::AlreadyTaken,
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
        TaskState::Free => RuntimeError::ExecutorDead,
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
/// handle comes back `ExecutorDead` instead of blocking
fn failed<T>(erased: *mut c_void) -> TaskHandle<T> {
    drop(unsafe { Box::from_raw(erased.cast::<Box<dyn ErasedTask>>()) });

    TaskHandle::new(MAX_TASK_ID)
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

    loop {
        POOL.tick();

        let count = match unsafe { KEvent::listen(id, &mut events) }.check() {
            Ok(count) => count as usize,
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(_) => break,
        };

        // Nothing in the events is worth reading. Being woken
        // is the whole of the message, and what to do about it
        // is whatever the next pass of policy decides
        for event in events.iter().take(count) {
            if event.flags & libc::EV_ERROR != 0 {
                continue;
            }
        }
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

    // Only tasks nothing has started. A `Running` task has a
    // thread inside it that will give the reference back
    // itself, and one whose thread died was already settled by
    // the sweep above, so failing either here would give the
    // same reference back twice
    for task in 0..DATA.high_water() {
        let Some(data) = slot(task) else {
            continue;
        };

        if data.state() != TaskState::Pending {
            continue;
        }

        data.set_state(TaskState::Failed);
        wake(data);

        // The reference the `Executor` took at creation, which
        // nothing is going to be around to give back otherwise
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

        fn prepare(&mut self) {}

        fn get_intptr_t_data(&self) -> libc::intptr_t {
            0
        }

        fn offload(&self, _queue: Option<i32>, _reactor_id: i32, _task_id: usize) -> Self::Output {
            0
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

        fn prepare(&mut self) {}

        fn get_intptr_t_data(&self) -> libc::intptr_t {
            0
        }

        fn offload(&self, _queue: Option<i32>, _reactor_id: i32, _task_id: usize) -> Self::Output {
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
            .map(|_| crate::Runtime::spawn(Liar))
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

        let before: Vec<_> = (0..256).map(|_| crate::Runtime::spawn(quick())).collect();
        let doomed = crate::Runtime::spawn(Panics);
        let after: Vec<_> = (0..256).map(|_| crate::Runtime::spawn(quick())).collect();

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
