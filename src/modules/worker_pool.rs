//! # Worker Pool
//! Every worker in the process, the queue they all pull from,
//! and the policy the manager applies to them
//!
//! The array is static and fixed. Workers are claimed out of it
//! and given back to it, and the memory behind one never moves
//! and is never freed, which is what lets a worker be parked on
//! the address of its own state word and cleaned up after by a
//! thread that never knew it
//!
//! Nothing in here is on the path a task takes to a worker. The
//! manager flips batches, grows, reaps and clears up after the
//! dead, and a worker finds its own work whether the manager is
//! running or not. That is deliberate: a pool that stops when
//! its manager stops isn't a pool that recovers

use crate::{
    constants::{
        IDLE_REAP, MANAGER_TICK, MAX_WORKERS, NO_TASK, SLEEP_MULTIPLIER, STARVE_AGE,
        WORKER_MULTIPLIER,
    },
    executor,
    modules::{
        injector::Injector, pool_stats::PoolStats, sleep_thread::SleepThread, worker::Worker,
        worker_state::WorkerState, worker_stats::WorkerStats,
    },
};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

/// Every worker in the process
pub(crate) static POOL: WorkerPool = WorkerPool::new();

/// The online core count, or 0 before it has been asked for
static CORES: AtomicUsize = AtomicUsize::new(0);

/// The pool of workers and the work waiting for them
pub(crate) struct WorkerPool {
    /// Every worker slot, used or not
    ///
    /// Fixed and static rather than grown, because a worker's
    /// state word is an address threads park on and its ring
    /// has to outlive the thread reading from it. Neither
    /// survives an array that reallocates
    workers: [Worker; MAX_WORKERS],

    /// The queue every task lands in
    injector: Injector,

    /// The threads that exist to be blocked
    ///
    /// Shared rather than one per worker. A worker that owned
    /// its own would put every blocking task it happened to
    /// find onto that one thread, and since the first worker
    /// awake drains the queue before the others have woken,
    /// one thread would end up running the lot in sequence
    sleeps: [SleepThread; MAX_WORKERS],

    /// Tasks that said they would block, waiting for one of
    /// those threads
    blocking: Injector,

    /// Slots currently claimed
    live: AtomicUsize,

    /// Sleep thread slots currently claimed
    sleeps_live: AtomicUsize,

    /// One past the highest sleep thread slot ever claimed
    sleeps_highest: AtomicUsize,

    /// Sleep threads asleep on their own state word
    sleeps_parked: AtomicU32,

    /// One past the highest slot ever claimed
    ///
    /// Everything that walks the pool stops here rather than at
    /// `MAX_WORKERS`, so the walk is the size of the pool that
    /// actually exists and not the size of the array holding it
    highest: AtomicUsize,

    /// Workers asleep on their own state word
    ///
    /// Read on every submission, so that the common case of
    /// everybody being awake costs one relaxed load and no
    /// walk of the pool at all
    parked: AtomicU32,

    /// Whether the pool has been shut for good
    ///
    /// Only ever set when the manager has given up *and* the
    /// pool couldn't be restarted, at which point everything
    /// queued has been failed and its slots given back. Coming
    /// back after that would mean queueing ids that now belong
    /// to somebody else
    stopped: AtomicBool,

    /// Where the next search for somebody to wake starts
    ///
    /// Rotated so a wake doesn't always land on the same
    /// worker. Starting at zero every time means the first slot
    /// takes every idle period the pool has, which spreads
    /// nothing and keeps one ring hot while the rest go cold
    wake: AtomicUsize,

    /// Where the next peer sweep starts
    ///
    /// Workers check one slot each time round their loop, so
    /// a dead one is found even with no manager running. A
    /// rotating cursor spreads that check out rather than
    /// having every worker look at slot zero forever
    sweep: AtomicUsize,
}

impl WorkerPool {
    /// An empty pool
    ///
    /// A `const fn` so it can be a plain static with no lazy
    /// initialisation guarding every access
    pub(crate) const fn new() -> Self {
        Self {
            workers: [const { Worker::new() }; MAX_WORKERS],
            injector: Injector::new(),
            sleeps: [const { SleepThread::new() }; MAX_WORKERS],
            blocking: Injector::new(),
            live: AtomicUsize::new(0),
            sleeps_live: AtomicUsize::new(0),
            sleeps_highest: AtomicUsize::new(0),
            sleeps_parked: AtomicU32::new(0),
            highest: AtomicUsize::new(0),
            parked: AtomicU32::new(0),
            stopped: AtomicBool::new(false),
            wake: AtomicUsize::new(0),
            sweep: AtomicUsize::new(0),
        }
    }

    /// The queue every task lands in
    #[inline(always)]
    pub(crate) fn injector(&self) -> &Injector {
        &self.injector
    }

    /// The queue blocking tasks wait in
    #[inline(always)]
    pub(crate) fn blocking(&self) -> &Injector {
        &self.blocking
    }

    /// Workers currently running
    #[inline(always)]
    pub(crate) fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// Sleep threads currently running
    #[inline(always)]
    pub(crate) fn sleeps_live(&self) -> usize {
        self.sleeps_live.load(Ordering::Acquire)
    }

    /// Hands a task that will block to a thread that can be
    /// blocked
    ///
    /// ## Behaviour
    /// A thread already asleep is woken; otherwise one is
    /// started on the spot rather than waited for, because a
    /// blocking task holds its thread for as long as it feels
    /// like and making it wait a tick for one is a tick of
    /// nothing happening
    ///
    /// Over-provisions slightly, since a thread that is awake
    /// but not yet looking counts as unavailable. The reaper
    /// takes those back
    pub(crate) fn offload(&'static self, id: usize) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }

        // Checked before the task is queued rather than after,
        // so a pool that genuinely can't take it says so while
        // the task is still the caller's to fail
        if self.sleeps_live() == 0 && !self.start_sleep() {
            return false;
        }

        self.blocking.push(id);

        if self.sleeps_parked.load(Ordering::SeqCst) != 0 {
            self.wake_sleep();
            return true;
        }

        // Everything already inside a task, and this one will
        // hold whatever takes it for as long as it likes, so it
        // gets a thread rather than a place in a queue
        self.start_sleep();

        true
    }

    /// Notes that a sleep thread has gone to sleep
    #[inline(always)]
    pub(crate) fn sleep_parked_in(&self) {
        self.sleeps_parked.fetch_add(1, Ordering::SeqCst);
    }

    /// Notes that a sleep thread has woken back up
    #[inline(always)]
    pub(crate) fn sleep_parked_out(&self) {
        self.sleeps_parked.fetch_sub(1, Ordering::SeqCst);
    }

    /// Notes that a sleep thread has given its slot back
    #[inline(always)]
    pub(crate) fn sleep_left(&self) {
        self.sleeps_live.fetch_sub(1, Ordering::AcqRel);
    }

    /// Starts one sleep thread, if there is room for one
    fn start_sleep(&'static self) -> bool {
        if self.stopped.load(Ordering::Acquire) || self.sleeps_live() >= sleep_cap() {
            return false;
        }

        for index in 0..MAX_WORKERS {
            let sleep = &self.sleeps[index];

            if !sleep.claim() {
                continue;
            }

            if !sleep.start() {
                return false;
            }

            self.sleeps_live.fetch_add(1, Ordering::AcqRel);
            raise(&self.sleeps_highest, index + 1);

            return true;
        }

        false
    }

    /// Wakes one parked sleep thread
    fn wake_sleep(&'static self) {
        let highest = self.sleeps_highest.load(Ordering::Acquire);

        if highest == 0 {
            return;
        }

        let start = self.wake.fetch_add(1, Ordering::Relaxed);

        for offset in 0..highest {
            let sleep = &self.sleeps[(start + offset) % highest];

            if sleep.state() == WorkerState::Parked {
                sleep.wake();
                return;
            }
        }
    }

    /// Queues a task and makes sure somebody will come for it
    ///
    /// ## Behaviour
    /// The task is queued before anything is woken, and a
    /// worker publishes that it is parked before it looks at
    /// the queue for the last time. So either the worker sees
    /// this task, or this sees the worker parked and wakes it.
    /// Both stores are sequentially consistent, which is what
    /// rules out neither happening
    ///
    /// Under load nothing is parked, so this is a push and a
    /// relaxed load and makes no syscall at all
    pub(crate) fn submit(&'static self, id: usize) -> bool {
        // Nothing alive to come for it, so whoever spawned it
        // starts the pool back up. Anyone who spawns can bring
        // the pool back, which is what stops a total wipeout
        // being the end of it
        //
        // Checked before the task is queued rather than after,
        // so a pool that genuinely can't be started says so
        // while the task is still the caller's to fail
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }

        if self.live() == 0 {
            self.ensure_floor();

            if self.live() == 0 {
                return false;
            }
        }

        self.injector.push(id);

        // Nobody is asleep, so nobody needs telling. This is
        // the whole of the wake under load: one relaxed read
        // of a counter and no syscall
        if self.parked.load(Ordering::SeqCst) != 0 {
            self.wake_one();
        }

        true
    }

    /// Finds something for a worker to do
    ///
    /// In order of how much it costs to get at: the worker's
    /// own ring first, then the shared queue, then somebody
    /// else's ring. Taking a batch out of the shared queue
    /// rather than one task keeps every worker from coming
    /// back to it between every single task
    pub(crate) fn find_work(&'static self, worker: &'static Worker) -> Option<usize> {
        if let Some(id) = worker.pop() {
            return Some(id);
        }

        if let Some(id) = self.injector.pop() {
            // Topping the ring up while it is here anyway, so
            // the next several tasks cost a local pop
            //
            // A share of what is queued rather than as much as
            // will fit. Taking the lot would have the first
            // worker awake hoard a whole batch, which is slower
            // than not batching at all when those tasks each
            // want a thread of their own. A share leaves every
            // other worker something to find
            let share = (self.injector.len() / self.live().max(1)).min(LOCAL_REFILL);

            while worker.backlog() < share {
                let Some(extra) = self.injector.pop() else {
                    break;
                };

                if !worker.push(extra) {
                    self.injector.push(extra);
                    break;
                }
            }

            return Some(id);
        }

        self.steal(worker)
    }

    /// Takes work off a peer that has more than it needs
    fn steal(&'static self, thief: &'static Worker) -> Option<usize> {
        let highest = self.highest.load(Ordering::Acquire);

        for offset in 0..highest {
            let index = (self.sweep.fetch_add(1, Ordering::Relaxed) + offset) % highest.max(1);
            let victim = &self.workers[index];

            if std::ptr::eq(victim, thief) || !victim.state().alive() {
                continue;
            }

            if victim.steal_into(thief) == 0 {
                continue;
            }

            if let Some(id) = thief.pop() {
                return Some(id);
            }
        }

        None
    }

    /// Checks one slot for a worker that died without saying so
    ///
    /// Called by workers as they go round, so the pool clears
    /// up after itself even with no manager running. One slot
    /// per pass, because this is on a worker's own path and a
    /// full walk of the pool is not
    pub(crate) fn sweep_one(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        if highest == 0 {
            return;
        }

        let index = self.sweep.fetch_add(1, Ordering::Relaxed) % highest;

        if self.workers[index].state().needs_recovery() {
            self.recover(index);
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        if sleeps != 0 && self.sleeps[index % sleeps].state().needs_recovery() {
            self.recover_sleep(index % sleeps);
        }
    }

    /// Picks up after a worker that went down
    ///
    /// ## Behaviour
    /// Everything the worker still had queued goes back to the
    /// shared queue, where anybody can reach it. The one task
    /// it had already claimed is failed instead: its `Task` was
    /// swapped out of the slot before it started and went down
    /// with the thread, so there is nothing left to run again
    /// and a listener waiting on it should be let go rather
    /// than left waiting on a result that isn't coming
    ///
    /// A sleep thread still winding down holds the slot back
    /// until it has gone, so a fresh worker never inherits one
    /// that is half way out
    pub(crate) fn recover(&'static self, index: usize) {
        let worker = &self.workers[index];

        let (queued, stranded) = worker.recover();

        for id in queued {
            self.injector.push(id);
        }

        if stranded != NO_TASK {
            executor::fail(stranded);
        }

        worker.release();
        self.left();
    }

    /// Picks up after a sleep thread that went down
    ///
    /// Only the task it had claimed is lost, and only because
    /// that task's `Task` went down with the thread. Anything
    /// it hadn't started is still in the shared blocking queue,
    /// which is the point of that queue being shared
    pub(crate) fn recover_sleep(&'static self, index: usize) {
        let stranded = self.sleeps[index].recover();

        if stranded != NO_TASK {
            executor::fail(stranded);
        }

        self.sleep_left();
    }

    /// Starts workers until there are at least as many as
    /// there are cores
    pub(crate) fn ensure_floor(&'static self) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }

        while self.live() < floor() {
            if !self.start_one() {
                return;
            }
        }
    }

    /// Starts one worker, if there is room for one
    pub(crate) fn start_one(&'static self) -> bool {
        if self.live() >= cap() {
            return false;
        }

        for index in 0..MAX_WORKERS {
            let worker = &self.workers[index];

            if !worker.claim() {
                continue;
            }

            if !worker.start() {
                return false;
            }

            self.live.fetch_add(1, Ordering::AcqRel);
            raise(&self.highest, index + 1);

            return true;
        }

        false
    }

    /// One pass of the manager's policy
    ///
    /// Everything in here is something a worker can't do for
    /// itself: it can't decide the pool is too small, it can't
    /// decide it is too big, and it can't see how long the
    /// oldest queued task has been waiting
    pub(crate) fn tick(&'static self) {
        self.injector.refill();
        self.age();
        self.sweep_all();
        self.ensure_floor();
        self.balance();
        self.grow();
        self.reap();
        self.reap_sleeps();
    }

    /// Stops sleep threads that have had nothing to do
    ///
    /// No floor under these. A process that never spawns a
    /// blocking task should not be holding threads open for
    /// the possibility, and starting one costs a thread spawn
    /// against a task that was going to hold it for
    /// milliseconds at least
    fn reap_sleeps(&'static self) {
        let highest = self.sleeps_highest.load(Ordering::Acquire);
        let ticks = idle_ticks();

        for index in 0..highest {
            let sleep = &self.sleeps[index];

            if !sleep.state().alive() {
                continue;
            }

            if sleep.state() != WorkerState::Parked {
                sleep.busied();
                continue;
            }

            if sleep.idled() >= ticks {
                sleep.stop();
            }
        }
    }

    /// Wakes parked workers when there is work they could take
    ///
    /// ## Behaviour
    /// Woken and not handed anything, because a live worker's
    /// ring has exactly one writer and the manager isn't it.
    /// A worker that wakes finds its own work, out of the
    /// shared queue or off a peer, which is the same path it
    /// takes every other time round its loop
    ///
    /// Only as many are woken as there is work for, so a
    /// single queued task doesn't start a stampede
    fn balance(&'static self) {
        if self.parked.load(Ordering::SeqCst) == 0 {
            return;
        }

        let highest = self.highest.load(Ordering::Acquire);
        let mut spare = self.injector.len();

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.state().alive() {
                continue;
            }

            // Everything past the one it is about to run is
            // work somebody else could be having instead
            spare += worker.backlog().saturating_sub(1);
        }

        for index in 0..highest {
            if spare == 0 {
                return;
            }

            let worker = &self.workers[index];

            if worker.state() == WorkerState::Parked {
                worker.wake();
                spare -= 1;
            }
        }
    }

    /// Lifts the oldest queued task out of the way of
    /// everything overtaking it
    ///
    /// One task is moved up a band, and the whole queue is
    /// marked as starving so that pops serve the oldest band
    /// first until it isn't. Moving one task per tick would
    /// take a hundred seconds to clear a backlog of ten
    /// thousand, which is not a guarantee worth having; the
    /// flag drains them at full speed instead
    fn age(&'static self) {
        let now = executor::sequence();

        let Some((band, id)) = self.injector.oldest() else {
            self.injector.set_starving(false);
            return;
        };

        let Some(data) = executor::slot(id) else {
            return;
        };

        let starving = data.age(now) > STARVE_AGE;
        self.injector.set_starving(starving);

        if starving {
            self.injector.promote(band);
        }
    }

    /// Empties every dead worker back into the shared queue
    ///
    /// Used when the pool is being written off, so that no
    /// task is left sitting in a ring nothing is ever going to
    /// read. A task still in a ring when its slot was freed
    /// would be queued again by the next worker to sweep, long
    /// after its id belonged to somebody else
    pub(crate) fn abandon(&'static self) {
        self.sweep_all();
    }

    /// Shuts the pool for good
    ///
    /// Nothing restarts it after this, because everything it
    /// was holding has been failed and its slots handed back
    pub(crate) fn stop_permanently(&'static self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// Clears up after every worker that went down
    fn sweep_all(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            if self.workers[index].state().needs_recovery() {
                self.recover(index);
            }
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        for index in 0..sleeps {
            if self.sleeps[index].state().needs_recovery() {
                self.recover_sleep(index);
            }
        }
    }

    /// Adds a worker if the pool isn't getting through what
    /// it has
    ///
    /// ## Behaviour
    /// Being busy is not on its own a reason to grow, since a
    /// pool getting through thousands of tasks a second looks
    /// exactly as busy as one stuck in a single long sleep.
    /// What separates them is whether anything finished, so
    /// that is what is measured
    ///
    /// #### Note
    /// Measured per worker and not across the pool. `Runtime`
    /// is process wide, so there is nearly always something
    /// else spawning into it, and a total that keeps climbing
    /// would let one worker getting through short tasks hide
    /// every other worker being pinned by a long one
    fn grow(&'static self) {
        if self.live() >= cap() {
            return;
        }

        let highest = self.highest.load(Ordering::Acquire);

        // Counted across the rings as well as the shared queue.
        // Work sitting behind a stuck worker is the least
        // reachable work there is, since that worker can't get
        // to it either, and a new one could steal it
        let mut pending = self.injector.len();
        let mut stuck = false;
        let mut idle = false;

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.state().alive() {
                continue;
            }

            // Asked of every worker rather than short circuited,
            // because this is what records that the manager has
            // looked. Skipping one leaves it looking stuck on
            // the tick after
            let moved = worker.moved();
            let backlog = worker.backlog();

            pending += backlog;

            if !worker.busy() && backlog == 0 {
                idle = true;
            }

            // Running, and finished nothing in a whole tick.
            // Whatever it is on has it, and the queue behind it
            // is getting nothing from it
            if worker.busy() && !moved {
                stuck = true;
            }
        }

        // Nothing waiting anywhere, or somebody free to take it,
        // or the pool getting through what it has. None of the
        // three is a reason to add a thread
        if pending == 0 || idle || !stuck {
            return;
        }

        self.start_one();
    }

    /// Stops workers that have had nothing to do for a while
    ///
    /// Never below the floor, so a burst arriving after a
    /// quiet spell doesn't have to wait for threads to be
    /// created before any of it runs
    fn reap(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);
        let ticks = idle_ticks();

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.state().alive() {
                continue;
            }

            let quiet = worker.state() == WorkerState::Parked && worker.backlog() == 0;

            if !quiet {
                worker.busied();
                continue;
            }

            if worker.idled() < ticks || self.live() <= floor() {
                continue;
            }

            worker.stop();
        }
    }

    /// Wakes one parked worker
    fn wake_one(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        if highest == 0 {
            return;
        }

        let start = self.wake.fetch_add(1, Ordering::Relaxed);

        for offset in 0..highest {
            let worker = &self.workers[(start + offset) % highest];

            if worker.state() == WorkerState::Parked {
                worker.wake();
                return;
            }
        }
    }

    /// Notes that a worker has given its slot back
    #[inline(always)]
    pub(crate) fn left(&self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
    }

    /// Notes that a worker has gone to sleep
    #[inline(always)]
    pub(crate) fn parked_in(&self) {
        self.parked.fetch_add(1, Ordering::SeqCst);
    }

    /// Notes that a worker has woken back up
    #[inline(always)]
    pub(crate) fn parked_out(&self) {
        self.parked.fetch_sub(1, Ordering::SeqCst);
    }

    /// What the pool looks like right now
    pub(crate) fn stats(&'static self) -> PoolStats {
        let highest = self.highest.load(Ordering::Acquire);
        let mut workers = Vec::new();

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.state().alive() {
                continue;
            }

            workers.push(WorkerStats {
                busy: worker.busy(),
                backlog: worker.backlog(),
                completed: worker.completed(),
            });
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);
        let mut sleep_threads = 0;
        let mut sleep_busy = 0;

        for index in 0..sleeps {
            let sleep = &self.sleeps[index];

            if !sleep.running() {
                continue;
            }

            sleep_threads += 1;
            sleep_busy += sleep.busy() as usize;
        }

        PoolStats {
            queued: self.injector.len(),
            blocking_queued: self.blocking.len(),
            workers,
            sleep_threads,
            sleep_busy,
            slots: executor::slots(),
        }
    }
}

/// Tasks a worker tops its ring up to when it visits the
/// shared queue
///
/// A fraction of the ring rather than all of it, so a worker
/// leaves work where other workers can still reach it instead
/// of hoarding a full ring nobody else can see
const LOCAL_REFILL: usize = crate::constants::LOCAL_QUEUE / 4;

/// Raises a walk limit to cover a newly claimed slot
fn raise(bound: &AtomicUsize, limit: usize) {
    let mut highest = bound.load(Ordering::Acquire);

    while limit > highest {
        match bound.compare_exchange_weak(highest, limit, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return,
            Err(current) => highest = current,
        }
    }
}

/// Manager ticks that add up to the idle window
#[inline(always)]
fn idle_ticks() -> u32 {
    (IDLE_REAP.as_nanos() / MANAGER_TICK.as_nanos().max(1)) as u32
}

/// Sleep threads the pool aims to stay under
///
/// #### Note
/// A target rather than a bound, and deliberately so. The count
/// is read and then acted on, so several threads offloading at
/// once can all see room and all take it, and the pool settles
/// a few over. Those few are threads that spend their lives
/// parked in a `kevent` call, and having one spare to hand a
/// sleep to beats making the sleep wait for one, so the
/// overshoot is worth more than the exactness would be
#[inline(always)]
fn sleep_cap() -> usize {
    (cores() * SLEEP_MULTIPLIER).min(MAX_WORKERS)
}

/// Workers the pool never goes below
#[inline(always)]
pub(crate) fn floor() -> usize {
    cores()
}

/// Workers the pool aims to stay under
///
/// A target rather than a bound, for the same reason
/// `sleep_cap` is: the count is read and then acted on, so a
/// burst of growth can settle a little over it
#[inline(always)]
pub(crate) fn cap() -> usize {
    (cores() * WORKER_MULTIPLIER).min(MAX_WORKERS)
}

/// Online cores, asking the kernel on first use
fn cores() -> usize {
    let cached = CORES.load(Ordering::Relaxed);

    if cached != 0 {
        return cached;
    }

    let count = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    let count = if count < 1 { 1 } else { count as usize };

    CORES.store(count, Ordering::Relaxed);

    count
}
