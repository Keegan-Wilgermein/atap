//! # Worker Pool
//! Every worker in the process, the queues they pull from, and
//! the policy the manager applies to them
//!
//! A worker finds its own work whether the manager is running
//! or not. The manager only grows, shrinks and cleans up

use crate::{
    constants::{
        IDLE_REAP, MANAGER_TICK, MAX_WORKERS, NO_TASK, SLEEP_MULTIPLIER, STARVE_AGE,
        TRIM_INTERVAL, WORKER_MULTIPLIER,
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
    /// Fixed and static, since workers park on their state word
    /// and their rings outlive their threads
    workers: [Worker; MAX_WORKERS],

    /// The queue every task lands in
    injector: Injector,

    /// The threads that exist to be blocked, shared by every worker
    sleeps: [SleepThread; MAX_WORKERS],

    /// Blocking tasks waiting for a sleep thread
    blocking: Injector,

    /// Slots currently claimed
    live: AtomicUsize,

    /// Sleep thread slots currently claimed
    sleeps_live: AtomicUsize,

    /// One past the highest sleep thread slot ever claimed
    sleeps_highest: AtomicUsize,

    /// Sleep threads asleep on their own state word
    sleeps_parked: AtomicU32,

    /// One past the highest slot ever claimed, which bounds every
    /// walk of the pool
    highest: AtomicUsize,

    /// Workers asleep on their own state word
    parked: AtomicU32,

    /// Whether the pool has been shut for good
    ///
    /// Nothing restarts it once set, since everything it held has
    /// been failed and given back
    stopped: AtomicBool,

    /// Manager ticks since the table was last trimmed
    trim_ticks: AtomicU32,

    /// Where the next search for somebody to wake starts, rotated
    /// to spread wakes out
    wake: AtomicUsize,

    /// Where the next peer sweep or steal starts
    sweep: AtomicUsize,
}

impl WorkerPool {
    /// An empty pool
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
            trim_ticks: AtomicU32::new(0),
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
    /// Wakes a parked thread if it can claim one, and starts a new
    /// one otherwise
    pub(crate) fn offload(&'static self, id: usize) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }

        // Checked first, so a pool that can't take the task says so
        // while it is still the caller's to fail
        if self.sleeps_live() == 0 && !self.start_sleep() {
            return false;
        }

        if !self.blocking.push(id) {
            return false;
        }

        if self.sleeps_parked.load(Ordering::SeqCst) != 0 && self.wake_sleep() {
            return true;
        }

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

    /// Notes that a sleep thread has given its slot back,
    /// saturating at zero
    #[inline(always)]
    pub(crate) fn sleep_left(&self) {
        let _ = self
            .sleeps_live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                Some(live.saturating_sub(1))
            });
    }

    /// Starts one sleep thread, if there is room for one
    ///
    /// The count goes up before the spawn, so a thread that exits
    /// at once can't take it below zero
    fn start_sleep(&'static self) -> bool {
        if self.stopped.load(Ordering::Acquire) || self.sleeps_live() >= sleep_cap() {
            return false;
        }

        for index in 0..MAX_WORKERS {
            let sleep = &self.sleeps[index];

            if !sleep.claim() {
                continue;
            }

            self.sleeps_live.fetch_add(1, Ordering::AcqRel);

            if !sleep.start() {
                self.sleep_left();

                // A refused spawn may succeed on the next slot
                continue;
            }

            raise(&self.sleeps_highest, index + 1);

            return true;
        }

        false
    }

    /// Wakes one parked sleep thread
    ///
    /// ## Returns
    /// Whether one was actually taken out of a park. A lost
    /// exchange moves on to the next slot
    fn wake_sleep(&'static self) -> bool {
        let highest = self.sleeps_highest.load(Ordering::Acquire);

        if highest == 0 {
            return false;
        }

        let start = self.wake.fetch_add(1, Ordering::Relaxed);

        for offset in 0..highest {
            let sleep = &self.sleeps[(start + offset) % highest];

            if sleep.state() == WorkerState::Parked && sleep.wake() {
                return true;
            }
        }

        false
    }

    /// Queues a task and makes sure somebody will come for it
    ///
    /// The task is queued before the parked count is read, and a
    /// worker announces it is parking before its last look at the
    /// queue, so one of them always sees the other
    pub(crate) fn submit(&'static self, id: usize) -> bool {
        // Checked first, so a pool that can't take the task says so
        // while it is still the caller's to fail. One with no workers
        // left is restarted by whoever spawns into it
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }

        if self.live() == 0 {
            self.ensure_floor();

            if self.live() == 0 {
                return false;
            }
        }

        // Refused only when the id has no live task behind it
        if !self.injector.push(id) {
            return false;
        }

        // Only a wake is needed. An awake worker finds the task on its
        // own
        if self.parked.load(Ordering::SeqCst) != 0 {
            self.wake_one();
        }

        true
    }

    /// Finds something for a worker to do: its own ring, then the
    /// shared queue, then a peer's ring
    pub(crate) fn find_work(&'static self, worker: &'static Worker) -> Option<usize> {
        if let Some(id) = worker.pop() {
            return Some(id);
        }

        if let Some((id, band)) = self.injector.pop_banded() {
            // Tops the ring up with a share of what is queued, leaving the
            // rest for other workers
            let share = (self.injector.len() / self.live().max(1)).min(LOCAL_REFILL);

            while worker.backlog() < share {
                // Only from the band just served, so a higher band arriving
                // mid refill isn't buried in the ring
                let Some(extra) = self.injector.pop_from(band) else {
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

        let start = self.sweep.fetch_add(1, Ordering::Relaxed);

        for offset in 0..highest {
            let index = (start + offset) % highest;
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

    /// Checks one slot for a thread that died without saying so
    ///
    /// Workers call this on their way to a park, so the pool
    /// recovers even with no manager
    pub(crate) fn sweep_one(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        if highest == 0 {
            return;
        }

        // Independent indexes, since there can be more sleep threads
        // than workers
        let cursor = self.sweep.fetch_add(1, Ordering::Relaxed);

        let index = cursor % highest;

        if self.workers[index].state().needs_recovery() {
            self.recover(index);
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        if sleeps != 0 {
            let sleep = cursor % sleeps;

            if self.sleeps[sleep].state().needs_recovery() {
                self.recover_sleep(sleep);
            }
        }
    }

    /// Picks up after a worker that went down
    ///
    /// Its queued tasks go back to the shared queue. The one it was
    /// running can't be run again, so it is failed
    pub(crate) fn recover(&'static self, index: usize) {
        let worker = &self.workers[index];

        if !worker.claim_recovery() {
            return;
        }

        let (queued, stranded) = worker.recover();

        for id in queued {
            // Refused only when the task's slot has already gone
            if !self.injector.push(id) {
                executor::fail(id);
            }
        }

        if stranded != NO_TASK {
            executor::fail(stranded);
        }

        worker.release();
        self.left();
    }

    /// Picks up after a sleep thread that went down, failing the
    /// task it was running
    pub(crate) fn recover_sleep(&'static self, index: usize) {
        let sleep = &self.sleeps[index];

        if !sleep.claim_recovery() {
            return;
        }

        let stranded = sleep.recover();

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
    ///
    /// The count goes up before the spawn, the same as
    /// `start_sleep`
    pub(crate) fn start_one(&'static self) -> bool {
        if self.live() >= cap() {
            return false;
        }

        for index in 0..MAX_WORKERS {
            let worker = &self.workers[index];

            if !worker.claim() {
                continue;
            }

            self.live.fetch_add(1, Ordering::AcqRel);

            if !worker.start() {
                self.left();
                continue;
            }

            raise(&self.highest, index + 1);

            return true;
        }

        false
    }

    /// One pass of the manager's policy
    pub(crate) fn tick(&'static self) {
        self.injector.refill();
        self.age();
        self.sweep_all();
        self.ensure_floor();
        self.balance();
        self.balance_blocking();
        self.grow();
        self.reap();
        self.reap_sleeps();
        self.trim();
    }

    /// Makes sure the blocking queue has threads coming for it
    ///
    /// Wakes parked sleep threads first, then starts new ones up to
    /// the cap, so anything `offload` missed is put right within a
    /// tick
    fn balance_blocking(&'static self) {
        let mut pending = self.blocking.len();

        while pending > 0 && self.sleeps_parked.load(Ordering::SeqCst) != 0 && self.wake_sleep() {
            pending -= 1;
        }

        while pending > 0 && self.start_sleep() {
            pending -= 1;
        }
    }

    /// Hands the unused top of the task table back now and then
    ///
    /// A refusal is the normal answer, and is ignored
    fn trim(&'static self) {
        let due = self.trim_ticks.fetch_add(1, Ordering::Relaxed) + 1;

        if due < TRIM_INTERVAL {
            return;
        }

        // Waits for the pool to go quiet, since a trim briefly takes
        // the free list away from spawns. The counter is kept, so it
        // runs the moment things go quiet
        if !self.idle() {
            return;
        }

        self.trim_ticks.store(0, Ordering::Relaxed);

        let _ = executor::trim();
    }

    /// Whether the pool has nothing whatever to do
    fn idle(&'static self) -> bool {
        if !self.injector.is_empty() || !self.blocking.is_empty() {
            return false;
        }

        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.state().alive() {
                continue;
            }

            if worker.busy() || worker.backlog() > 0 {
                return false;
            }
        }

        true
    }

    /// Stops sleep threads that have had nothing to do
    ///
    /// No floor, so a process with no blocking work holds no sleep
    /// threads
    fn reap_sleeps(&'static self) {
        // Nothing is idle while blocking work is waiting
        if !self.blocking.is_empty() {
            return;
        }

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
    /// Only as many as there is work for
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

            // Everything past the task it is about to run could go to
            // somebody else
            spare += worker.backlog().saturating_sub(1);
        }

        for index in 0..highest {
            if spare == 0 {
                return;
            }

            let worker = &self.workers[index];

            // Counted only when the wake was actually claimed
            if worker.state() == WorkerState::Parked && worker.wake() {
                spare -= 1;
            }
        }
    }

    /// Lifts the oldest queued task out of the way of everything
    /// overtaking it
    ///
    /// Moves it up a band, and grants a budget of oldest first pops
    /// while the queue is starving
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

    /// Empties every dead worker back into the shared queue, for
    /// when the pool is being written off
    pub(crate) fn abandon(&'static self) {
        self.sweep_all();
    }

    /// Shuts the pool for good
    pub(crate) fn stop_permanently(&'static self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// Asks every thread in the pool to stop between tasks
    ///
    /// Only useful after `stop_permanently`, or the next tick
    /// starts them straight back up
    pub(crate) fn stop_all(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            self.workers[index].stop();
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        for index in 0..sleeps {
            self.sleeps[index].stop();
        }
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

    /// Adds a worker if the pool isn't getting through what it has
    ///
    /// Grows only when a worker finished nothing in a whole tick,
    /// while work waits and nobody is free to take it
    fn grow(&'static self) {
        if self.live() >= cap() {
            return;
        }

        let highest = self.highest.load(Ordering::Acquire);

        // Includes work sitting in the rings, which a new worker could
        // steal
        let mut pending = self.injector.len();
        let mut stuck = false;
        let mut idle = false;

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.state().alive() {
                continue;
            }

            // Asked of every worker, since this also records that the
            // manager looked
            let moved = worker.moved();
            let backlog = worker.backlog();

            pending += backlog;

            // Free to take work, which a stopping worker isn't
            let available = matches!(
                worker.state(),
                WorkerState::Starting | WorkerState::Idle | WorkerState::Parked
            );

            if available && backlog == 0 {
                idle = true;
            }

            // Running, and finished nothing in a whole tick
            if worker.busy() && !moved {
                stuck = true;
            }
        }

        if pending == 0 || idle || !stuck {
            return;
        }

        self.start_one();
    }

    /// Stops workers that have had nothing to do for a while, never
    /// below the floor
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
    ///
    /// ## Returns
    /// Whether one was actually taken out of a park
    fn wake_one(&'static self) -> bool {
        let highest = self.highest.load(Ordering::Acquire);

        if highest == 0 {
            return false;
        }

        let start = self.wake.fetch_add(1, Ordering::Relaxed);

        for offset in 0..highest {
            let worker = &self.workers[(start + offset) % highest];

            if worker.state() == WorkerState::Parked && worker.wake() {
                return true;
            }
        }

        false
    }

    /// Notes that a worker has given its slot back
    ///
    /// Saturates at zero, since a wrapped count would stop the pool
    /// ever starting a thread again
    #[inline(always)]
    pub(crate) fn left(&self) {
        let _ = self
            .live
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                Some(live.saturating_sub(1))
            });
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

            workers.push(WorkerStats::new(
                worker.busy(),
                worker.backlog(),
                worker.completed(),
            ));
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

        PoolStats::new(
            executor::live(),
            self.injector.len(),
            self.blocking.len(),
            workers,
            sleep_threads,
            sleep_busy,
            executor::slots(),
            executor::peak_slots(),
        )
    }
}

/// Tasks a worker tops its ring up to when it visits the
/// shared queue
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
/// A target, not a bound: concurrent offloads can settle a few
/// over
#[inline(always)]
fn sleep_cap() -> usize {
    (cores() * SLEEP_MULTIPLIER).min(MAX_WORKERS)
}

/// Workers the pool never goes below
#[inline(always)]
pub(crate) fn floor() -> usize {
    cores()
}

/// Workers the pool aims to stay under, a target in the same
/// way as `sleep_cap`
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
