//! # Worker Pool
//! Every worker in the process, the queues they pull from, and
//! the policy the manager applies to them
//!
//! A worker finds its own work whether the manager is running
//! or not. The manager only grows, shrinks and cleans up

use crate::{
    constants::{
        IDLE_REAP, IDLE_REAP_OVER, LIFO_STREAK, MANAGER_TICK, MAX_WORKERS, NO_TASK, OVERLOAD_RATIO,
        RESTART_LIMIT, RESTART_WINDOW, SLEEP_MULTIPLIER, STARVE_AGE, THREAD_RESERVE, TRIM_INTERVAL,
        WORKER_MULTIPLIER,
    },
    executor,
    modules::{
        faults,
        injector::Injector,
        pool_stats::PoolStats,
        sleep_thread::SleepThread,
        task_data::{QUEUED_LOCAL, deadline_epoch},
        thread_slot::PoolThread,
        worker::Worker,
        worker_state::WorkerState,
        worker_stats::WorkerStats,
    },
};
use std::{
    mem, ptr,
    sync::atomic::{AtomicBool, AtomicIsize, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    thread,
};

/// Every worker in the process
pub(crate) static POOL: WorkerPool = WorkerPool::new();

/// The online core count, or 0 before it has been asked for
static CORES: AtomicUsize = AtomicUsize::new(0);

/// Why a worker is being started, which decides how far past the
/// target it may go
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Start {
    /// Bringing the pool up to its floor
    Floor,

    /// Growing for waiting work, bounded only by the ceiling
    Grow,

    /// Replacing a worker that is blocked or gone, bounded only by
    /// the ceiling
    Replace,
}

/// The pool of workers and the work waiting for them
pub(crate) struct WorkerPool {
    /// Every worker slot, used or not
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

    /// Whether the pool is shut, from a shutdown until the next
    /// `init`
    stopped: AtomicBool,

    /// Manager ticks since the table was last trimmed
    trim_ticks: AtomicU32,

    /// Where the next search for somebody to wake starts, rotated
    /// to spread wakes out
    wake: AtomicUsize,

    /// Where the next peer sweep or steal starts
    sweep: AtomicUsize,

    /// The most workers ever running at once
    peak_workers: AtomicUsize,

    /// The most sleep threads ever running at once
    peak_sleeps: AtomicUsize,

    /// Manager ticks in a row the workers have been overloaded at or
    /// past the target
    overloaded: AtomicU32,

    /// Manager ticks in a row the sleep threads have been overloaded
    /// at or past their target
    sleeps_overloaded: AtomicU32,

    /// Whether the oldest queued task had waited too long, as of the
    /// last tick
    starving: AtomicBool,

    /// Threads that died and haven't been recovered yet
    dead: AtomicUsize,

    /// Every thread death since the process started
    deaths: AtomicUsize,

    /// When the current run of deaths started, as nanoseconds past the
    /// deadline epoch plus one, or zero for none
    storm_started: AtomicU64,

    /// Deaths since the current run of them started
    storm_deaths: AtomicU32,

    /// Workers waiting inside a task with no depth left to help at
    blocked: AtomicUsize,

    /// LIFO slots holding a task, so a worker about to park sees work
    /// only a steal can reach
    ///
    /// Signed, since a take can land between a put's swap and its count
    lifo_filled: AtomicIsize,
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
            peak_workers: AtomicUsize::new(0),
            peak_sleeps: AtomicUsize::new(0),
            overloaded: AtomicU32::new(0),
            sleeps_overloaded: AtomicU32::new(0),
            starving: AtomicBool::new(false),
            dead: AtomicUsize::new(0),
            deaths: AtomicUsize::new(0),
            storm_started: AtomicU64::new(0),
            storm_deaths: AtomicU32::new(0),
            blocked: AtomicUsize::new(0),
            lifo_filled: AtomicIsize::new(0),
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

        // A dead thread still counts as running until it is swept, and
        // would stop a new one being started
        if self.dead.load(Ordering::SeqCst) != 0 {
            self.sweep_all();
        }

        // Checked first, so a pool that can't take the task says so
        // while it is still the caller's to fail
        if self.sleeps_live() == 0 && !self.start_sleep(sleep_target()) {
            return false;
        }

        if !self.blocking.push(id) {
            return false;
        }

        if self.sleeps_parked.load(Ordering::SeqCst) != 0 && self.wake_sleep() {
            return true;
        }

        self.start_sleep(sleep_target());

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

    /// Starts one sleep thread, if `limit` leaves room for one
    ///
    /// The count goes up before the spawn, so a thread that exits
    /// at once can't take it below zero. Never past the ceiling,
    /// whatever `limit` says
    fn start_sleep(&'static self, limit: usize) -> bool {
        let limit = limit.min(ceiling());

        if self.stopped.load(Ordering::Acquire) || self.sleeps_live() >= limit {
            return false;
        }

        let mut index = 0;

        // A slot given back below the highest one used, or the next one up.
        // Read again each pass, so a slot a racing start has just claimed
        // moves the reach on rather than ending the search
        while index < (self.sleeps_highest.load(Ordering::Acquire) + 1).min(MAX_WORKERS) {
            let sleep = &self.sleeps[index];

            index += 1;

            // Counted before the claim, so nothing racing this start reads
            // an empty pool while a thread is on its way. Reserved against
            // the limit, so racing starts can't carry it past
            if self
                .sleeps_live
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                    (live < limit).then_some(live + 1)
                })
                .is_err()
            {
                return false;
            }

            if !sleep.slot().claim() {
                self.sleep_left();
                continue;
            }

            raise(&self.sleeps_highest, index);

            if !sleep.start() {
                self.sleep_left();

                // A refused spawn may succeed on the next slot
                continue;
            }

            raise(&self.peak_sleeps, self.sleeps_live());

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

            if sleep.slot().state() == WorkerState::Parked && sleep.slot().wake() {
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

        // A dead thread still counts as live until it is swept, which
        // would stop the floor being brought back
        if self.dead.load(Ordering::SeqCst) != 0 {
            self.sweep_all();
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

    /// Queues a task spawned from inside a task running on `worker`:
    /// that worker's LIFO slot if it is empty, its ring if not, and the
    /// shared queue if both are full
    ///
    /// A peer that is parked is woken to steal the rest
    pub(crate) fn submit_local(&'static self, worker: &'static Worker, id: usize) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }

        let Some(data) = executor::slot(id) else {
            return false;
        };

        // Marked before it can be found, so whoever finds it can claim it
        data.mark_queued(QUEUED_LOCAL);

        if worker.put_lifo(id) {
            self.lifo_filled.fetch_add(1, Ordering::SeqCst);
        } else if !worker.push(id) {
            // Taken back first, since the shared queue links what it holds and
            // must never be handed a task somebody else has already run
            if !data.claim_queued(QUEUED_LOCAL) {
                return true;
            }

            return self.submit(id);
        }

        if self.parked.load(Ordering::SeqCst) != 0 {
            self.wake_one();
        }

        true
    }

    /// Takes the task in a worker's LIFO slot, counting the slot out
    ///
    /// ## Returns
    /// The task, if the slot held one nobody had already run
    #[inline(always)]
    pub(crate) fn take_lifo(&self, worker: &Worker) -> Option<usize> {
        let (id, claimed) = worker.take_lifo()?;

        self.lifo_filled.fetch_sub(1, Ordering::SeqCst);

        claimed.then_some(id)
    }

    /// Whether any worker's LIFO slot holds a task
    #[inline(always)]
    pub(crate) fn lifo_waiting(&self) -> bool {
        self.lifo_filled.load(Ordering::SeqCst) > 0
    }

    /// Finds something for a worker to do while a task it is running
    /// waits: its LIFO slot first, which is most likely the task being
    /// waited on, then, if `unrelated` allows, wherever it would look
    /// anyway
    pub(crate) fn find_help(
        &'static self,
        worker: &'static Worker,
        unrelated: bool,
    ) -> Option<usize> {
        if let Some(id) = self.take_lifo(worker) {
            return Some(id);
        }

        match unrelated {
            true => self.find_work(worker),
            false => None,
        }
    }

    /// Finds something for a worker to do: its LIFO slot, its own ring,
    /// the shared queue, then a peer's LIFO slot or ring
    ///
    /// The LIFO slot goes first only a few times in a row, then the
    /// rest get a turn before it
    pub(crate) fn find_work(&'static self, worker: &'static Worker) -> Option<usize> {
        if worker.streak() < LIFO_STREAK {
            if let Some(id) = self.take_lifo(worker) {
                worker.took(true);
                return Some(id);
            }
        }

        worker.took(false);

        if let Some(id) = worker.pop() {
            return Some(id);
        }

        if let Some((id, band)) = self.injector.pop_banded() {
            // Tops the ring up with a share of what is queued, leaving the
            // rest for other workers
            let share = (self.injector.len() / self.live().max(1)).min(LOCAL_REFILL);

            // Only while the ring has room. Nothing but this worker adds to it,
            // so every task taken out of the shared queue here goes in
            while worker.backlog() < share && worker.has_room() {
                // Only from the band just served, so a higher band arriving
                // mid refill isn't buried in the ring
                let Some(extra) = self.injector.pop_from(band) else {
                    break;
                };

                let Some(data) = executor::slot(extra) else {
                    continue;
                };

                // Local from here, so it can be claimed wherever it is found
                data.mark_queued(QUEUED_LOCAL);

                if worker.push(extra) {
                    continue;
                }

                // Can't happen while only this worker adds to the ring. Taken
                // back before it goes to the shared queue, which must never be
                // handed a task somebody else has already run
                if data.claim_queued(QUEUED_LOCAL) && !self.injector.push(extra) {
                    executor::fail(extra);
                }

                break;
            }

            return Some(id);
        }

        // Passed over above for the queue's turn, with nothing else there
        if let Some(id) = self.take_lifo(worker) {
            worker.took(true);
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

            if std::ptr::eq(victim, thief) || !victim.slot().state().alive() {
                continue;
            }

            if victim.steal_into(thief) != 0 {
                if let Some(id) = thief.pop() {
                    return Some(id);
                }
            }

            // A peer's newest spawn, which it may be too busy to reach
            if let Some(id) = self.take_lifo(victim) {
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

        if self.workers[index].slot().state().needs_recovery() {
            self.recover(index);
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        if sleeps != 0 {
            let sleep = cursor % sleeps;

            if self.sleeps[sleep].slot().state().needs_recovery() {
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

        if !worker.slot().claim_recovery() {
            return;
        }

        self.recovered();

        // Queued like the ring, since it never started
        if let Some(id) = self.take_lifo(worker) {
            if !self.injector.push(id) {
                executor::fail(id);
            }
        }

        let (queued, stranded) = worker.recover();

        for id in queued {
            // Refused only when the task's slot has already gone
            if !self.injector.push(id) {
                executor::fail(id);
            }
        }

        for id in stranded {
            executor::fail(id);
        }

        worker.release();
        self.left();
    }

    /// Picks up after a sleep thread that went down, failing the
    /// task it was running
    pub(crate) fn recover_sleep(&'static self, index: usize) {
        let sleep = &self.sleeps[index];

        if !sleep.slot().claim_recovery() {
            return;
        }

        self.recovered();

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
            if !self.start_one(Start::Floor) {
                return;
            }
        }
    }

    /// Starts one worker, if `start` leaves room for one
    ///
    /// The count goes up before the spawn, the same as
    /// `start_sleep`
    pub(crate) fn start_one(&'static self, start: Start) -> bool {
        let limit = match start {
            Start::Floor => target(),
            Start::Grow | Start::Replace => ceiling(),
        };

        if self.stopped.load(Ordering::Acquire) || self.live() >= limit {
            return false;
        }

        let mut index = 0;

        // A slot given back below the highest one used, or the next one up.
        // Read again each pass, so a slot a racing start has just claimed
        // moves the reach on rather than ending the search
        while index < (self.highest.load(Ordering::Acquire) + 1).min(MAX_WORKERS) {
            let worker = &self.workers[index];

            index += 1;

            // Counted before the claim, so a spawn racing this start never
            // reads an empty pool while a worker is on its way. Reserved
            // against the limit, so racing starts can't carry it past
            if self
                .live
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |live| {
                    (live < limit).then_some(live + 1)
                })
                .is_err()
            {
                return false;
            }

            if !worker.slot().claim() {
                self.left();
                continue;
            }

            raise(&self.highest, index);

            if !worker.start() {
                self.left();
                continue;
            }

            raise(&self.peak_workers, self.live());

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
    /// the target. Past the target only when every sleep thread is
    /// stuck or busy with work still waiting
    fn balance_blocking(&'static self) {
        let mut pending = self.blocking.len();

        while pending > 0 && self.sleeps_parked.load(Ordering::SeqCst) != 0 && self.wake_sleep() {
            pending -= 1;
        }

        while pending > 0 && self.start_sleep(sleep_target()) {
            pending -= 1;
        }

        let live = self.sleeps_live();

        if pending == 0 || live < sleep_target() {
            self.sleeps_overloaded.store(0, Ordering::Relaxed);
            return;
        }

        // Every sleep thread held in one task for a whole tick, with more
        // waiting behind them. As many are started as are waiting, at most
        // doubling the pool
        if self.sleeps_all_stuck() {
            if earned(&self.sleeps_overloaded, true, 0) {
                for _ in 0..pending.min(live.max(1)) {
                    if !self.start_sleep(ceiling()) {
                        break;
                    }
                }
            }

            return;
        }

        let overloaded = pending >= live.max(1) * OVERLOAD_RATIO && self.sleeps_all_busy();

        if earned(
            &self.sleeps_overloaded,
            overloaded,
            live.saturating_sub(sleep_target()),
        ) {
            self.start_sleep(ceiling());
        }
    }

    /// Whether every running sleep thread is inside a task it hasn't
    /// finished since the last look
    fn sleeps_all_stuck(&'static self) -> bool {
        let highest = self.sleeps_highest.load(Ordering::Acquire);
        let mut all = true;

        for index in 0..highest {
            let sleep = &self.sleeps[index];

            if !sleep.slot().alive() {
                continue;
            }

            // Asked of every one, since this also records the look
            let moved = sleep.moved();

            all &= sleep.slot().busy() && !moved;
        }

        all
    }

    /// Whether every running sleep thread is inside a task
    fn sleeps_all_busy(&'static self) -> bool {
        let highest = self.sleeps_highest.load(Ordering::Acquire);

        (0..highest)
            .map(|index| self.sleeps[index].slot())
            .filter(|slot| slot.alive())
            .all(|slot| slot.busy())
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
        // the free list away from spawns
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

            if !worker.slot().state().alive() {
                continue;
            }

            if worker.slot().busy() || worker.backlog() > 0 {
                return false;
            }
        }

        true
    }

    /// Stops sleep threads that have had nothing to do
    ///
    /// No floor, so a process with no blocking work holds no sleep
    /// threads. Past the target they go sooner
    fn reap_sleeps(&'static self) {
        // Nothing is idle while blocking work is waiting
        if !self.blocking.is_empty() {
            return;
        }

        let highest = self.sleeps_highest.load(Ordering::Acquire);

        for index in 0..highest {
            let sleep = &self.sleeps[index];

            if !sleep.slot().state().alive() {
                continue;
            }

            if sleep.slot().state() != WorkerState::Parked {
                sleep.slot().busied();
                continue;
            }

            let ticks = match self.sleeps_live() > sleep_target() {
                true => over_ticks(),
                false => idle_ticks(),
            };

            if sleep.slot().idled() >= ticks {
                sleep.slot().stop();
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

            if !worker.slot().state().alive() {
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
            if worker.slot().state() == WorkerState::Parked && worker.slot().wake() {
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
            self.starving.store(false, Ordering::Relaxed);
            return;
        };

        let Some(data) = executor::slot(id) else {
            return;
        };

        let starving = data.age(now) > STARVE_AGE;
        self.injector.set_starving(starving);
        self.starving.store(starving, Ordering::Relaxed);

        if starving {
            self.injector.promote(band);
        }
    }

    /// Empties every dead worker back into the shared queue, for
    /// when the pool is being written off
    pub(crate) fn abandon(&'static self) {
        self.sweep_all();
    }

    /// Shuts the pool, so nothing new is queued or started
    pub(crate) fn close(&'static self) {
        self.stopped.store(true, Ordering::Release);
    }

    /// Opens the pool again, for a runtime that is starting
    pub(crate) fn open(&'static self) {
        self.stopped.store(false, Ordering::Release);
    }

    /// Asks every thread in the pool to stop between tasks
    ///
    /// Only useful after `close`, or the next tick
    /// starts them straight back up
    pub(crate) fn stop_all(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            self.workers[index].slot().stop();
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        for index in 0..sleeps {
            self.sleeps[index].slot().stop();
        }
    }

    /// Clears up after every worker that went down
    pub(crate) fn sweep_all(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            if self.workers[index].slot().state().needs_recovery() {
                self.recover(index);
            }
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        for index in 0..sleeps {
            if self.sleeps[index].slot().state().needs_recovery() {
                self.recover_sleep(index);
            }
        }
    }

    /// Adds a worker if the pool isn't getting through what it has
    ///
    /// Workers stuck waiting in the kernel don't count against the
    /// target. Below the target, grows when work waits and nobody is
    /// free to take it. At or past it, only when every worker is stuck,
    /// and a tick later for each worker already past. Never past the
    /// ceiling
    fn grow(&'static self) {
        let live = self.live();

        if live >= ceiling() {
            return;
        }

        let highest = self.highest.load(Ordering::Acquire);

        // Includes work sitting in the rings, which a new worker could
        // steal
        let mut pending = self.injector.len();
        let mut stuck = false;
        let mut all_stuck = true;
        let mut idle = false;
        let mut stranded = false;
        let mut stuck_workers = Vec::new();

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.slot().state().alive() {
                continue;
            }

            // Asked of every worker, since this also records that the
            // manager looked
            let moved = worker.moved();
            let backlog = worker.backlog();

            pending += backlog;

            // Free to take work, which a stopping worker isn't
            let available = matches!(
                worker.slot().state(),
                WorkerState::Starting | WorkerState::Idle | WorkerState::Parked
            );

            if available && backlog == 0 {
                idle = true;
            }

            // Running, and finished nothing in a whole tick
            match worker.slot().busy() && !moved {
                true => {
                    stuck = true;
                    stuck_workers.push(index);

                    // Only a steal can move these, and a stuck worker's
                    // peers may all be stuck too
                    stranded |= backlog > 0;
                }
                false => all_stuck = false,
            }
        }

        if pending == 0 || idle || !stuck {
            self.overloaded.store(0, Ordering::Relaxed);
            return;
        }

        // Workers blocked in the kernel don't count against the target
        let blocked = stuck_workers
            .iter()
            .filter(|index| self.workers[**index].blocked_in_a_call())
            .count();

        let running = live.saturating_sub(blocked);

        if blocked > 0 && running < target() {
            if earned(&self.overloaded, true, 0) {
                for _ in 0..(target() - running).min(pending) {
                    if !self.start_one(Start::Grow) {
                        break;
                    }
                }
            }

            return;
        }

        if live < target() {
            self.start_one(Start::Grow);
            return;
        }

        // Every worker stuck, and work waiting that has nowhere to go: a
        // queue deep enough, a queue that has waited too long, or tasks in
        // the rings of stuck workers
        let overloaded = all_stuck
            && (pending >= live.max(1) * OVERLOAD_RATIO
                || self.starving.load(Ordering::Relaxed)
                || stranded);

        if earned(&self.overloaded, overloaded, live - target()) {
            self.start_one(Start::Grow);
        }
    }

    /// Stops workers that have had nothing to do for a while, never
    /// below the floor
    ///
    /// Past the target they go sooner
    fn reap(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.slot().state().alive() {
                continue;
            }

            let quiet = worker.slot().state() == WorkerState::Parked && worker.backlog() == 0;

            if !quiet {
                worker.slot().busied();
                continue;
            }

            let ticks = match self.live() > target() {
                true => over_ticks(),
                false => idle_ticks(),
            };

            if worker.slot().idled() < ticks || self.live() <= floor() {
                continue;
            }

            worker.slot().stop();
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

            if worker.slot().state() == WorkerState::Parked && worker.slot().wake() {
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

    /// Threads that died and haven't been recovered yet
    #[inline(always)]
    pub(crate) fn dead(&self) -> usize {
        self.dead.load(Ordering::SeqCst)
    }

    /// Whether any task is waiting in the shared queue or a worker's
    /// ring
    pub(crate) fn has_queued_work(&'static self) -> bool {
        if !self.injector.is_empty() {
            return true;
        }

        let highest = self.highest.load(Ordering::Acquire);

        (0..highest).any(|index| self.workers[index].backlog() > 0)
    }

    /// Notes that a worker is waiting inside a task with no depth left
    /// to help at
    #[inline(always)]
    pub(crate) fn blocked_in(&self) {
        self.blocked.fetch_add(1, Ordering::SeqCst);
    }

    /// Notes that a blocked worker's wait is over
    #[inline(always)]
    pub(crate) fn blocked_out(&self) {
        self.blocked.fetch_sub(1, Ordering::SeqCst);
    }

    /// Whether every worker is waiting inside a task, so nothing queued
    /// can move until another is started
    #[inline(always)]
    pub(crate) fn all_blocked(&self) -> bool {
        self.blocked.load(Ordering::SeqCst) >= self.live()
    }

    /// Counts a thread going down, before its slot reads dead
    pub(crate) fn note_death(&self) {
        self.dead.fetch_add(1, Ordering::SeqCst);
        self.deaths.fetch_add(1, Ordering::Relaxed);

        let now = deadline_epoch().elapsed().as_nanos() as u64 + 1;
        let started = self.storm_started.load(Ordering::Relaxed);

        // Deaths inside one restart window are one run of them
        if started == 0 || now.saturating_sub(started) > RESTART_WINDOW.as_nanos() as u64 {
            self.storm_started.store(now, Ordering::Relaxed);
            self.storm_deaths.store(1, Ordering::Relaxed);
            return;
        }

        self.storm_deaths.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts a dead thread's slot as recovered
    fn recovered(&self) {
        let _ = self
            .dead
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |dead| {
                Some(dead.saturating_sub(1))
            });
    }

    /// Whether threads have been dying faster than a restart window
    /// allows
    ///
    /// Scaled to the pool, so the whole pool going at once isn't a
    /// storm
    fn in_a_storm(&self) -> bool {
        let started = self.storm_started.load(Ordering::Relaxed);

        if started == 0 {
            return false;
        }

        let now = deadline_epoch().elapsed().as_nanos() as u64 + 1;

        now.saturating_sub(started) <= RESTART_WINDOW.as_nanos() as u64
            && self.storm_deaths.load(Ordering::Relaxed) as usize
                > RESTART_LIMIT as usize * self.largest_pool()
    }

    /// The most threads, workers and sleep threads together, the pool
    /// has run at once, and never less than its floor
    fn largest_pool(&self) -> usize {
        let peak =
            self.peak_workers.load(Ordering::Relaxed) + self.peak_sleeps.load(Ordering::Relaxed);

        peak.max(floor()).max(1)
    }

    /// Starts a thread of its own to recover the pool, for a thread
    /// going down that can't count on anyone being left to notice
    ///
    /// A start that is refused is left to the other ways the pool
    /// recovers: the next spawn, the manager's tick, or a waiter
    pub(crate) fn send_for_help(&'static self) {
        if faults::spawn_refused() {
            return;
        }

        let _ = thread::Builder::new()
            .name(String::from("atap-recovery"))
            .spawn(move || self.heal());
    }

    /// Recovers the pool, if any thread has died since it last was
    pub(crate) fn heal_if_needed(&'static self) {
        if self.dead.load(Ordering::SeqCst) != 0 {
            self.heal();
        }
    }

    /// Clears up after dead threads and brings the pool back up
    ///
    /// ## Behaviour
    /// Needs no manager and no live thread. If nothing can be started
    /// and nothing is left to try again, everything still waiting is
    /// written off
    pub(crate) fn heal(&'static self) {
        self.sweep_all();

        if self.stopped.load(Ordering::Acquire) {
            return;
        }

        let storm = self.in_a_storm() && !executor::manager_alive();

        if !storm {
            self.ensure_floor();

            if !self.blocking.is_empty() {
                self.start_sleep(sleep_target());
            }
        }

        let stranded = self.live() == 0 && !executor::manager_alive();

        if storm || stranded {
            executor::write_off_pool();
        }
    }

    /// Wakes every parked thread, so deaths a test asked for land
    /// together
    pub(crate) fn wake_everyone(&'static self) {
        let highest = self.highest.load(Ordering::Acquire);

        for index in 0..highest {
            let _ = self.workers[index].slot().wake();
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);

        for index in 0..sleeps {
            let _ = self.sleeps[index].slot().wake();
        }
    }

    /// What the pool looks like right now
    pub(crate) fn stats(&'static self) -> PoolStats {
        let highest = self.highest.load(Ordering::Acquire);
        let mut workers = Vec::new();

        for index in 0..highest {
            let worker = &self.workers[index];

            if !worker.slot().state().alive() {
                continue;
            }

            workers.push(WorkerStats::new(
                worker.slot().busy(),
                worker.backlog(),
                worker.completed(),
            ));
        }

        let sleeps = self.sleeps_highest.load(Ordering::Acquire);
        let mut sleep_threads = 0;
        let mut sleep_busy = 0;

        for index in 0..sleeps {
            let sleep = &self.sleeps[index];

            if !sleep.slot().alive() {
                continue;
            }

            sleep_threads += 1;
            sleep_busy += sleep.slot().busy() as usize;
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
            target(),
            ceiling(),
            sleep_target(),
            self.peak_workers.load(Ordering::Relaxed),
            self.peak_sleeps.load(Ordering::Relaxed),
            self.deaths.load(Ordering::Relaxed),
            self.dead.load(Ordering::SeqCst),
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

/// Manager ticks that add up to the shorter idle window past the
/// target
#[inline(always)]
fn over_ticks() -> u32 {
    (IDLE_REAP_OVER.as_nanos() / MANAGER_TICK.as_nanos().max(1)).max(1) as u32
}

/// Counts a tick of overload, and says whether it has lasted long
/// enough to earn one more thread
///
/// Each thread already past the target makes the next wait a tick
/// longer. A tick without overload starts the count again, and so
/// does earning a thread
fn earned(ticks: &AtomicU32, overloaded: bool, over: usize) -> bool {
    if !overloaded {
        ticks.store(0, Ordering::Relaxed);
        return false;
    }

    let held = ticks.fetch_add(1, Ordering::Relaxed) as usize + 1;

    if held < over + 2 {
        return false;
    }

    ticks.store(0, Ordering::Relaxed);

    true
}

/// Sleep threads the pool settles around under load
///
/// Passed only when every sleep thread is busy and the blocking
/// queue is deep, and never past the ceiling
#[inline(always)]
pub(crate) fn sleep_target() -> usize {
    (cores() * SLEEP_MULTIPLIER).min(ceiling())
}

/// Workers the pool never goes below
#[inline(always)]
pub(crate) fn floor() -> usize {
    cores()
}

/// Workers the pool settles around under load
///
/// Passed only on overload or real need, and never past the ceiling
#[inline(always)]
pub(crate) fn target() -> usize {
    (cores() * WORKER_MULTIPLIER).min(ceiling())
}

/// The most threads of either kind the pool will ever run
///
/// The lower of the static arrays and what the kernel lets a process
/// hold, less a reserve for every thread that isn't the pool's
#[inline(always)]
pub(crate) fn ceiling() -> usize {
    thread_budget()
        .saturating_sub(THREAD_RESERVE)
        .clamp(floor(), MAX_WORKERS)
}

/// Threads the kernel lets one process hold, asked on first use
fn thread_budget() -> usize {
    static BUDGET: AtomicUsize = AtomicUsize::new(0);

    let cached = BUDGET.load(Ordering::Relaxed);

    if cached != 0 {
        return cached;
    }

    let mut threads: libc::c_int = 0;
    let mut size = mem::size_of::<libc::c_int>();

    let asked = unsafe {
        libc::sysctlbyname(
            c"kern.num_taskthreads".as_ptr(),
            (&mut threads as *mut libc::c_int).cast::<libc::c_void>(),
            &mut size,
            ptr::null_mut(),
            0,
        )
    };

    // A kernel that won't say is taken to allow the arrays' worth
    let budget = match asked == 0 && threads > 0 {
        true => threads as usize,
        false => MAX_WORKERS + THREAD_RESERVE,
    };

    BUDGET.store(budget, Ordering::Relaxed);

    budget
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
