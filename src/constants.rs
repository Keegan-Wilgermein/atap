//! # Constants
//! Constants used throughout the crate

use std::time::Duration;

/// Defines the crossover between pausing the thread
/// when sleeping or starting a syscall
pub(crate) const SLEEP_TOLERANCE: Duration = Duration::from_micros(500);

/// Amount of kevents that can be processed
/// by a single `kevent`
pub(crate) const KEVENT_COUNT: usize = 16;

/// Delay before the first restart of a dead `Reactor`,
/// multiplied by the number of consecutive failures
pub(crate) const RESTART_BACKOFF: Duration = Duration::from_millis(10);

/// How long a `Reactor` has to survive before its
/// restart is treated as a one off rather than a cycle
pub(crate) const RESTART_WINDOW: Duration = Duration::from_secs(5);

/// Consecutive restarts inside `RESTART_WINDOW`
/// before the supervisor stops trying
pub(crate) const RESTART_LIMIT: u32 = 5;

/// Stored in place of the kqueue id once no
/// `Reactor` is listening on one
pub(crate) const DEAD_KQUEUE_ID: i32 = -1;

/// Bytes between the start of a task's slot and its payload
///
/// Doubles as the budget for a slot's header, which an assert
/// in `TaskData::init` holds it to. Slots sit at multiples of
/// `SLOT_SIZE` inside a page aligned block, so an offset of 128
/// also aligns the payload for any output type that doesn't
/// ask for more than 128 byte alignment
///
/// #### Note
/// Raised from 64 once the header had been exactly full for
/// long enough that every new field was going into padding.
/// The room is deliberate headroom rather than space anything
/// currently needs
pub(crate) const PAYLOAD_OFFSET: usize = 128;

/// Bytes one task slot takes up in a table block
///
/// Slots are carved out of shared pages rather than given a
/// mapping each, so this is the whole cost of a task whose
/// output fits beside its header. Sized so a slot lands on
/// its own cache line, which keeps two tasks from sharing the
/// word their listeners are blocked on
pub(crate) const SLOT_SIZE: usize = 256;

/// Bytes of output a slot can hold beside its header
///
/// Anything larger gets a mapping of its own and the header
/// points at it, which nothing realistic ever needs
///
/// #### Note
/// A real cliff rather than a gentle one. An output a byte
/// over this costs a whole page instead of the bytes it
/// actually needs, so the difference between just under and
/// just over is two orders of magnitude
pub(crate) const INLINE_PAYLOAD: usize = SLOT_SIZE - PAYLOAD_OFFSET;

/// Slots in the first block of the task table
pub(crate) const FIRST_BLOCK: usize = 256;

/// `FIRST_BLOCK` as a power of two
pub(crate) const FIRST_BLOCK_LOG2: u32 = FIRST_BLOCK.trailing_zeros();

/// Blocks in the task table, each twice the size of the last
///
/// Held at 23 rather than anything larger because a slot's
/// free list and run queue links are `u32`s, so an id plus one
/// has to fit in 32 bits. That caps the table at a shade over
/// two billion live tasks, which is 274GB of slots and so not
/// a ceiling anybody is going to reach
pub(crate) const TABLE_BLOCKS: usize = 23;

/// Bits of a tagged stack head given over to the index,
/// leaving the rest for the tag that defeats ABA
///
/// Shared by the task table's free list and the injector's
/// ready bands, which are the same structure pointed at
/// different ends of a task's life
pub(crate) const TAG_SHIFT: u32 = 48;

/// Picks the index back out of a tagged stack head
pub(crate) const INDEX_MASK: usize = (1 << TAG_SHIFT) - 1;

/// One past the highest id the task table can address
///
/// Anything at or above this has no slot and never will, so
/// it is the id a handle gets when there was nowhere to put
/// its task
pub(crate) const MAX_TASK_ID: usize = (FIRST_BLOCK << TABLE_BLOCKS) - FIRST_BLOCK;

/// The `kevent` ident the `Reactor` wakes a waiting thread on
///
/// One ident covers every wake, because a thread can only be
/// waiting on one thing at a time. Waiting is what it is doing
/// instead of running, so a second wait can't overlap the first
///
/// #### Note
/// Doesn't collide with the sleep timers a thread registers on
/// the same queue, even at the same value. A kqueue keys an
/// event on its ident and its filter together, and these are
/// `EVFILT_USER` against their `EVFILT_TIMER`
pub(crate) const WAKE_IDENT: usize = 0;

/// Task ids one worker can hold in its own ring
///
/// A power of two so the ring indexes with a mask rather than
/// a division. Small on purpose: a worker only needs enough
/// work to keep itself fed between visits to the injector, and
/// a long local queue is work that isn't available to be stolen
pub(crate) const LOCAL_QUEUE: usize = 256;

/// Picks a ring position out of a head or tail counter
pub(crate) const LOCAL_QUEUE_MASK: u32 = (LOCAL_QUEUE - 1) as u32;

/// Workers the static pool has room for
///
/// The bound on the array, not the number that run. The live
/// cap is `WORKER_MULTIPLIER` times the core count, so this
/// only has to be large enough that no real machine hits it
/// 
/// 512 so a CPU with 128 cores will be able to make use of all
/// of it's cores without using too much memory
/// 
/// More for future proofing than anything
pub(crate) const MAX_WORKERS: usize = 512;

/// Live workers allowed per core
///
/// Above one because a worker handing a blocking task to its
/// sleep thread is still occupied by it, so a pool capped at
/// the core count would sit idle with work queued
pub(crate) const WORKER_MULTIPLIER: usize = 4;

/// Sleep threads allowed per core
///
/// Higher than the worker multiplier because a sleep thread is
/// almost always inside a `kevent` call rather than on a core,
/// so the number of them that makes sense has very little to
/// do with how many cores there are
pub(crate) const SLEEP_MULTIPLIER: usize = 8;

/// How often the manager wakes to do policy on its own
pub(crate) const MANAGER_TICK: Duration = Duration::from_millis(10);

/// How often a shutdown looks to see whether the pool has
/// finished draining
///
/// Polled rather than woken because a shutdown happens once and
/// the thread asking for it has nothing else to do. Wiring a
/// wake through every path that could empty the pool would put
/// a cost on every task to save a few milliseconds at the end
/// of the process
pub(crate) const SHUTDOWN_POLL: Duration = Duration::from_millis(1);

/// How long a worker sits idle before it is reaped
pub(crate) const IDLE_REAP: Duration = Duration::from_millis(500);

/// How much of a file or a pipe one read or write
/// syscall asks for
///
/// A file task can't be taken out of the kernel the way a sleep
/// can, so the loop between chunks is the only place a cancel
/// has to land. That makes this a cancellation granularity as
/// much as a buffer size: a task cancelled the instant after a
/// chunk starts runs until that chunk comes back
///
/// #### Note
/// Small enough that a cancel isn't left waiting on a slow
/// mount, large enough that a big file isn't a syscall per page
///
/// Shared with the process tasks draining a child's output
/// rather than given a constant of its own. The same number is
/// right there for a second reason — it is what a pipe holds —
/// so a read this size empties a full one in a single call
pub(crate) const FILE_CHUNK: usize = 64 * 1024;

/// How long a process task waits before looking at
/// a child again, when it has no queue to wait on
///
/// ## Behaviour
/// Only reached when the kernel wouldn't give this thread a
/// kqueue, which takes it running out of descriptors. The
/// ordinary path waits on an event and this one doesn't wait on
/// anything, so it has to come round often enough that a cancel
/// isn't left sitting behind a child that runs for hours
///
/// #### Note
/// A cancellation granularity, the same as `FILE_CHUNK`, and
/// picked the same way: short enough that a cancelled child is
/// killed promptly, long enough that a degraded path isn't also
/// a busy one
pub(crate) const PROCESS_POLL: Duration = Duration::from_millis(50);

/// Tasks that may overtake a queued one before it counts
/// as starving
///
/// Measured in tasks rather than time because that is what a
/// task's sequence number counts, and being overtaken is the
/// thing that actually starves a task
pub(crate) const STARVE_AGE: u64 = 4096;

/// Pops a starving queue is served oldest first for, each
/// time the manager finds it starving
///
/// A budget rather than a mode. Turning the order upside down
/// and leaving it there would invert priority for exactly as
/// long as the backlog is deep, which is when priority is
/// worth having. A budget per tick drains the oldest work
/// steadily while everything else is still served in the order
/// the caller asked for
pub(crate) const STARVE_RELIEF: u32 = 64;

/// Priority bands the injector serves, highest first
pub(crate) const PRIORITY_BANDS: usize = 4;

/// Turns a priority class into the band that serves it
pub(crate) const PRIORITY_BAND_SHIFT: u32 = 6;

/// Bits of a packed priority given over to the class,
/// leaving the rest for the sequence
pub(crate) const PRIORITY_CLASS_SHIFT: u32 = 56;

/// Picks the sequence back out of a packed priority
pub(crate) const PRIORITY_SEQUENCE_MASK: u64 = (1 << PRIORITY_CLASS_SHIFT) - 1;

/// The class a task gets when the caller doesn't pick one
///
/// Halfway up on purpose, so a caller has as much room to
/// drop a task below the default as to lift one above it
pub const DEFAULT_PRIORITY: u8 = 128;

/// Slots kept spare above the live count when trimming
///
/// Headroom, so a table trimmed the moment a burst ends isn't
/// immediately grown again by the next one
pub(crate) const TRIM_THRESHOLD: usize = 4096;

/// The most of itself a table may keep when trimming, as a
/// percentage
///
/// A fifth off at a time rather than everything at once, so
/// repeated passes converge on the floor gently instead of one
/// pass giving back everything a program was about to reuse
pub(crate) const TRIM_KEEP_PERCENT: usize = 80;

/// Slots a table keeps whatever else happens
pub(crate) const TRIM_MINIMUM: usize = 100;

/// Manager ticks between attempts to trim the table
///
/// Far rarer than anything else the manager does, because a
/// trim walks the free list and a table worth trimming is one
/// nothing is in a hurry about
pub(crate) const TRIM_INTERVAL: u32 = 500;

/// Stored in a slot's waiting field when its task isn't
/// sitting in a kernel wait
pub(crate) const NOT_WAITING: i32 = -1;

/// The `waiting` value for a slot nobody is selecting on
pub(crate) const NO_SELECT: i32 = -1;

/// The ident a `join_first` notification arrives under
///
/// A fixed number rather than the task's id, because the thread
/// waiting on it re-reads every slot it was given anyway and so
/// never needs to be told which one woke it. One ident keeps
/// the fire side a single constant and the wait side a single
/// comparison
///
/// #### Note
/// Unique on a thread's own queue for the pair it is used as.
/// `WAKE_IDENT` is the only other `EVFILT_USER` ident that
/// lands there, and sleeps use `EVFILT_TIMER` with the task id,
/// which is a different filter
pub(crate) const SELECT_IDENT: usize = 1;

/// How long a `join_first` waits before looking again anyway
///
/// The notification is what makes it prompt; this is what makes
/// it correct. Nothing about the result depends on a wake ever
/// arriving — a missed one costs latency, not an answer
pub(crate) const SELECT_POLL: Duration = Duration::from_millis(50);

/// Stored in a slot's waiting field while a canceller is part
/// way through interrupting it
///
/// The waiting thread can't clear the field, and so can't
/// finish and let its queue be closed, until the canceller has
/// put it back. That is what stops a cancel landing on a
/// descriptor that has already been closed and reused
pub(crate) const CANCELLING: i32 = i32::MIN;

/// Stored in place of a task id when a worker isn't on one
pub(crate) const NO_TASK: usize = usize::MAX;

/// The first `kevent` ident a scheduled task may use on the
/// manager's queue
///
/// A `repeat_every` task waits on a timer identified by its own
/// id, and a kqueue keys an event on its ident and filter
/// together — so an id of 1 would land on the same timer as
/// `MANAGER_TICK_IDENT` and quietly re-arm the manager's own
/// tick instead of the task. Shifting task idents clear of the
/// ones this crate reserves is what keeps them apart
pub(crate) const SCHEDULE_IDENT_BASE: usize = 2;

/// The `kevent` ident the manager's own tick arrives on
///
/// Distinct from `WAKE_IDENT` so a tick and a poke can be told
/// apart, though the manager does the same work either way
pub(crate) const MANAGER_TICK_IDENT: usize = 1;
