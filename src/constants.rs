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
/// Also the budget for a slot's header, which `TaskData::init`
/// asserts. Aligns the payload for anything up to 128 byte
/// alignment
pub(crate) const PAYLOAD_OFFSET: usize = 128;

/// Bytes one task slot takes up in a table block
///
/// A cache line of its own, so two tasks never share the word
/// their listeners are blocked on
pub(crate) const SLOT_SIZE: usize = 256;

/// Bytes of output a slot can hold beside its header
///
/// Anything larger gets a mapping of its own, which costs a
/// whole page
pub(crate) const INLINE_PAYLOAD: usize = SLOT_SIZE - PAYLOAD_OFFSET;

/// Slots in the first block of the task table
pub(crate) const FIRST_BLOCK: usize = 256;

/// `FIRST_BLOCK` as a power of two
pub(crate) const FIRST_BLOCK_LOG2: u32 = FIRST_BLOCK.trailing_zeros();

/// Blocks in the task table, each twice the size of the last
///
/// Capped by the `u32` free list and queue links, which need
/// an id plus one to fit in 32 bits
pub(crate) const TABLE_BLOCKS: usize = 23;

/// Bits of a tagged stack head given over to the index,
/// leaving the rest for the tag that defeats ABA
pub(crate) const TAG_SHIFT: u32 = 48;

/// Picks the index back out of a tagged stack head
pub(crate) const INDEX_MASK: usize = (1 << TAG_SHIFT) - 1;

/// One past the highest id the task table can address
///
/// The id a handle gets when there was nowhere to put its task
pub(crate) const MAX_TASK_ID: usize = (FIRST_BLOCK << TABLE_BLOCKS) - FIRST_BLOCK;

/// The `kevent` ident the `Reactor` wakes a waiting thread on
///
/// One ident covers every wake, since a thread only waits on
/// one thing at a time
pub(crate) const WAKE_IDENT: usize = 0;

/// Task ids one worker can hold in its own ring
///
/// A power of two so the ring indexes with a mask
pub(crate) const LOCAL_QUEUE: usize = 256;

/// Picks a ring position out of a head or tail counter
pub(crate) const LOCAL_QUEUE_MASK: u32 = (LOCAL_QUEUE - 1) as u32;

/// Workers the static pool has room for
///
/// The bound on the array, not the number that run
pub(crate) const MAX_WORKERS: usize = 512;

/// Live workers allowed per core
pub(crate) const WORKER_MULTIPLIER: usize = 4;

/// Sleep threads allowed per core
///
/// Higher than the worker multiplier, since a sleep thread is
/// almost always waiting in the kernel rather than on a core
pub(crate) const SLEEP_MULTIPLIER: usize = 8;

/// How often the manager wakes to do policy on its own
pub(crate) const MANAGER_TICK: Duration = Duration::from_millis(10);

/// How often a shutdown looks to see whether the pool has
/// finished draining
pub(crate) const SHUTDOWN_POLL: Duration = Duration::from_millis(1);

/// How long a worker sits idle before it is reaped
pub(crate) const IDLE_REAP: Duration = Duration::from_millis(500);

/// How much of a file or a pipe one read or write
/// syscall asks for
///
/// Cancels land between chunks, so this is also how long a
/// cancelled file task can keep running
pub(crate) const FILE_CHUNK: usize = 64 * 1024;

/// How long a process task waits before looking at
/// a child again, when it has no queue to wait on
pub(crate) const PROCESS_POLL: Duration = Duration::from_millis(50);

/// Tasks that may overtake a queued one before it counts
/// as starving
pub(crate) const STARVE_AGE: u64 = 1_000_000;

/// Pops a starving queue is served oldest first for, each
/// time the manager finds it starving
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
pub const DEFAULT_PRIORITY: u8 = 128;

/// Slots kept spare above the live count when trimming
pub(crate) const TRIM_THRESHOLD: usize = 4096;

/// The most of itself a table may keep when trimming, as a
/// percentage
pub(crate) const TRIM_KEEP_PERCENT: usize = 80;

/// Slots a table keeps whatever else happens
pub(crate) const TRIM_MINIMUM: usize = 100;

/// Manager ticks between attempts to trim the table
pub(crate) const TRIM_INTERVAL: u32 = 500;

/// Stored in a slot's waiting field when its task isn't
/// sitting in a kernel wait
pub(crate) const NOT_WAITING: i32 = -1;

/// The `waiting` value for a slot nobody is selecting on
pub(crate) const NO_SELECT: i32 = -1;

/// The ident a `join_first` notification arrives under
pub(crate) const SELECT_IDENT: usize = 1;

/// How long a `join_first` waits before looking again anyway
///
/// A missed wake costs latency, not an answer
pub(crate) const SELECT_POLL: Duration = Duration::from_millis(50);

/// Stored in a slot's waiting field while a canceller is part
/// way through interrupting it
///
/// Stops a cancel landing on a descriptor that has already
/// been closed and reused
pub(crate) const CANCELLING: i32 = i32::MIN;

/// Stored in place of a task id when a worker isn't on one
pub(crate) const NO_TASK: usize = usize::MAX;

/// The first `kevent` ident a scheduled task may use on the
/// manager's queue
///
/// Keeps task timers clear of `MANAGER_TICK_IDENT`
pub(crate) const SCHEDULE_IDENT_BASE: usize = 2;

/// The `kevent` ident the manager's own tick arrives on
pub(crate) const MANAGER_TICK_IDENT: usize = 1;
