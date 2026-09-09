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
/// `SLOT_SIZE` inside a page aligned block, so an offset of 64
/// also aligns the payload for any output type that doesn't
/// ask for more than 64 byte alignment
pub(crate) const PAYLOAD_OFFSET: usize = 64;

/// Bytes one task slot takes up in a table block
///
/// Slots are carved out of shared pages rather than given a
/// mapping each, so this is the whole cost of a task whose
/// output fits beside its header. Sized so a slot lands on
/// its own cache line, which keeps two tasks from sharing the
/// word their listeners are blocked on
pub(crate) const SLOT_SIZE: usize = 128;

/// Bytes of output a slot can hold beside its header
///
/// Anything larger gets a mapping of its own and the header
/// points at it, which nothing realistic ever needs
pub(crate) const INLINE_PAYLOAD: usize = SLOT_SIZE - PAYLOAD_OFFSET;

/// Slots in the first block of the task table
pub(crate) const FIRST_BLOCK: usize = 256;

/// `FIRST_BLOCK` as a power of two
pub(crate) const FIRST_BLOCK_LOG2: u32 = FIRST_BLOCK.trailing_zeros();

/// Blocks in the task table, each twice the size of the last
///
/// Doubling from 256 means 32 blocks cover roughly 2^40 ids,
/// so the table has no ceiling worth planning around
pub(crate) const TABLE_BLOCKS: usize = 32;

/// Bits of the free list head given over to the index,
/// leaving the rest for the tag that defeats ABA
pub(crate) const FREE_TAG_SHIFT: u32 = 48;

/// Picks the index back out of a free list head
pub(crate) const FREE_INDEX_MASK: usize = (1 << FREE_TAG_SHIFT) - 1;

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
