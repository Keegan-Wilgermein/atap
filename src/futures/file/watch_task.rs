//! # Watch task
//! The task `File::watch` returns, and everything it does once
//! run
//!
//! The one file task that parks, holding no thread while it waits

use crate::modules::input::Token;
use crate::{
    RuntimeError,
    constants::{INLINE_PAYLOAD, VNODE_POLL},
    futures::{
        file::change::{Change, EVERY_NOTE, Snapshot},
        net::step::{Clock, settle},
        task::{
            Nothing, Task,
            sealed::{self, Park, Step},
        },
    },
    modules::{c_path::c_path, fd::Fd, park, retried::retried},
};
use std::{
    ffi::CString,
    mem,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Change, RuntimeError>>() <= INLINE_PAYLOAD);

/// Waits for a path to change
///
/// ## Returns
/// What changed, as the notes that moved since this task last
/// reported
///
/// ## Behaviour
/// A `.repeat()` of one loses nothing: each run compares against
/// what the last one reported, so a change that lands between
/// runs is still found
///
/// #### Note
/// `.at_rate()` is the exception. Every run there is a fresh copy,
/// and each copy compares against where the original started, so
/// runs report the same change over again. Use `.repeat()` for a
/// stream of changes
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct WatchTask {
    /// The path to watch, already in the form the kernel takes
    ///
    /// `None` when the path had a zero byte in it, which is
    /// reported when the task runs
    path: Option<CString>,

    /// Which notes count, both for the watch the kernel is given
    /// and for what the comparison looks at
    notes: u32,

    /// The timeout
    clock: Clock,

    /// What this task last reported, and `None` before it has ever
    /// run
    ///
    /// Kept across runs, so a repeat loses nothing
    seen: Option<Snapshot>,

    /// The descriptor the watch goes on, held open for as long as
    /// the task is
    ///
    /// Shared, so a copy watches the same file rather than opening
    /// the path again
    fd: Option<Arc<Fd>>,
}

impl WatchTask {
    /// Watches `path`
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            notes: EVERY_NOTE,
            clock: Clock::default(),
            seen: None,
            fd: None,
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Counted from when the run starts. Running out with nothing
    /// having changed gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Settles on nothing but the changes named
    ///
    /// ## Behaviour
    /// Every change counts without this. Naming a narrower set
    /// leaves the rest to happen without waking the task:
    ///
    /// ```no_run
    /// # use atap::fs::{Change, File};
    /// # let path = "/tmp/watched";
    /// let watch = File::watch(&path).only(Change::REMOVED | Change::RENAMED);
    /// ```
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last, as long as it is
    /// set before the task runs: the first run is what puts the
    /// watch on
    pub fn only(mut self, wanted: Change) -> Self {
        self.notes = wanted.notes();
        self
    }

    /// The descriptor the watch goes on and the snapshot the next
    /// look is against, opening the path on the first run
    ///
    /// ## Returns
    /// A copy of both
    fn watching(&mut self) -> Result<(Arc<Fd>, Snapshot), RuntimeError> {
        if let (Some(fd), Some(seen)) = (self.fd.as_ref(), self.seen) {
            return Ok((Arc::clone(fd), seen));
        }

        // A path that isn't there fails here, out of the open
        let (fd, seen) = {
            let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;
            let fd = Arc::new(open_watch(path)?);
            let seen = Snapshot::take(&fd, path)?;

            (fd, seen)
        };

        self.fd = Some(Arc::clone(&fd));
        self.seen = Some(seen);

        Ok((fd, seen))
    }

    /// Looks at the path, and parks if nothing wanted has moved
    fn advance(&mut self) -> Result<Step<Result<Change, RuntimeError>>, RuntimeError> {
        let (fd, seen) = self.watching()?;

        let now = {
            let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

            Snapshot::take(&fd, path)?
        };

        if let Some(change) = seen.against(&now, self.notes) {
            self.seen = Some(now);

            return Ok(Step::Done(Ok(change)));
        }

        if self.clock.expired() {
            return Err(RuntimeError::TimedOut);
        }

        // A change landing before the watch goes on wakes nothing, so
        // the backstop bounds the wait. A file with no names left
        // needs none
        let deadline = match seen.gone() {
            true => self.clock.deadline(),

            false => {
                let backstop = Instant::now() + VNODE_POLL;

                Some(match self.clock.deadline() {
                    Some(limit) => limit.min(backstop),
                    None => backstop,
                })
            }
        };

        Ok(Step::Park(Park {
            ident: fd.raw(),
            filter: libc::EVFILT_VNODE,
            notes: self.notes,
            deadline,
        }))
    }
}

impl sealed::Sealed for WatchTask {}

impl Task for WatchTask {
    type Output = Result<Change, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    /// Only the clock starts afresh. What this task has already
    /// seen, and the descriptor it watches, carry across runs
    fn prepare(&mut self, _token: Token) {
        self.clock.start();
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

/// Opens a path for watching and nothing else
///
/// ## Behaviour
/// `O_EVTONLY`, so a watch doesn't hold a volume from being
/// unmounted, and a directory opens as readily as a file
fn open_watch(path: &CString) -> Result<Fd, RuntimeError> {
    let flags = libc::O_EVTONLY | libc::O_NONBLOCK | libc::O_CLOEXEC;

    retried(|| unsafe { libc::open(path.as_ptr(), flags) }).map(Fd::new)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::input::token;

    /// A watch is the one file task that gives its thread back
    #[test]
    fn a_watch_does_not_block() {
        assert!(
            !WatchTask::new("a").blocking(token()),
            "a watch parks instead"
        );
    }

    /// A path with a zero byte in it is reported when it runs,
    /// rather than being opened as the part before the byte
    #[test]
    fn a_path_with_a_zero_byte_is_a_bad_path() {
        let mut task = WatchTask::new("a\0b");

        assert!(matches!(task.advance(), Err(RuntimeError::BadPath)));
    }

    /// A path that isn't there says so, rather than waiting for
    /// one to appear
    #[test]
    fn a_path_that_is_not_there_says_so() {
        let mut task = WatchTask::new("/nonexistent-atap-watch-target");

        assert_eq!(
            task.advance().err(),
            Some(RuntimeError::CheckError(Some(libc::ENOENT))),
        );
    }

    /// Naming a narrower set is what the kernel is handed
    #[test]
    fn a_narrowed_watch_asks_for_less() {
        let task = WatchTask::new("a").only(Change::REMOVED | Change::RENAMED);

        assert_eq!(task.notes, (Change::REMOVED | Change::RENAMED).notes());
        assert_eq!(WatchTask::new("a").notes, EVERY_NOTE);
    }
}
