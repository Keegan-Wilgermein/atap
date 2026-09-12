//! # Watch task
//! The task `File::watch` returns, and everything it does once
//! run
//!
//! The one file task that parks. A watch is nearly all waiting,
//! so it holds the descriptor and gives its thread back the same
//! way a socket task does, rather than sitting on a sleep thread

use crate::{
    RuntimeError,
    constants::{INLINE_PAYLOAD, VNODE_POLL},
    futures::{
        file::{
            change::{Change, EVERY_NOTE, Snapshot},
            file_task::{Fd, as_c_path},
        },
        net::step::{Clock, settle},
        task::{
            Task,
            sealed::{self, Park, Step},
        },
    },
    modules::{int_check::IntCheck, park},
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
pub struct WatchTask {
    /// The path to watch, already in the form the kernel takes
    ///
    /// `None` when the path had a zero byte in it, which is
    /// reported when the task runs
    path: Option<CString>,

    /// Which notes count, both for the watch the kernel is given
    /// and for what the comparison looks at, so the two can never
    /// disagree
    notes: u32,

    /// The timeout
    clock: Clock,

    /// What this task last reported, and `None` before it has ever
    /// run
    ///
    /// Deliberately kept across runs, which is what makes a repeat
    /// lose nothing: the next run compares against where the last
    /// one stopped
    seen: Option<Snapshot>,

    /// The descriptor the watch goes on, held open for as long as
    /// the task is
    ///
    /// Shared, so a copy watches the same file rather than opening
    /// the path again and finding whatever is there by then
    fd: Option<Arc<Fd>>,
}

impl WatchTask {
    /// Watches `path`
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: as_c_path(path),
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
    /// ```ignore
    /// File::watch(&path).only(Change::REMOVED | Change::RENAMED)
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
    /// A copy of both, so the caller isn't holding a borrow of the
    /// task while it updates what the task has seen. The
    /// descriptor is shared rather than opened again, so every run
    /// watches the file the first one found
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

        // A path's watch only reports what happens once it is on,
        // unlike a socket's. So a change landing between the look
        // above and the watch going on would wake nothing, and the
        // backstop is what makes that cost latency rather than the
        // answer
        //
        // A file whose last name is gone is the one case that needs
        // no backstop: nothing can reach it again, so looking again
        // would only burn a thread
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

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    /// Only the clock starts afresh. What this task has already
    /// seen, and the descriptor it watches, carry across runs
    fn prepare(&mut self) {
        self.clock.start();
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

/// Opens a path for watching and nothing else
///
/// ## Behaviour
/// `O_EVTONLY` is the descriptor this filter wants: it doesn't
/// count as a use of the file, so a watch can't hold a volume
/// from being unmounted. It opens a directory as readily as a
/// file, which is what lets a directory be watched at all
fn open_watch(path: &CString) -> Result<Fd, RuntimeError> {
    let flags = libc::O_EVTONLY | libc::O_NONBLOCK | libc::O_CLOEXEC;

    loop {
        let raw = unsafe { libc::open(path.as_ptr(), flags) }.check();

        match raw {
            Ok(fd) => return Ok(Fd::new(fd)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => continue,
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A watch is the one file task that gives its thread back
    #[test]
    fn a_watch_does_not_block() {
        assert!(!WatchTask::new("a").blocking(), "a watch parks instead");
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
