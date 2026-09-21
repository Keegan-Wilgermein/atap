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
        net::step::settle,
        task::{
            Nothing, Task,
            sealed::{self, Park, Step},
        },
    },
    modules::{c_path::c_path, fd::Fd, park, retried::retried},
};
use std::{ffi::CString, mem, path::Path, sync::Arc, time::Instant};

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

    /// Whether a path that isn't there is waited for
    appear: bool,

    /// The directory watched while the path isn't there yet
    parent: Option<Arc<Fd>>,
}

impl WatchTask {
    /// Watches `path`
    pub(crate) fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: c_path(path),
            notes: EVERY_NOTE,
            seen: None,
            fd: None,
            appear: false,
            parent: None,
        }
    }

    /// Waits for a path that isn't there yet, rather than failing
    ///
    /// ## Behaviour
    /// A run that finds nothing at the path watches the directory
    /// it would be in, and settles with [`Change::CREATED`] once
    /// something appears there. Later runs watch what appeared. A
    /// path already there is watched as usual from the start
    ///
    /// ## Returns
    /// The task
    ///
    /// #### Note
    /// The directory itself has to be there. One that isn't gives
    /// `ENOENT`
    ///
    /// [`Change::CREATED`]: crate::fs::Change::CREATED
    pub fn appear(mut self) -> Self {
        self.appear = true;
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

    /// Looks for a path that wasn't there, and parks on its
    /// directory if it still isn't
    ///
    /// ## Returns
    /// `None` once the path is there and watched as usual
    fn arrival(&mut self) -> Result<Option<Step<Result<Change, RuntimeError>>>, RuntimeError> {
        let path = self.path.clone().ok_or(RuntimeError::BadPath)?;
        let waited = self.parent.is_some();

        if exists(&path) {
            match self.watching() {
                Ok(_) if waited => {
                    self.parent = None;

                    return Ok(Some(Step::Done(Ok(Change::CREATED))));
                }

                Ok(_) => return Ok(None),

                // Gone again before it could be opened
                Err(RuntimeError::CheckError(Some(libc::ENOENT))) => {}
                Err(error) => return Err(error),
            }
        }

        let parent = match &self.parent {
            Some(parent) => Arc::clone(parent),

            None => {
                let parent = Arc::new(open_watch(&directory_of(&path)?)?);
                self.parent = Some(Arc::clone(&parent));

                // Looked for again, since it may have come while the
                // directory was being opened
                return self.arrival();
            }
        };

        Ok(Some(Step::Park(Park {
            ident: parent.raw(),
            filter: libc::EVFILT_VNODE,
            notes: libc::NOTE_WRITE,
            deadline: Some(Instant::now() + VNODE_POLL),
        })))
    }

    /// Looks at the path, and parks if nothing wanted has moved
    fn advance(&mut self) -> Result<Step<Result<Change, RuntimeError>>, RuntimeError> {
        if self.appear && self.fd.is_none() {
            if let Some(step) = self.arrival()? {
                return Ok(step);
            }
        }

        let (fd, seen) = self.watching()?;

        let now = {
            let path = self.path.as_ref().ok_or(RuntimeError::BadPath)?;

            Snapshot::take(&fd, path)?
        };

        if let Some(change) = seen.against(&now, self.notes) {
            self.seen = Some(now);

            return Ok(Step::Done(Ok(change)));
        }

        // A change landing before the watch goes on wakes nothing, so
        // the backstop bounds the wait. A file with no names left
        // needs none
        let deadline = match seen.gone() {
            true => None,
            false => Some(Instant::now() + VNODE_POLL),
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

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

/// Whether anything is at a path, following a link
fn exists(path: &CString) -> bool {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::stat(path.as_ptr(), &mut raw) }).is_ok()
}

/// The directory a path is in
fn directory_of(path: &CString) -> Result<CString, RuntimeError> {
    let bytes = path.as_bytes();
    let trimmed = bytes.strip_suffix(b"/").unwrap_or(bytes);

    let parent = match trimmed.iter().rposition(|byte| *byte == b'/') {
        Some(0) => &b"/"[..],
        Some(at) => &trimmed[..at],
        None => &b"."[..],
    };

    CString::new(parent).map_err(|_| RuntimeError::BadPath)
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
