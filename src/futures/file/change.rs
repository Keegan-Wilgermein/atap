//! # Change
//! What happened to a watched path, and the snapshot a watch
//! works it out from
//!
//! The kernel reports a change by waking the task and nothing
//! more, so what changed is read back off the path itself rather
//! than off the event

use crate::{
    RuntimeError,
    futures::file::file_task::{Fd, retried},
};
use std::{
    ffi::CString,
    mem,
    ops::{BitOr, BitOrAssign},
};

/// Every note a watch looks for unless it is told otherwise
///
/// `NOTE_REVOKE` is deliberately not among them. Nothing here
/// can report it, and a watch only ever asks for notes it has an
/// answer for. A revoked file is found by the backstop instead,
/// as the error its next look fails with
pub(super) const EVERY_NOTE: u32 = libc::NOTE_WRITE
    | libc::NOTE_EXTEND
    | libc::NOTE_ATTRIB
    | libc::NOTE_LINK
    | libc::NOTE_RENAME
    | libc::NOTE_DELETE;

/// What happened to a watched path
///
/// Several of these can be true at once: one write that makes a
/// file longer is both [`written`] and [`extended`]
///
/// ## Behaviour
/// Also what [`WatchTask::only`] takes, built by naming the parts
/// wanted:
///
/// ```ignore
/// File::watch(&path).only(Change::REMOVED | Change::RENAMED)
/// ```
///
/// [`written`]: Change::written
/// [`extended`]: Change::extended
/// [`WatchTask::only`]: crate::WatchTask::only
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Change(u32);

impl Change {
    /// The contents were written to
    pub const WRITTEN: Self = Self(libc::NOTE_WRITE);

    /// The file grew
    pub const EXTENDED: Self = Self(libc::NOTE_EXTEND);

    /// The permissions or the owner moved
    pub const ATTRIBUTES: Self = Self(libc::NOTE_ATTRIB);

    /// A hard link to the file came or went
    pub const LINKED: Self = Self(libc::NOTE_LINK);

    /// The path stopped leading to the file
    pub const RENAMED: Self = Self(libc::NOTE_RENAME);

    /// The last name the file had was taken away
    pub const REMOVED: Self = Self(libc::NOTE_DELETE);

    /// Every one of them, which is what a watch looks for unless
    /// it is told otherwise
    pub const ANY: Self = Self(EVERY_NOTE);

    /// Wraps a set of notes
    pub(super) const fn new(notes: u32) -> Self {
        Self(notes)
    }

    /// The notes themselves, for handing to the kernel
    pub(super) const fn notes(self) -> u32 {
        self.0
    }

    /// Whether nothing at all is named
    const fn is_nothing(self) -> bool {
        self.0 == 0
    }

    /// Whether the contents were written to
    ///
    /// True for any write, including one that leaves the file the
    /// same length
    pub fn written(self) -> bool {
        self.holds(Self::WRITTEN)
    }

    /// Whether the file grew
    ///
    /// A write past the old end is both this and [`written`]. A
    /// write that shortens the file is only `written`
    ///
    /// [`written`]: Change::written
    pub fn extended(self) -> bool {
        self.holds(Self::EXTENDED)
    }

    /// Whether the permissions or the owner moved
    ///
    /// Timestamps are not among them: a write time set by hand
    /// reads as a [`written`], and a read time is not reported at
    /// all, since a watch is not meant to fire on someone reading
    /// the file
    ///
    /// [`written`]: Change::written
    pub fn attributes(self) -> bool {
        self.holds(Self::ATTRIBUTES)
    }

    /// Whether a hard link to the file came or went
    ///
    /// The last one going is a removal instead. For a directory,
    /// this is a subdirectory coming or going
    pub fn linked(self) -> bool {
        self.holds(Self::LINKED)
    }

    /// Whether the path stopped leading to the file
    ///
    /// True whether the file was moved out from under the path or
    /// something else was moved on top of it. The watch stays on
    /// the file it started on either way, since it holds that
    /// file's descriptor rather than the name
    pub fn renamed(self) -> bool {
        self.holds(Self::RENAMED)
    }

    /// Whether the last name the file had was taken away
    ///
    /// Nothing can happen to the file after this: the watch holds
    /// the only reference left to it
    pub fn removed(self) -> bool {
        self.holds(Self::REMOVED)
    }

    /// Whether every note in `part` is named here
    fn holds(self, part: Self) -> bool {
        self.0 & part.0 == part.0
    }
}

impl BitOr for Change {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

impl BitOrAssign for Change {
    fn bitor_assign(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

/// What a watched path looked like the last time a run reported
///
/// ## Behaviour
/// Taken through the descriptor the watch holds as well as
/// through the path, which is what lets a removal or a rename be
/// told apart: the descriptor keeps the file alive after its last
/// name is gone
///
/// #### Note
/// The change time is deliberately not among these, even though
/// it is what `NOTE_ATTRIB` follows. A filesystem may move it on
/// its own schedule: APFS bumps it tens to hundreds of
/// milliseconds after a write lands, leaving the write time
/// where it was, which is indistinguishable from someone setting
/// a mode. So the attributes are compared directly instead, and
/// the change time is not read at all
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Snapshot {
    /// Which file this is
    ino: u64,

    /// Length in bytes
    size: u64,

    /// Last write, to the nanosecond
    mtime: (i64, i64),

    /// The full mode word, permissions and type together
    mode: u32,

    /// Who owns it
    owner: (u32, u32),

    /// Names the file still has, and zero once it has none
    nlink: u64,

    /// Which file the path leads to now, and `None` for nothing
    at_path: Option<u64>,
}

impl Snapshot {
    /// Looks at the file and at the path it was opened by
    pub(super) fn take(fd: &Fd, path: &CString) -> Result<Self, RuntimeError> {
        let mut raw: libc::stat = unsafe { mem::zeroed() };

        retried(|| unsafe { libc::fstat(fd.raw(), &mut raw) })?;

        Ok(Self {
            ino: raw.st_ino,
            size: raw.st_size.max(0) as u64,
            mtime: (raw.st_mtime, raw.st_mtime_nsec),
            mode: raw.st_mode as u32,
            owner: (raw.st_uid, raw.st_gid),
            nlink: raw.st_nlink as u64,
            at_path: at_path(path),
        })
    }

    /// What moved between this snapshot and a later one
    ///
    /// ## Behaviour
    /// Only the notes in `wanted` are looked for, so a watch
    /// narrowed to removals doesn't settle on a write
    ///
    /// ## Returns
    /// `None` when nothing wanted moved
    pub(super) fn against(&self, now: &Self, wanted: u32) -> Option<Change> {
        let mut change = Change::new(0);

        // Every one of these is a move between the two snapshots
        // rather than something true of the later one. A file that
        // is still gone, or still away from its path, has not
        // changed again since the run that said so, and reporting
        // it twice would leave a repeat spinning on an answer it
        // has already given
        if now.nlink == 0 {
            // Nothing else can be said about a file with no names
            // left, and the notes that go with losing one would only
            // muddy it
            if self.nlink != 0 {
                change |= Change::REMOVED;
            }
        } else {
            if now.at_path != self.at_path {
                change |= Change::RENAMED;
            }

            if now.nlink != self.nlink {
                change |= Change::LINKED;
            }

            if now.size > self.size {
                change |= Change::EXTENDED;
            }

            if now.mtime != self.mtime {
                change |= Change::WRITTEN;
            }

            // The attributes themselves rather than the change time
            // that follows them, which a filesystem moves on its own
            // after a write
            if now.mode != self.mode || now.owner != self.owner {
                change |= Change::ATTRIBUTES;
            }
        }

        let wanted = Change::new(change.notes() & wanted);

        match wanted.is_nothing() {
            true => None,
            false => Some(wanted),
        }
    }

    /// Whether the file has no names left, so nothing can happen
    /// to it again
    pub(super) fn gone(&self) -> bool {
        self.nlink == 0
    }
}

/// Which file a path leads to now
///
/// Follows a symbolic link, the same as the open the watch was
/// started by
///
/// ## Returns
/// `None` for a path that leads nowhere, and for one that can't
/// be asked about at all, which amounts to the same thing for a
/// watch
fn at_path(path: &CString) -> Option<u64> {
    let mut raw: libc::stat = unsafe { mem::zeroed() };

    retried(|| unsafe { libc::stat(path.as_ptr(), &mut raw) })
        .ok()
        .map(|_| raw.st_ino)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A snapshot of a file that hasn't moved
    fn settled() -> Snapshot {
        Snapshot {
            ino: 7,
            size: 100,
            mtime: (1000, 0),
            mode: 0o100_644,
            owner: (501, 20),
            nlink: 1,
            at_path: Some(7),
        }
    }

    /// Nothing moving reports nothing
    #[test]
    fn a_snapshot_against_itself_is_no_change() {
        let seen = settled();

        assert_eq!(seen.against(&seen, EVERY_NOTE), None);
    }

    /// A longer file with a newer write time is both
    #[test]
    fn a_write_past_the_end_is_written_and_extended() {
        let seen = settled();
        let mut now = seen;

        now.size = 200;
        now.mtime = (1001, 0);

        let change = seen.against(&now, EVERY_NOTE).expect("a write must report");

        assert!(change.written(), "the write time moved");
        assert!(change.extended(), "the file got longer");
        assert!(!change.attributes(), "a write is not an attribute change");
    }

    /// A write that leaves the length alone is only a write
    #[test]
    fn a_write_in_place_does_not_extend() {
        let seen = settled();
        let mut now = seen;

        now.mtime = (1001, 0);

        let change = seen.against(&now, EVERY_NOTE).expect("a write must report");

        assert!(change.written(), "the write time moved");
        assert!(!change.extended(), "the file is the same length");
    }

    /// A mode or an owner moving is an attribute change
    #[test]
    fn a_new_mode_or_owner_is_an_attribute_change() {
        let seen = settled();

        let mut chmodded = seen;
        chmodded.mode = 0o100_600;

        let mut chowned = seen;
        chowned.owner = (0, 0);

        for now in [chmodded, chowned] {
            let change = seen.against(&now, EVERY_NOTE).expect("an attribute must report");

            assert!(change.attributes(), "an attribute moved");
            assert!(!change.written(), "the write time stayed where it was");
        }
    }

    /// A file with no names left reads as removed, and as nothing
    /// else
    #[test]
    fn an_unlinked_file_reads_as_removed() {
        let seen = settled();
        let mut now = seen;

        now.nlink = 0;
        now.at_path = None;
        now.mtime = (1001, 0);

        let change = seen.against(&now, EVERY_NOTE).expect("a removal must report");

        assert_eq!(change, Change::REMOVED, "a removal is the whole answer");
    }

    /// A path that no longer leads to the file reads as a rename,
    /// whether the file moved or something took its place
    #[test]
    fn a_path_that_lost_the_file_reads_as_renamed() {
        let seen = settled();

        let mut moved = seen;
        moved.at_path = None;

        let mut replaced = seen;
        replaced.at_path = Some(9);

        for now in [moved, replaced] {
            let change = seen.against(&now, EVERY_NOTE).expect("a rename must report");

            assert!(change.renamed(), "the path stopped leading to the file");
            assert!(!change.removed(), "the file itself is still there");
        }
    }

    /// Reporting a removal once is enough: the run after it finds
    /// a file that is still gone, which is not a change
    #[test]
    fn a_removal_is_not_reported_twice() {
        let seen = settled();
        let mut gone = seen;

        gone.nlink = 0;
        gone.at_path = None;

        assert_eq!(seen.against(&gone, EVERY_NOTE), Some(Change::REMOVED));
        assert_eq!(gone.against(&gone, EVERY_NOTE), None, "still gone is not news");
    }

    /// The same for a rename: a path that is still away from its
    /// file has not moved again
    #[test]
    fn a_rename_is_not_reported_twice() {
        let seen = settled();
        let mut moved = seen;

        moved.at_path = None;

        let change = seen.against(&moved, EVERY_NOTE).expect("a rename must report");

        assert!(change.renamed(), "the path stopped leading to the file");
        assert_eq!(moved.against(&moved, EVERY_NOTE), None, "still away is not news");
    }

    /// A hard link coming or going moves the count
    #[test]
    fn a_new_link_reads_as_linked() {
        let seen = settled();
        let mut now = seen;

        now.nlink = 2;

        let change = seen.against(&now, EVERY_NOTE).expect("a link must report");

        assert!(change.linked(), "the link count moved");
    }

    /// A watch narrowed to removals doesn't settle on a write
    #[test]
    fn a_narrowed_watch_ignores_what_it_did_not_ask_for() {
        let seen = settled();
        let mut now = seen;

        now.size = 200;
        now.mtime = (1001, 0);

        assert_eq!(seen.against(&now, Change::REMOVED.notes()), None);

        now.nlink = 0;

        assert_eq!(
            seen.against(&now, Change::REMOVED.notes()),
            Some(Change::REMOVED),
        );
    }

    /// Naming parts builds the set the kernel is handed
    #[test]
    fn naming_two_parts_holds_both() {
        let picked = Change::REMOVED | Change::RENAMED;

        assert!(picked.removed(), "a removal was named");
        assert!(picked.renamed(), "a rename was named");
        assert!(!picked.written(), "a write was not");
        assert_eq!(Change::ANY.notes(), EVERY_NOTE);
    }
}
