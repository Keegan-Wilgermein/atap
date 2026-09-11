//! # Exit status
//! What a child said on its way out, in a shape that fits in a
//! task slot

/// The bits of a status word that say how the child ended
///
/// Zero for an ordinary exit, `STOPPED` for a stop, and
/// anything else is the signal that killed it
const STATUS_MASK: i32 = 0o177;

/// What `STATUS_MASK` reads back from a child that stopped
/// rather than ended
const STOPPED: i32 = 0o177;

/// How far up the word an exit code sits
const CODE_SHIFT: i32 = 8;

/// How a child ended
///
/// ## Returns
/// [`ExitStatus::code`] and [`ExitStatus::signal`] never both
/// answer. A child either ran to its own end and has a code,
/// or was killed and has a signal
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExitStatus {
    /// The status word, exactly as `waitpid` gave it back
    raw: i32,
}

impl ExitStatus {
    /// Keeps a status word to be taken apart later
    pub(crate) fn from_raw(raw: i32) -> Self {
        Self { raw }
    }

    /// Whether the child ran to its own end and was happy
    /// about it
    pub fn success(&self) -> bool {
        self.code() == Some(0)
    }

    /// The code the child exited with
    ///
    /// ## Returns
    /// `None` when the child didn't exit on its own terms,
    /// which means a signal killed it and [`ExitStatus::signal`]
    /// has the answer instead
    pub fn code(&self) -> Option<i32> {
        if self.raw & STATUS_MASK != 0 {
            return None;
        }

        Some((self.raw >> CODE_SHIFT) & 0xff)
    }

    /// The signal that killed the child
    ///
    /// ## Returns
    /// `None` when nothing did, which means the child exited on
    /// its own and [`ExitStatus::code`] has the answer instead
    pub fn signal(&self) -> Option<i32> {
        let status = self.raw & STATUS_MASK;

        if status == 0 || status == STOPPED {
            return None;
        }

        Some(status)
    }

    /// The status word as the kernel gave it
    pub fn raw(&self) -> i32 {
        self.raw
    }
}

/// What a child wrote, and how it ended
///
/// Both streams are read to their end, so these are everything
/// the child wrote and not a prefix of it
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessOutput {
    /// Everything the child wrote to its standard output
    stdout: Vec<u8>,

    /// Everything the child wrote to its standard error
    stderr: Vec<u8>,

    /// How it ended
    status: ExitStatus,
}

impl ProcessOutput {
    /// Puts together what a drained child came back with
    pub(crate) fn new(stdout: Vec<u8>, stderr: Vec<u8>, status: ExitStatus) -> Self {
        Self {
            stdout,
            stderr,
            status,
        }
    }

    /// How the child ended
    pub fn status(&self) -> ExitStatus {
        self.status
    }

    /// What the child wrote to its standard output
    pub fn stdout(&self) -> &[u8] {
        &self.stdout
    }

    /// What the child wrote to its standard error
    pub fn stderr(&self) -> &[u8] {
        &self.stderr
    }

    /// Takes the buffers out without copying them
    pub fn into_parts(self) -> (Vec<u8>, Vec<u8>, ExitStatus) {
        (self.stdout, self.stderr, self.status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An ordinary exit reads back as a code and nothing else
    #[test]
    fn an_ordinary_exit_has_a_code_and_no_signal() {
        let ok = ExitStatus::from_raw(0);

        assert!(ok.success(), "a zero status must be a success");
        assert_eq!(ok.code(), Some(0), "a zero status must read as code 0");
        assert_eq!(ok.signal(), None, "a clean exit must have no signal");

        let failed = ExitStatus::from_raw(1 << 8);

        assert!(!failed.success(), "a non zero code must not be a success");
        assert_eq!(failed.code(), Some(1), "the code must come out of the high byte");
        assert_eq!(failed.signal(), None, "a clean exit must have no signal");
    }

    /// A killed child reads back as a signal and nothing else
    #[test]
    fn a_killed_child_has_a_signal_and_no_code() {
        let killed = ExitStatus::from_raw(libc::SIGKILL);

        assert!(!killed.success(), "a killed child must not be a success");
        assert_eq!(killed.code(), None, "a killed child has no code of its own");
        assert_eq!(
            killed.signal(),
            Some(libc::SIGKILL),
            "the signal must come out of the low bits"
        );
    }

    /// A stopped child is neither a code nor a signal
    #[test]
    fn a_stopped_child_is_neither_a_code_nor_a_signal() {
        let stopped = ExitStatus::from_raw((libc::SIGSTOP << 8) | 0o177);

        assert_eq!(stopped.code(), None, "a stop is not an exit");
        assert_eq!(stopped.signal(), None, "a stop is not a kill");
        assert!(!stopped.success(), "a stop is certainly not a success");
    }
}
