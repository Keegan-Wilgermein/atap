//! # Join policy
//! What becomes of the tasks that didn't win a `join_first`

/// What to do with the losers of a [`Runtime::join_first`]
///
/// [`Runtime::join_first`]: crate::Runtime::join_first
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JoinPolicy {
    /// Cancel every task that didn't win
    ///
    /// #### Note
    /// A loser already inside a syscall finishes that syscall
    /// before it stops. Its output is thrown away either way
    Cancel,

    /// Hand the losers back, still running
    ///
    /// The default, and the only policy that returns `Some`. The
    /// losers keep the order they were given in, minus the winner
    #[default]
    PassBack,

    /// Drop the losers' handles and let them run to the end
    ///
    /// Their outputs are thrown away, but anything they do along
    /// the way still happens
    Drop,
}
