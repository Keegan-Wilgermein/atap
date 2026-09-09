//! # Join policy
//! What becomes of the tasks that didn't win a `join_first`

/// What to do with the losers of a [`Runtime::join_first`]
///
/// ## Behaviour
/// A race has one winner and a set of tasks that are still
/// going. None of the three answers to that is right often
/// enough to be the only one, which is why this is asked for
/// rather than assumed
///
/// [`Runtime::join_first`]: crate::Runtime::join_first
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum JoinPolicy {
    /// Cancel every task that didn't win
    ///
    /// ## Behaviour
    /// What a hedged request wants: three ways of getting the
    /// same answer, and once one of them has it the other two
    /// are wasted work
    ///
    /// #### Note
    /// Cancelling is not stopping. A task already inside a
    /// syscall runs to the end of it — a file read finishes the
    /// chunk it is on, and a sleep is taken back out of the
    /// kernel but only because sleeps can be. What cancelling
    /// promises is that the output is thrown away and the slot
    /// comes back, not that the work stops this instant
    Cancel,

    /// Hand the losers back, still running
    ///
    /// ## Behaviour
    /// The default policy variant
    /// 
    /// A search that found one match may well want the rest,
    /// and a set raced to see which source is quickest usually
    /// wants the slower answers too
    ///
    /// This is the only policy that returns a `Some`. The order
    /// they were given in is kept, minus the winner
    #[default]
    PassBack,

    /// Drop the losers where they stand
    ///
    /// ## Behaviour
    /// The handles go, the tasks carry on. Every one of them
    /// runs to completion, publishes into a slot nobody is
    /// holding, and gives that slot back when the last listener
    /// leaves — which, once the handle is gone, is the
    /// `Executor` itself
    ///
    /// #### Note
    /// Cheaper than `Cancel` and more wasteful than it. Worth
    /// it when the losers are short, or when their side effects
    /// are wanted even though their outputs are not — a set of
    /// writes where only the first acknowledgement mattered
    Drop,
}
