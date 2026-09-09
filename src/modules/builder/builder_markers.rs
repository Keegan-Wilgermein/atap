//! # Builder Markers
//! The type level states a `TaskBuilder` moves through, and the
//! traits that say what may be done from each of them
//!
//! None of these hold anything or exist at runtime. They are
//! parameters on `TaskBuilder` and nothing else, and what they
//! do is decide which `impl` block a method is found in — so a
//! chain that doesn't make sense is a compile error rather than
//! a field quietly overwriting another
//!
//! ## The three axes
//! A task being built has three independent decisions to make,
//! and they are tracked separately rather than as one state.
//! One marker for all of it would need a variant per reachable
//! combination, which is the product of the three and grows
//! every time one of them gains an option
//!
//! - **Kind** — `Once`, then `Repeat` or `Rate`
//! - **Deadline** — `Open` until `for_duration` or `until` sets
//!   one, then `Set`
//! - **Count** — `Open` until `count` sets one, then `Set`
//!
//! ## Why the bound axes are two rather than one
//! A count and a deadline can both be given, and whichever is
//! reached first ends the series. Sharing one marker between
//! them would make setting either one close the other

/// Closes the traits in here to the outside world
///
/// The same trick [`Task`](crate::Task) uses, and for the same
/// reason. `Repeatable` has to be nameable from outside the
/// crate, because it appears in the bounds on public methods,
/// but implementing it out there would unlock the bound setters
/// on a state that has no meaning for them
pub(crate) mod sealed {
    /// Implemented for every marker this crate defines
    pub trait Sealed {}
}

/// No kind has been chosen, so the task runs once
///
/// Where every chain starts, and the only state `repeat` and
/// `at_rate` can be reached from — which is what makes the two
/// of them mutually exclusive without a runtime check
pub struct Once;

/// Runs again when the last run finishes, one at a time
///
/// The state `every` belongs to, because a gap between runs
/// only means anything where runs don't overlap
pub struct Repeat;

/// Starts a run on the period whatever the last one is doing
///
/// Has no `every` of its own: the period was given to `at_rate`
/// and a schedule has no second interval to set
pub struct Rate;

/// This axis hasn't been set, so its setter is available
pub struct Open;

/// This axis has been set, or `after` closed it
///
/// #### Note
/// `after` closes both bound axes without touching the kind.
/// That is deliberate rather than incidental — it is what makes
/// `.repeat().after(d).every(gap)` legal while
/// `.repeat().after(d).count(10)` is not
pub struct Set;

impl sealed::Sealed for Once {}
impl sealed::Sealed for Repeat {}
impl sealed::Sealed for Rate {}
impl sealed::Sealed for Open {}
impl sealed::Sealed for Set {}

/// A kind that a bound means something for
///
/// `for_duration`, `until` and `count` are all bounded on this,
/// so a bound on a one shot doesn't compile. A `.count(10)` on
/// something that was only ever going to run once would have to
/// either do nothing or lie, and refusing to build is better
/// than either
///
/// #### Note
/// `private_bounds` is allowed here for the same reason it is
/// on `Task`: the supertrait is deliberately unnameable from
/// outside, which is the whole of how the sealing works
#[allow(private_bounds)]
pub trait Repeatable: sealed::Sealed {}

impl Repeatable for Repeat {}
impl Repeatable for Rate {}
