//! # Builder Markers
//! The type level states a `TaskBuilder` moves through, which
//! make a chain that doesn't make sense fail to compile
//!
//! A builder tracks three things separately:
//!
//! - **Kind** — `Once`, then `Repeat` or `Rate`
//! - **Deadline** — `Open` until `for_duration` or `until`,
//!   then `Set`
//! - **Count** — `Open` until `count`, then `Set`

/// Stops the traits in here being implemented outside the crate
pub(crate) mod sealed {
    /// Implemented for every marker this crate defines
    pub trait Sealed {}
}

/// No kind chosen yet, so the task runs once
///
/// The only state `repeat` and `at_rate` can be called from
pub struct Once;

/// Runs again when the last run finishes, one at a time
///
/// The only state `every` can be called from
pub struct Repeat;

/// Starts a run on the period, whatever the last one is doing
pub struct Rate;

/// This axis hasn't been set, so its setter is available
pub struct Open;

/// This axis has been set, or `after` closed it
///
/// `after` closes both bound axes, so bounds have to come
/// before it
pub struct Set;

impl sealed::Sealed for Once {}
impl sealed::Sealed for Repeat {}
impl sealed::Sealed for Rate {}
impl sealed::Sealed for Open {}
impl sealed::Sealed for Set {}

/// A kind a bound can be put on
///
/// Bounds only exist on repeats, so a `count` on a one shot
/// doesn't compile
#[allow(private_bounds)]
pub trait Repeatable: sealed::Sealed {}

impl Repeatable for Repeat {}
impl Repeatable for Rate {}
