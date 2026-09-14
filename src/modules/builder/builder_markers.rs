//! # Builder Markers
//! The type level states a `TaskBuilder` moves through, which
//! make a chain that doesn't make sense fail to compile
//!
//! A builder tracks four things separately:
//!
//! - **Kind** — `Once`, then `Repeat` or `Rate`
//! - **Deadline** — `Open` until `for_duration` or `until`,
//!   then `Set`
//! - **Count** — `Open` until `count`, then `Set`. Each state
//!   that has a count opens it afresh
//! - **Wiring** — `NoWait`, or what starts each run: gives with
//!   `WaitFor<T>`, a whole set with `ReceiveAll<H>`, or any of a
//!   set with `ReceiveAny<H>`

use std::marker::PhantomData;

/// Stops the traits in here being implemented outside the crate
pub(crate) mod sealed {
    /// Implemented for every marker this crate defines
    pub trait Sealed {}
}

/// No kind chosen yet, so the task runs once
///
/// The only state `repeat`, `at_rate`, `wait_for`, `receive` and
/// `receive_any` can be called from
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

/// Runs as soon as it is spawned, the way every task does unless
/// told to wait
pub struct NoWait;

/// Waits for a give of `T` before each run, or each series
pub struct WaitFor<T>(PhantomData<fn(T)>);

/// Waits for every task in the set `H` to publish before each run,
/// or each series
pub struct ReceiveAll<H>(PhantomData<fn(H)>);

/// Starts a run, or a series, with each output of any task in the
/// set `H`
pub struct ReceiveAny<H>(PhantomData<fn(H)>);

impl sealed::Sealed for Once {}
impl sealed::Sealed for Repeat {}
impl sealed::Sealed for Rate {}
impl sealed::Sealed for Open {}
impl sealed::Sealed for Set {}
impl sealed::Sealed for NoWait {}
impl<T> sealed::Sealed for WaitFor<T> {}
impl<H> sealed::Sealed for ReceiveAll<H> {}
impl<H> sealed::Sealed for ReceiveAny<H> {}

/// A kind a bound can be put on
///
/// Bounds only exist on repeats, so a `count` on a one shot
/// doesn't compile
#[allow(private_bounds)]
pub trait Repeatable: sealed::Sealed {}

impl Repeatable for Repeat {}
impl Repeatable for Rate {}

/// What starts a task's runs, and what the builder carries for it
#[allow(private_bounds)]
pub trait Wiring: sealed::Sealed {
    /// What the builder holds until spawn: the set a receive links
    /// to, or nothing
    #[doc(hidden)]
    type Link;

    /// The count axis once a kind is chosen
    ///
    /// Unchanged for a task that doesn't wait. A wait state had a
    /// count of its own, so choosing a kind opens a fresh one
    #[doc(hidden)]
    type AfterKind<C>;
}

impl Wiring for NoWait {
    type Link = ();
    type AfterKind<C> = C;
}

impl<T> Wiring for WaitFor<T> {
    type Link = ();
    type AfterKind<C> = Open;
}

impl<H> Wiring for ReceiveAll<H> {
    type Link = H;
    type AfterKind<C> = Open;
}

impl<H> Wiring for ReceiveAny<H> {
    type Link = H;
    type AfterKind<C> = Open;
}

/// A wiring where something arriving starts each run, so a count of
/// arrivals and a kind can be chained after it
#[allow(private_bounds)]
pub trait Waits: Wiring {}

impl<T> Waits for WaitFor<T> {}
impl<H> Waits for ReceiveAll<H> {}
impl<H> Waits for ReceiveAny<H> {}
