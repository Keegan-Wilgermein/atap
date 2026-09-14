//! # Handle Kind
//! What a handle can do beyond reading its task: nothing more for
//! a plain handle, and giving input for a handle to a task that
//! waits

use crate::{executor, modules::mailbox::Mailbox};
use std::{marker::PhantomData, sync::Arc};

/// Stops `HandleKind` being implemented outside the crate
pub(crate) mod sealed {
    /// Implemented for every kind of handle this crate hands out
    pub trait Sealed {}
}

/// What kind of task a handle points at, and what that lets the
/// handle do
#[allow(private_bounds, private_interfaces)]
pub trait HandleKind: sealed::Sealed + 'static {
    /// What the handle carries beside the task's id
    #[doc(hidden)]
    type Extra: Send + Sync;

    /// Records another handle carrying the same extra
    #[doc(hidden)]
    fn cloned(extra: &Self::Extra) -> Self::Extra;

    /// Records that a handle carrying this extra is gone
    #[doc(hidden)]
    fn dropped(id: usize, extra: &Self::Extra);

    /// The extra of a handle to no task at all
    #[doc(hidden)]
    fn detached() -> Self::Extra;
}

/// A handle that reads its task and nothing more
pub struct Plain;

/// A handle to a task spawned with `wait_for`, which can give it
/// the value its next run is handed
///
/// `T` is what the task waits for
pub struct Waiting<T>(PhantomData<fn(T)>);

impl sealed::Sealed for Plain {}
impl<T> sealed::Sealed for Waiting<T> {}

impl HandleKind for Plain {
    type Extra = ();

    #[inline(always)]
    fn cloned(_extra: &Self::Extra) -> Self::Extra {}

    #[inline(always)]
    fn dropped(_id: usize, _extra: &Self::Extra) {}

    #[inline(always)]
    fn detached() -> Self::Extra {}
}

impl<T> HandleKind for Waiting<T>
where
    T: Send + 'static,
{
    type Extra = Arc<Mailbox<T>>;

    /// Another handle that can give
    fn cloned(extra: &Self::Extra) -> Self::Extra {
        extra.gate().add_giver();

        Arc::clone(extra)
    }

    /// Lets the task go if that was the last handle that could give
    /// to it
    fn dropped(id: usize, extra: &Self::Extra) {
        if extra.gate().drop_giver() {
            executor::abandon(id);
        }
    }

    fn detached() -> Self::Extra {
        Arc::new(Mailbox::detached())
    }
}
