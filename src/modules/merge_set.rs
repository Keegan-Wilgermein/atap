//! # Merge Set
//! Handles a task can receive from one at a time, of any types that
//! turn into what the task takes

use crate::{
    executor::Executor,
    futures::task::Nothing,
    modules::{
        forward::{Forwarder, cloned_into, nothing},
        handle_kind::{HandleKind, Waiting},
        handle_set::sealed,
        input::{Ignore, Use},
        task_handle::TaskHandle,
    },
};

/// Handles a task can receive from one at a time
///
/// Each output of any task in the set starts the receiving task,
/// turned into its input `V` with `Into`. A task that takes
/// [`Nothing`] is only started. `M` is worked out by the compiler
#[diagnostic::on_unimplemented(
    message = "`{Self}` can't be received one at a time by a task that takes `{V}`",
    label = "not mergeable into `{V}`",
    note = "every handle's output has to turn into the task's input with `Into`, and a set is a handle, a tuple of up to 12 sets, an array of sets, or a `Vec` of sets"
)]
#[allow(private_bounds, private_interfaces)]
pub trait MergeSet<V, M>: sealed::Sealed + Send + Sized + 'static {
    /// What each give to the receiving task carries
    #[doc(hidden)]
    type Given: Send + 'static;

    /// Registers every task in the set to give to `target`
    ///
    /// Each handle's claim on its task is kept in `held`, as a plain
    /// claim, until the receiving task is finished
    #[doc(hidden)]
    fn link(self, target: &TaskHandle<(), Waiting<Self::Given>>, held: &mut Vec<Box<dyn Send>>);
}

impl<U, V, W> MergeSet<V, Use> for TaskHandle<U, W>
where
    U: Clone + Into<V> + Send + 'static,
    V: Send + 'static,
    W: HandleKind,
{
    type Given = V;

    fn link(self, target: &TaskHandle<(), Waiting<V>>, held: &mut Vec<Box<dyn Send>>) {
        Executor::forward(
            self.id(),
            Box::new(Forwarder::new(target.retyped(), cloned_into::<U, V>)),
        );

        held.push(Box::new(self.into_plain()));
    }
}

impl<U, W> MergeSet<Nothing, Ignore> for TaskHandle<U, W>
where
    U: Send + 'static,
    W: HandleKind,
{
    type Given = ();

    fn link(self, target: &TaskHandle<(), Waiting<()>>, held: &mut Vec<Box<dyn Send>>) {
        Executor::forward(
            self.id(),
            Box::new(Forwarder::new(target.retyped(), nothing)),
        );

        held.push(Box::new(self.into_plain()));
    }
}

impl<V, M, H> MergeSet<V, M> for Vec<H>
where
    H: MergeSet<V, M>,
{
    type Given = H::Given;

    fn link(self, target: &TaskHandle<(), Waiting<Self::Given>>, held: &mut Vec<Box<dyn Send>>) {
        for set in self {
            set.link(target, held);
        }
    }
}

impl<V, M, H, const N: usize> MergeSet<V, M> for [H; N]
where
    H: MergeSet<V, M>,
{
    type Given = H::Given;

    fn link(self, target: &TaskHandle<(), Waiting<Self::Given>>, held: &mut Vec<Box<dyn Send>>) {
        for set in self {
            set.link(target, held);
        }
    }
}
