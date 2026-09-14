//! # Handle Set
//! The handles a task can receive from all at once, of any types,
//! and the shape of the value the task is handed
//!
//! A single handle is the smallest set. Tuples, arrays and `Vec`s of
//! sets are sets too, so any shape can be built by nesting

use crate::{
    executor::Executor,
    modules::{
        gather::{Access, Gather, GatherLeaf, access},
        handle_kind::HandleKind,
        task_handle::TaskHandle,
    },
};
use std::sync::Arc;

/// Stops the set traits being implemented outside the crate
pub(crate) mod sealed {
    /// Implemented for every shape of set this crate knows
    pub trait Sealed {}
}

/// Handles a task can receive from all at once
///
/// The task is handed [`Output`](HandleSet::Output) once every task
/// in the set has published since its last run: the output itself
/// for a handle, a tuple for a tuple, an array for an array, and a
/// `Vec` for a `Vec`
#[diagnostic::on_unimplemented(
    message = "`{Self}` isn't a set of task handles",
    label = "not a set of handles",
    note = "a set is a handle, a tuple of up to 12 sets (nest them for more), an array of sets, or a `Vec` of sets"
)]
#[allow(private_bounds, private_interfaces)]
pub trait HandleSet: sealed::Sealed + Send + Sized + 'static {
    /// What a task receiving the set is handed
    type Output: Send + 'static;

    /// Where a round's values wait, shaped like the set
    #[doc(hidden)]
    type Slots: Send + 'static;

    /// Empty slots for this set
    #[doc(hidden)]
    fn slots(&self) -> Self::Slots;

    /// Whether every slot holds a value
    #[doc(hidden)]
    fn filled(slots: &Self::Slots) -> bool;

    /// Empties full slots into the value handed on
    #[doc(hidden)]
    fn assemble(slots: &mut Self::Slots) -> Self::Output;

    /// Registers every task in the set on the gather its values go to
    ///
    /// Each handle's claim on its task is kept in `held`, as a plain
    /// claim, until the receiving task is finished
    #[doc(hidden)]
    fn link<R>(
        self,
        gather: &Arc<Gather<R>>,
        access: Access<R::Slots, Self::Slots>,
        held: &mut Vec<Box<dyn Send>>,
    ) where
        R: HandleSet;
}

impl<U, W> sealed::Sealed for TaskHandle<U, W> where W: HandleKind {}
impl<H> sealed::Sealed for Vec<H> {}
impl<H, const N: usize> sealed::Sealed for [H; N] {}

impl<U, W> HandleSet for TaskHandle<U, W>
where
    U: Clone + Send + 'static,
    W: HandleKind,
{
    type Output = U;
    type Slots = Option<U>;

    #[inline(always)]
    fn slots(&self) -> Self::Slots {
        None
    }

    #[inline(always)]
    fn filled(slots: &Self::Slots) -> bool {
        slots.is_some()
    }

    fn assemble(slots: &mut Self::Slots) -> Self::Output {
        slots
            .take()
            .expect("a set is only assembled once every slot is full")
    }

    fn link<R>(
        self,
        gather: &Arc<Gather<R>>,
        access: Access<R::Slots, Self::Slots>,
        held: &mut Vec<Box<dyn Send>>,
    ) where
        R: HandleSet,
    {
        gather.add_leaf();

        Executor::forward(
            self.id(),
            Box::new(GatherLeaf::<R, U>::new(Arc::clone(gather), access)),
        );

        // A plain claim, so holding it never counts as a way to give
        held.push(Box::new(self.into_plain()));
    }
}

impl<H> HandleSet for Vec<H>
where
    H: HandleSet,
{
    type Output = Vec<H::Output>;
    type Slots = Vec<H::Slots>;

    fn slots(&self) -> Self::Slots {
        self.iter().map(H::slots).collect()
    }

    fn filled(slots: &Self::Slots) -> bool {
        slots.iter().all(H::filled)
    }

    fn assemble(slots: &mut Self::Slots) -> Self::Output {
        slots.iter_mut().map(H::assemble).collect()
    }

    fn link<R>(
        self,
        gather: &Arc<Gather<R>>,
        outer: Access<R::Slots, Self::Slots>,
        held: &mut Vec<Box<dyn Send>>,
    ) where
        R: HandleSet,
    {
        for (index, set) in self.into_iter().enumerate() {
            let outer = Arc::clone(&outer);

            set.link(
                gather,
                access(move |root: &mut R::Slots| &mut outer(root)[index]),
                held,
            );
        }
    }
}

impl<H, const N: usize> HandleSet for [H; N]
where
    H: HandleSet,
{
    type Output = [H::Output; N];
    type Slots = [H::Slots; N];

    fn slots(&self) -> Self::Slots {
        std::array::from_fn(|index| self[index].slots())
    }

    fn filled(slots: &Self::Slots) -> bool {
        slots.iter().all(H::filled)
    }

    fn assemble(slots: &mut Self::Slots) -> Self::Output {
        std::array::from_fn(|index| H::assemble(&mut slots[index]))
    }

    fn link<R>(
        self,
        gather: &Arc<Gather<R>>,
        outer: Access<R::Slots, Self::Slots>,
        held: &mut Vec<Box<dyn Send>>,
    ) where
        R: HandleSet,
    {
        for (index, set) in self.into_iter().enumerate() {
            let outer = Arc::clone(&outer);

            set.link(
                gather,
                access(move |root: &mut R::Slots| &mut outer(root)[index]),
                held,
            );
        }
    }
}
