//! # Input
//! What a task can run with when nothing gives it an input, and
//! which values a task spawned with `wait_for` can wait for

use crate::{
    futures::task::{Nothing, Task},
    modules::mailbox::Mailbox,
};

/// Stops the traits here being implemented or called outside the
/// crate
pub(crate) mod sealed {
    /// Implemented for every input a task can make up for itself
    pub trait Sealed {}

    /// Proof a call came from inside the crate
    ///
    /// Nothing outside can name it, so nothing outside can make up
    /// an input or hand one to a task
    pub struct Token(pub(crate) ());
}

/// An input a task can run with when nothing gives it one
///
/// `()` for a compute whose closure takes nothing, and
/// [`Nothing`] for every other task
#[allow(private_bounds, private_interfaces)]
pub trait Standalone: sealed::Sealed + Sized {
    /// The value a run gets when nothing gave it one
    #[doc(hidden)]
    fn standalone(token: sealed::Token) -> Self;
}

impl sealed::Sealed for () {}
impl sealed::Sealed for Nothing {}

impl Standalone for () {
    #[inline(always)]
    fn standalone(_token: sealed::Token) -> Self {}
}

impl Standalone for Nothing {
    #[inline(always)]
    fn standalone(_token: sealed::Token) -> Self {
        Nothing(())
    }
}

/// Marks a wait whose value the task runs with
pub struct Use;

/// Marks a wait whose value is dropped, for a task that takes no
/// input and only waits for the give
pub struct Ignore;

/// An input a task can be given values of `T` for
///
/// `M` is worked out by the compiler and never written: [`Use`]
/// when `T` is the input itself, and [`Ignore`] when the task
/// takes [`Nothing`] and only waits
#[diagnostic::on_unimplemented(
    message = "a task that takes `{Self}` can't wait for `{T}`",
    label = "waits for `{T}`",
    note = "a compute can take `_: {T}` to wait for it without using the value"
)]
#[allow(private_bounds, private_interfaces)]
pub trait Receives<T, M>: Sized {
    /// Hands the task the latest value given, for a run about to
    /// start
    #[doc(hidden)]
    fn deliver<F>(token: sealed::Token, task: &mut F, mailbox: &Mailbox<T>)
    where
        F: Task<Input = Self>;
}

impl<T> Receives<T, Ignore> for Nothing {
    #[inline(always)]
    fn deliver<F>(_token: sealed::Token, _task: &mut F, _mailbox: &Mailbox<T>)
    where
        F: Task<Input = Self>,
    {
    }
}

impl<V> Receives<V, Use> for V
where
    V: Clone,
{
    #[inline(always)]
    fn deliver<F>(_token: sealed::Token, task: &mut F, mailbox: &Mailbox<V>)
    where
        F: Task<Input = Self>,
    {
        if let Some(value) = mailbox.latest() {
            task.give(value);
        }
    }
}

/// The input a task makes up for itself
#[inline(always)]
pub(crate) fn standalone<V>() -> V
where
    V: Standalone,
{
    V::standalone(token())
}

/// Proof a call comes from inside the crate
#[inline(always)]
pub(crate) fn token() -> sealed::Token {
    sealed::Token(())
}
