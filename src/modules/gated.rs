//! # Gated
//! The wrapper a task spawned with `wait_for` runs as, which hands
//! each run the latest value given

use crate::modules::input::{Token, token};
use crate::{
    futures::task::{
        Nothing, Task,
        sealed::{self, Step},
    },
    modules::{
        input::{self, Receives},
        mailbox::Mailbox,
    },
};
use std::{marker::PhantomData, sync::Arc};

/// A task that waits for gives, and the mailbox they land in
///
/// `M` says whether a run is handed the value or only started by it
pub(crate) struct Gated<F, T, M> {
    /// The task itself
    inner: F,

    /// Where gives leave their value
    mailbox: Arc<Mailbox<T>>,

    /// How a value reaches the task, decided at spawn
    _marker: PhantomData<fn() -> M>,
}

impl<F, T, M> Gated<F, T, M> {
    /// Wraps `inner` around the mailbox its gives land in
    pub(crate) fn new(inner: F, mailbox: Arc<Mailbox<T>>) -> Self {
        Self {
            inner,
            mailbox,
            _marker: PhantomData,
        }
    }
}

impl<F, T, M> Clone for Gated<F, T, M>
where
    F: Clone,
{
    /// A copy sharing the same mailbox, for a schedule's runs
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            mailbox: Arc::clone(&self.mailbox),
            _marker: PhantomData,
        }
    }
}

impl<F, T, M> Drop for Gated<F, T, M> {
    /// Drops what was given once the task takes no more gives
    ///
    /// Only then, since a schedule's runs are copies of the task, and
    /// one of them ending isn't the task ending
    fn drop(&mut self) {
        if self.mailbox.gate().finished() {
            self.mailbox.clear();
        }
    }
}

impl<F, T, M> sealed::Sealed for Gated<F, T, M> {}

impl<F, T, M> Task for Gated<F, T, M>
where
    F: Task,
    F::Input: Receives<T, M>,
    T: Send + 'static,
    M: 'static,
{
    type Output = F::Output;
    type Input = Nothing;

    /// Runs the task with whatever it was last handed
    #[inline(always)]
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        self.inner.execute(token(), reactor_id, task_id)
    }

    /// Hands the task the latest value, then prepares it
    fn prepare(&mut self, _token: Token) {
        self.mailbox.gate().begin();

        <F::Input as Receives<T, M>>::deliver(input::token(), &mut self.inner, &self.mailbox);

        self.inner.prepare(token());
    }

    /// Whatever the task inside says
    #[inline(always)]
    fn blocking(&self, _token: Token) -> bool {
        self.inner.blocking(token())
    }

    #[inline(always)]
    fn step(&mut self, _token: Token, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        self.inner.step(token(), reactor_id, task_id)
    }
}
