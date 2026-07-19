//! # Runtime
//! 
//! `Runtime` manages every event called into it and returns
//! their results as they finish

use std::sync::mpsc::{self, Sender};

use libc::kqueue;
use crate::{Task, executor::Executor, reactor::Reactor, pending::Pending, modules::{int_check::IntCheck}};

pub struct Runtime {
    executor: Executor,
    reactor: Reactor,
    /// Clone this into each `Waker`
    /// so it can wake the exectutor thread
    waker_sender: Sender<i32>,
}

impl Runtime {
    /// Creates a new thread local runtime
    /// 
    /// Call this at the start of `main()`
    /// and at the beginning of every thread that you want to use with async
    pub fn new() -> Self {
        let kqueue = unsafe { kqueue() }.check();
        let (tx, rx) = mpsc::channel();

        Self {
            executor: Executor::new(kqueue, rx),
            reactor: Reactor::new(kqueue),
            waker_sender: tx,
        }
    }

    /// Normal blocking async call using `async` / `await` calls
    pub fn block_on<F>(&self, future: F)
    -> F::Output
    where
        F: Future,
    {
        let mut future = Box::pin(future);
        todo!()
    }

    /// Defers the execution of the passed function to the executor,
    /// returning a `Future` that can be manually checked
    /// whenever you feel like to see if the task has finished
    /// 
    /// Will never block the current thread
    pub fn whenever<T, F>(
        &self,
        function: F,
    ) -> Pending<T>
    where
        F: Fn() -> T,
    { 
        let task = Task::new(function);
        Pending::new()
    }
}
