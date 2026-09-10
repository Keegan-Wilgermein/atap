//! # Process
//! Tasks that run other programs, and the types they hand back
//!
//! Split by what a reader came for. [`process`] is the API —
//! every task starts at a constructor there, and the reasons a
//! call behaves the way it does are written on it.
//! [`process_task`] is what those constructors return and what
//! actually runs, which is where the spawning and the waiting
//! live. [`exit_status`] is the outputs that needed types of
//! their own
//!
//! #### Note
//! Input, a working directory and an environment are set on the
//! task rather than chosen at a constructor. Three axes that
//! compose would be eight constructors on a facade that has two
//! — the combinatorial spread `File` avoided by keeping its
//! axes few — and they are three methods instead
//!
//! That leaves two kinds of chaining in the crate, which is
//! worth being precise about rather than apologising for.
//! `TaskBuilder` is the only chain that *schedules*: when a
//! task runs, how often, and under what bound. The setters here
//! *configure*, and configuration belongs to the task because
//! it is part of what the task is. Both being method chains no
//! more makes them the same shape than `as_millis` makes a
//! `Duration` a builder

pub mod exit_status;
pub mod process;
pub mod process_task;

pub use exit_status::{ExitStatus, ProcessOutput};
pub use process::Process;
pub use process_task::{OutputTask, StatusTask};
