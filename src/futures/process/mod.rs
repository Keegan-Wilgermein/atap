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
//! There is no way to set a working directory or an
//! environment yet, and that is a decision rather than a gap.
//! They are two independent axes, so adding them as
//! constructors would mean four more of them on a facade that
//! currently has two — the combinatorial spread `File` avoided
//! by keeping its axes few. A builder would be the answer, but
//! `TaskBuilder` is the one chain in this crate and everything
//! else is a plain constructor, so a second one would be a new
//! shape rather than a new feature. Both slot in later without
//! changing either task type

pub mod exit_status;
pub mod process;
pub mod process_task;

pub use exit_status::{ExitStatus, ProcessOutput};
pub use process::Process;
pub use process_task::{OutputTask, StatusTask};
