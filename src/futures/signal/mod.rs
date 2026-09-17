//! # Signal
//! Tasks that wait for signals sent to this program, and send
//! them to other processes

pub(crate) mod dispatch;
pub mod signal;
pub mod signal_task;

pub use signal::{Signal, SignalKind, SignalReleasePolicy};
pub use signal_task::{SendSignalTask, SignalTask};
