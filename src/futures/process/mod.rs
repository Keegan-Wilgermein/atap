//! # Process
//! Tasks that run other programs, and the types they hand back

pub mod exit_status;
pub mod process;
pub mod process_task;

pub use exit_status::{ExitStatus, ProcessOutput};
pub use process::Process;
pub use process_task::{OutputTask, StatusTask};
