//! # File
//! Tasks that read and write the filesystem, and the types
//! they hand back
//!
//! Split by what a reader came for. [`file`] is the API — every
//! task starts at a constructor there, and the reasons a call
//! behaves the way it does are written on it. [`file_task`] is
//! what those constructors return and what actually runs, which
//! is where the syscalls and their loops live. [`metadata`] is
//! the one output that needed a type of its own

pub mod file;
pub mod file_task;
pub mod metadata;

pub use file::File;
pub use file_task::{MetadataTask, PathTask, ReadDirTask, ReadTask, WriteTask};
pub use metadata::{FileKind, Metadata};
