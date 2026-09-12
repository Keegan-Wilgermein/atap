//! # File
//! Tasks that read and write the filesystem, and the types
//! they hand back

pub mod change;
pub mod file;
pub mod file_task;
pub mod metadata;
pub mod watch_task;

pub use change::Change;
pub use file::File;
pub use file_task::{MetadataTask, PathTask, ReadDirTask, ReadTask, WriteTask};
pub use metadata::{FileKind, Metadata};
pub use watch_task::WatchTask;
