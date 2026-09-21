//! # File
//! Tasks that read and write the filesystem, and the types
//! they hand back

pub mod change;
pub mod dir_entry;
pub mod file;
pub mod file_task;
pub mod metadata;
pub mod open_file;
pub mod watch_task;

pub use change::Change;
pub use dir_entry::DirEntry;
pub use file::File;
pub use file_task::{
    CopyTask, MetadataTask, PathBufTask, PathTask, ReadDirTask, ReadTask, WriteTask,
};
pub use metadata::{FileKind, Metadata};
pub use open_file::{
    FileMetadataTask, FileOpTask, FileReadTask, FileWriteTask, LockKind, OpenFile, OpenTask,
};
pub use watch_task::WatchTask;
