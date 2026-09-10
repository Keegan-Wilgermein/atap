//! # atap

pub(crate) mod constants;
pub(crate) mod executor;
pub(crate) mod reactor;
pub mod runtime;

mod modules {
    pub(crate) mod address_lock;
    pub mod builder;
    pub(crate) mod erased_task;
    pub mod errors;
    pub(crate) mod event_desc;
    pub(crate) mod injector;
    pub mod join_policy;
    pub(crate) mod int_check;
    pub(crate) mod kevent;
    pub(crate) mod kqueue;
    pub(crate) mod mapping;
    pub mod pool_stats;
    pub mod runtime_status;
    pub(crate) mod series;
    pub(crate) mod sleep_thread;
    pub(crate) mod task_data;
    pub(crate) mod task_kind;
    pub(crate) mod task_setup;
    pub(crate) mod task_handle;
    pub mod task_state;
    pub(crate) mod task_table;
    pub(crate) mod waiter;
    pub(crate) mod wake_target;
    pub(crate) mod worker;
    pub(crate) mod worker_pool;
    pub(crate) mod worker_state;
    pub mod worker_stats;
}

mod futures {
    pub mod file;
    pub(crate) mod kernel_wait;
    pub mod process;
    pub mod sleep;
    pub(crate) mod sleep_task;
    pub(crate) mod task;
}

// Re-exports
pub use futures::file::{
    File, FileKind, Metadata, MetadataTask, PathTask, ReadDirTask, ReadTask, WriteTask,
};
pub use futures::process::{ExitStatus, OutputTask, Process, ProcessOutput, StatusTask};
pub use futures::sleep::Sleep;
pub use futures::sleep_task::SleepTask;
pub use futures::task::Task;
pub use modules::errors::RuntimeError;
pub use modules::join_policy::JoinPolicy;
pub use modules::event_desc::EventDesc;
pub use modules::pool_stats::PoolStats;
pub use modules::runtime_status::RuntimeStatus;
pub use modules::builder::{Once, Open, Rate, Repeat, Repeatable, Set, TaskBuilder};
pub use modules::task_handle::TaskHandle;
pub use modules::task_state::TaskState;
pub use modules::worker_stats::WorkerStats;
pub use constants::DEFAULT_PRIORITY;
pub use runtime::Runtime;
