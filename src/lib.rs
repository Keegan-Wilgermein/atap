pub(crate) mod constants;
pub mod runtime;
pub(crate) mod executor;
pub(crate) mod reactor;

mod modules {
    pub(crate) mod event_desc;
    pub(crate) mod int_check;
    pub(crate) mod kevent;
    pub(crate) mod kqueue;
    pub(crate) mod task_handle;
    pub(crate) mod thread_policy;
    pub(crate) mod task_data;
    pub(crate) mod erased_task;
    pub(crate) mod mapping;
    pub(crate) mod task_state;
    pub(crate) mod task_table;
    pub mod errors;
}

mod futures {
    pub mod sleep;
    pub(crate) mod sleep_task;
    pub(crate) mod task;
}

// Re-exports
pub use modules::errors::RuntimeError;
pub use modules::task_handle::TaskHandle;
pub use futures::task::Task;
pub use futures::sleep::Sleep;
pub use futures::sleep_task::SleepTask;
pub use runtime::Runtime;
pub use modules::event_desc::EventDesc;
