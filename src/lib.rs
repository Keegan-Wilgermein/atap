//! # atap

#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(not(target_os = "macos"))]
compile_error!("atap only supports macOS");

pub(crate) mod constants;
pub(crate) mod executor;
pub(crate) mod reactor;
pub(crate) mod runtime;

mod modules {
    pub(crate) mod address_lock;
    pub mod builder;
    pub(crate) mod c_path;
    pub(crate) mod erased_task;
    pub mod errors;
    pub(crate) mod event_desc;
    pub(crate) mod exit_guard;
    pub(crate) mod extras;
    pub(crate) mod faults;
    pub(crate) mod fd;
    pub(crate) mod forward;
    pub(crate) mod gate;
    pub(crate) mod gated;
    pub(crate) mod gather;
    pub(crate) mod handle_kind;
    pub(crate) mod handle_set;
    pub(crate) mod handle_set_tuples;
    pub(crate) mod help;
    pub(crate) mod injector;
    pub(crate) mod input;
    pub(crate) mod int_check;
    pub mod join_policy;
    pub(crate) mod kevent;
    pub(crate) mod kqueue;
    pub(crate) mod mailbox;
    pub(crate) mod mapping;
    pub(crate) mod merge_set;
    pub(crate) mod merge_set_tuples;
    pub(crate) mod park;
    pub mod pool_stats;
    pub(crate) mod receivers;
    pub(crate) mod retried;
    pub mod runtime_status;
    pub(crate) mod series;
    pub(crate) mod sleep_thread;
    pub(crate) mod task_data;
    pub(crate) mod task_handle;
    pub(crate) mod task_kind;
    pub(crate) mod task_setup;
    pub mod task_state;
    pub(crate) mod task_table;
    pub(crate) mod thread_slot;
    pub(crate) mod waiter;
    pub(crate) mod wake_target;
    pub(crate) mod worker;
    pub(crate) mod worker_pool;
    pub(crate) mod worker_state;
    pub mod worker_stats;
}

mod futures {
    pub mod compute;
    pub mod file;
    pub(crate) mod kernel_wait;
    pub mod net;
    pub mod process;
    pub mod signal;
    pub mod sleep;
    pub(crate) mod sleep_task;
    pub(crate) mod task;
    pub mod tcp;
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    pub mod tls;
    pub mod udp;
    pub mod unix;
}

// Re-exports
pub use constants::DEFAULT_PRIORITY;
pub use futures::task::{Nothing, Task};
pub use modules::errors::RuntimeError;
pub use modules::join_policy::JoinPolicy;
pub use modules::pool_stats::PoolStats;
pub use modules::runtime_status::RuntimeStatus;
pub use modules::task_handle::TaskHandle;
pub use modules::task_state::TaskState;
pub use modules::worker_stats::WorkerStats;
pub use runtime::Runtime;

/// Everything most programs need
pub mod prelude {
    pub use crate::compute::Compute;
    pub use crate::fs::File;
    pub use crate::process::Process;
    pub use crate::signal::{Signal, SignalKind};
    pub use crate::sleep::{Sleep, SleepMode};
    pub use crate::tcp::Tcp;
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    pub use crate::tls::Tls;
    pub use crate::udp::Udp;
    pub use crate::unix::Unix;
    pub use crate::{JoinPolicy, Runtime, RuntimeError, TaskHandle};
}

/// The states and traits a `TaskBuilder` and a `TaskHandle` are
/// typed by
pub mod builder {
    pub use crate::modules::builder::{
        NoWait, Once, Open, Rate, ReceiveAll, ReceiveAny, Repeat, Repeatable, Set, TaskBuilder,
        Unset, WaitFor, Waits, Wiring,
    };
    pub use crate::modules::handle_kind::{HandleKind, Plain, Waiting};
    pub use crate::modules::handle_set::HandleSet;
    pub use crate::modules::input::{Ignore, Receives, Standalone, Use};
    pub use crate::modules::merge_set::MergeSet;
}

/// Tasks that run the program's own closures
pub mod compute {
    pub use crate::futures::compute::{Compute, ComputeTask};
}

/// Tasks that read, write and watch the filesystem
pub mod fs {
    pub use crate::futures::file::{
        Change, File, FileKind, Metadata, MetadataTask, PathTask, ReadDirTask, ReadTask, WatchTask,
        WriteTask,
    };
}

/// What every socket family shares: addresses, and the send and
/// receive tasks a byte stream hands out
pub mod net {
    pub use crate::futures::net::{NetAddress, RecvTask, SendTask};
}

/// Tasks that run other programs
pub mod process {
    pub use crate::futures::process::{ExitStatus, OutputTask, Process, ProcessOutput, StatusTask};
}

/// Tasks that wait for signals and send them
pub mod signal {
    pub use crate::futures::signal::{
        SendSignalTask, Signal, SignalKind, SignalReleasePolicy, SignalTask,
    };
}

/// Tasks that wait for a set time
pub mod sleep {
    pub use crate::futures::sleep::{Sleep, SleepMode};
    pub use crate::futures::sleep_task::SleepTask;
}

/// Tasks that talk over TCP
pub mod tcp {
    pub use crate::futures::tcp::{
        AcceptTask, ConnectTask, Connection, ListenTask, Listener, RequestTask, Tcp,
    };
}

/// Tasks that talk over TLS, on top of TCP
#[cfg(feature = "tls")]
#[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
pub mod tls {
    pub use crate::futures::tls::{
        Tls, TlsAcceptTask, TlsConnectTask, TlsConnection, TlsListenTask, TlsListener,
        TlsRequestTask,
    };
}

/// Tasks that send and receive datagrams over UDP
pub mod udp {
    pub use crate::futures::udp::{BindTask, RecvFromTask, SendToTask, Udp, UdpSocket};
}

/// Tasks that talk to other programs on this machine over Unix
/// sockets
pub mod unix {
    pub use crate::futures::unix::{
        Unix, UnixAcceptTask, UnixBindTask, UnixConnectTask, UnixConnection, UnixDatagram,
        UnixListenTask, UnixListener, UnixRecvFromTask, UnixSendToTask,
    };
}
