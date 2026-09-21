//! # atap
//! Any Time Any Place: an async runtime for macOS
//!
//! There is no `async`, no `await`, no `Future` and no `Pin`. A
//! task is an ordinary value that does nothing until it is run, so
//! any function can make one. Waiting tasks hold
//! no thread, and values are passed between tasks by the runtime
//! rather than by hand.
//!
//! ## Starting
//! [`Runtime::init`] comes first, and a spawn before it settles
//! with [`RuntimeError::NotInitialised`]. [`Runtime::builder`]
//! starts it with sizes of your own.
//!
//! ```no_run
//! use atap::{Runtime, RuntimeError, compute::Compute};
//!
//! # fn main() -> Result<(), RuntimeError> {
//! Runtime::init()?;
//!
//! let doubling = Runtime::task(Compute::compute(|()| 21 * 2)).spawn();
//!
//! assert_eq!(doubling.join()?, 42);
//! # Ok(())
//! # }
//! ```
//!
//! ## Running a task
//! A task is built inert, then run one of two ways. [`Runtime::block`]
//! runs it on the calling thread and hands back its output.
//! [`Runtime::task`] puts it on the pool and hands back a
//! [`TaskHandle`].
//!
//! A handle is a listener rather than the task itself: `join` reads
//! the output and leaves it, `take` moves it out once, `try_join`
//! looks without waiting, and `cancel` ends it for every listener.
//! Cloning a handle makes another listener, and dropping every
//! handle destroys the result.
//!
//! ## Settings
//! Settings are builder calls, and entering a state opens the calls
//! that belong to it, so a repeat's settings only exist once a
//! repeat has been asked for.
//!
//! ```no_run
//! use atap::{Runtime, RuntimeError, compute::Compute};
//! use std::time::Duration;
//!
//! # fn main() -> Result<(), RuntimeError> {
//! let ticking = Runtime::task(Compute::compute(|()| 1))
//!     .priority(200)
//!     .timeout(Duration::from_secs(1))
//!     .repeat()
//!     .every(Duration::from_millis(100))
//!     .count(10)
//!     .spawn();
//! # Ok(())
//! # }
//! ```
//!
//! ## Passing data between tasks
//! A task can wait for a value with
//! `wait_for` and be handed one with `give`. It can take another
//! task's output with `receive`, or be pointed at whoever needs its
//! own with `give_to`. [`Runtime::join_first`] races a set and
//! settles the losers by [`JoinPolicy`]. A [`channel`] carries
//! values between threads and tasks in order.
//!
//! ```no_run
//! use atap::{Runtime, RuntimeError, compute::Compute};
//!
//! # fn main() -> Result<(), RuntimeError> {
//! let doubler = Runtime::task(Compute::compute(|value: u64| value * 2))
//!     .wait_for::<u64>()
//!     .spawn();
//!
//! doubler.give(21)?;
//!
//! assert_eq!(doubler.join()?, 42);
//! # Ok(())
//! # }
//! ```
//!
//! ## Types of `Task`
//! | Module | What it runs |
//! |---|---|
//! | [`compute`] | closures on a worker |
//! | [`sleep`] | waits, spun for accuracy or left to the kernel |
//! | [`fs`] | reads, writes, listings, open files and watches |
//! | [`process`] | other programs, run or held open |
//! | [`signal`] | signals waited for and sent |
//! | [`channel`] | values between threads and tasks |
//! | [`tcp`], [`udp`], [`unix`] | sockets |
//! | [`tls`] | TCP with a handshake, behind the `tls` feature |
//!
//! ## Work splitting
//! Tasks that block are kept apart from tasks that do not. A file
//! read or a relaxed sleep goes to a sleep thread, so it never ties
//! up a worker, and a task spawned from inside another stays near
//! the thread that made it. The pool settles around a target rather
//! than a fixed size, growing under load and reaping what it stops
//! needing.
//!
//! ## Failure recovery
//! The pool is supervised. A worker or sleep thread that dies has
//! its queued work put back and is replaced, and the manager is
//! restarted if it falls over. Tasks that can never run again
//! settle with a reason rather than waiting forever.
//! [`Runtime::pool`] and [`Runtime::status`] say what the runtime is
//! doing at any moment.
//!
//! ## macOS only
//! This is built on kqueue and other Apple interfaces, and a build
//! anywhere else stops at a `compile_error!`.

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
    pub(crate) mod tuning;
    pub(crate) mod waiter;
    pub(crate) mod wake_target;
    pub(crate) mod worker;
    pub(crate) mod worker_pool;
    pub(crate) mod worker_state;
    pub mod worker_stats;
}

mod futures {
    pub mod channel;
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
    pub use crate::channel::Channel;
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
        NoWait, Once, Open, Rate, ReceiveAll, ReceiveAny, Repeat, Repeatable, RuntimeBuilder, Set,
        TaskBuilder, Unset, WaitFor, Waits, Wiring,
    };
    pub use crate::modules::handle_kind::{HandleKind, Plain, Waiting};
    pub use crate::modules::handle_set::HandleSet;
    pub use crate::modules::input::{Ignore, Receives, Standalone, Use};
    pub use crate::modules::merge_set::MergeSet;
}

/// Handing values between threads and tasks
pub mod channel {
    pub use crate::futures::channel::{
        Bounded, BoundedSender, Channel, ChannelRecvTask, ChannelSendTask, Receiver, Sender,
        Unbounded,
    };
}

/// Tasks that run the program's own closures
pub mod compute {
    pub use crate::futures::compute::{Compute, ComputeTask};
}

/// Tasks that read, write and watch the filesystem
pub mod fs {
    pub use crate::futures::file::{
        Change, CopyTask, DirEntry, File, FileKind, FileMetadataTask, FileOpTask, FileReadTask,
        FileWriteTask, LockKind, Metadata, MetadataTask, OpenFile, OpenTask, PathBufTask, PathTask,
        ReadDirTask, ReadTask, WatchTask, WriteTask,
    };
}

/// What every socket family shares: addresses, and the send and
/// receive tasks a byte stream hands out
pub mod net {
    pub use crate::futures::net::{FinishTask, NetAddress, RecvTask, SendTask};
}

/// Tasks that run other programs
pub mod process {
    pub use crate::futures::process::{
        ChildOutput, ChildSignalTask, ChildStdin, ChildWaitTask, ExitStatus, OutputTask, Process,
        ProcessOutput, RunningChild, SpawnTask, StatusTask,
    };
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
    pub use crate::futures::udp::{
        BindTask, RecvFromTask, SendToTask, Udp, UdpConnectTask, UdpRecvTask, UdpSendTask,
        UdpSocket,
    };
}

/// Tasks that talk to other programs on this machine over Unix
/// sockets
pub mod unix {
    pub use crate::futures::unix::{
        Credentials, Unix, UnixAcceptTask, UnixBindTask, UnixConnectTask, UnixConnection,
        UnixDatagram, UnixListenTask, UnixListener, UnixPairTask, UnixRecvFromTask, UnixSendToTask,
    };
}
