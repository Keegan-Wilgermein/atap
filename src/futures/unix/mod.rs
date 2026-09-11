//! # Unix
//! Tasks that talk to other programs on this machine over Unix
//! sockets, both the stream kind and the datagram kind

pub(crate) mod path;
pub mod unix;
pub mod unix_socket;
pub mod unix_task;

pub use unix::Unix;
pub use unix_socket::{UnixConnection, UnixDatagram, UnixListener};
pub use unix_task::{
    UnixAcceptTask, UnixBindTask, UnixConnectTask, UnixListenTask, UnixRecvFromTask,
    UnixSendToTask,
};
