//! # TCP
//! Tasks that talk over TCP, and the connections they hand
//! back

pub mod address;
pub mod connection;
pub mod tcp;
pub mod tcp_task;

pub use address::TcpAddress;
pub use connection::{Connection, Listener};
pub use tcp::Tcp;
pub use tcp_task::{AcceptTask, ConnectTask, ListenTask, RecvTask, RequestTask, SendTask};
