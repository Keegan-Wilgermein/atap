//! # TLS
//! Tasks that talk over TLS, on top of TCP, and the connections
//! they hand back
//!
//! Only built with the `tls` feature

pub(crate) mod config;
pub mod connection;
pub(crate) mod fd_io;
pub(crate) mod handshake;
pub mod tls;
pub mod tls_task;

pub use connection::{TlsConnection, TlsListener};
pub use tls::Tls;
pub use tls_task::{TlsAcceptTask, TlsConnectTask, TlsListenTask, TlsRequestTask};
