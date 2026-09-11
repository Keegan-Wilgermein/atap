//! # TLS
//! The constructors every TLS task is started from

use crate::futures::{
    net::address::NetAddress,
    tls::tls_task::{TlsConnectTask, TlsListenTask, TlsRequestTask},
};
use std::{path::Path, sync::Arc};

/// Talks to other programs over TLS, on top of TCP
///
/// Needs the `tls` feature. It doesn't implement `Task`, so a
/// method has to be called on it to get something that does
///
/// ## Behaviour
/// Everything [`Tcp`] does, encrypted, with the other side's
/// certificate checked. A [`TlsConnection`] hands out the same send
/// and receive tasks as a TCP one
///
/// ```ignore
/// let reply = Runtime::block(
///     Tls::request(
///         "www.example.com:443",
///         b"GET / HTTP/1.0\r\nHost: www.example.com\r\n\r\n".as_slice(),
///     )
///     .timeout(Duration::from_secs(10)),
/// )?;
/// ```
///
/// ## Certificates
/// A server's certificate is checked by macOS against the
/// system's trust store, the same one Safari uses, revocation
/// included. `.trust(pem)` adds roots of your own on top, for a
/// private certificate authority
///
/// A certificate that doesn't check out gives
/// [`RuntimeError::BadCertificate`], and anything else that goes
/// wrong in the session [`RuntimeError::TlsFailed`]
///
/// ## Waiting
/// A spawned task waiting on the network holds no thread, the
/// same as a TCP one. A connect still asks for a sleep thread for
/// its steps, since checking a certificate can itself go to the
/// network
///
/// [`Tcp`]: crate::Tcp
/// [`TlsConnection`]: crate::TlsConnection
/// [`RuntimeError::BadCertificate`]: crate::RuntimeError::BadCertificate
/// [`RuntimeError::TlsFailed`]: crate::RuntimeError::TlsFailed
pub struct Tls;

impl Tls {
    /// Opens a TLS connection to `addr`
    ///
    /// ## Behaviour
    /// Connects over TCP, then runs the handshake. The server's
    /// certificate has to be for the host in `addr`, or for the
    /// name given to `.server_name()`
    ///
    /// ## Returns
    /// The connection, ready to send and receive on
    pub fn connect(addr: impl NetAddress) -> TlsConnectTask {
        TlsConnectTask::new(addr.target())
    }

    /// Connects to `addr` over TLS, sends `data`, and reads
    /// everything that comes back
    ///
    /// ## Behaviour
    /// The answer is read until the server closes the session,
    /// which is how HTTP/1.0 marks its end
    ///
    /// ## Returns
    /// Everything the server sent. [`RuntimeError::Closed`] instead
    /// if it dropped the connection without closing the session,
    /// since the answer may have been cut short
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn request(addr: impl NetAddress, data: impl Into<Arc<[u8]>>) -> TlsRequestTask {
        TlsRequestTask::new(addr.target(), data.into())
    }

    /// Opens a socket that waits for TLS connections on `addr`
    ///
    /// ## Behaviour
    /// `cert` is the certificate chain and `key` its private key,
    /// both PEM files, read when the task runs. Port 0 picks any
    /// free port
    ///
    /// ## Returns
    /// The listener. A file that can't be read keeps the kernel's
    /// reason, and one that doesn't parse, or a key that doesn't
    /// match, gives [`RuntimeError::BadCertificate`]
    ///
    /// [`RuntimeError::BadCertificate`]: crate::RuntimeError::BadCertificate
    pub fn listen(
        addr: impl NetAddress,
        cert: impl AsRef<Path>,
        key: impl AsRef<Path>,
    ) -> TlsListenTask {
        TlsListenTask::new(
            addr.target(),
            cert.as_ref().to_path_buf(),
            key.as_ref().to_path_buf(),
        )
    }
}
