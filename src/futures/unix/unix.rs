//! # Unix
//! The constructors every Unix socket task is started from

use crate::futures::unix::unix_task::{UnixBindTask, UnixConnectTask, UnixListenTask};
use std::path::Path;

/// Talks to other programs on this machine over Unix sockets
///
/// It doesn't implement `Task`, so a method has to be called on
/// it to get something that does
///
/// ## Behaviour
/// A Unix socket is addressed by a path rather than a host and
/// port. [`Unix::connect`] and [`Unix::listen`] are the stream
/// kind, which behaves exactly like TCP and hands out the same
/// send and receive tasks. [`Unix::bind`] is the datagram kind,
/// which behaves like UDP
///
/// ```ignore
/// let listener = Runtime::block(Unix::listen("/tmp/app.sock"))?;
/// let conn = Runtime::block(Unix::connect("/tmp/app.sock"))?;
/// let served = Runtime::block(listener.accept())?;
/// ```
///
/// ## Paths
/// A path can be at most 103 bytes, which is all the kernel has
/// room for. A longer one, an empty one, or one with a zero byte
/// in it gives [`RuntimeError::BadPath`]
///
/// A socket bound to a path makes a file there, and removes it
/// again once the last handle to the socket goes
///
/// ## Waiting
/// A spawned task waiting on a Unix socket holds no thread, the
/// same as a TCP one
///
/// [`RuntimeError::BadPath`]: crate::RuntimeError::BadPath
pub struct Unix;

impl Unix {
    /// Opens a connection to the socket listening at `path`
    ///
    /// ## Returns
    /// The connection. Nothing at the path is
    /// `CheckError(Some(ENOENT))`, and a socket file left behind
    /// with nobody listening on it is `ECONNREFUSED`
    pub fn connect(path: impl AsRef<Path>) -> UnixConnectTask {
        UnixConnectTask::new(path.as_ref().to_path_buf())
    }

    /// Opens a socket that waits for connections at `path`
    ///
    /// ## Behaviour
    /// Makes a socket file at `path`, which is removed when the
    /// last handle to the listener goes
    ///
    /// ## Returns
    /// The listener. Something already at the path is
    /// `CheckError(Some(EADDRINUSE))`, including a socket file
    /// another program left behind, which has to be removed first
    pub fn listen(path: impl AsRef<Path>) -> UnixListenTask {
        UnixListenTask::new(path.as_ref().to_path_buf())
    }

    /// Opens a datagram socket bound to `path`
    ///
    /// ## Behaviour
    /// Makes a socket file at `path`, which is removed when the
    /// last handle to the socket goes
    ///
    /// ## Returns
    /// The socket. Something already at the path is
    /// `CheckError(Some(EADDRINUSE))`
    pub fn bind(path: impl AsRef<Path>) -> UnixBindTask {
        UnixBindTask::new(path.as_ref().to_path_buf())
    }
}
