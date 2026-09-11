//! # TCP
//! The constructors every TCP task is started from

use crate::futures::{
    net::address::NetAddress,
    tcp::tcp_task::{ConnectTask, ListenTask, RequestTask},
};
use std::sync::Arc;

/// Talks to other programs over TCP
///
/// It doesn't implement `Task`, so a method has to be called on
/// it to get something that does
///
/// ## Behaviour
/// A connection is a value. [`Tcp::connect`] and
/// [`Listener::accept`] hand one back, and its methods build the
/// tasks that send and receive on it. [`Tcp::request`] does the
/// whole exchange in one task, for when that is all there is
///
/// ```ignore
/// let reply = Runtime::block(
///     Tcp::request("example.com:80", b"GET / HTTP/1.0\r\n\r\n".as_slice())
///         .timeout(Duration::from_secs(5)),
/// )?;
/// ```
///
/// ## Waiting
/// A spawned task waiting on the network holds no thread. It
/// parks, and the runtime runs it again once its socket is ready,
/// so any number of them can wait at once
///
/// [`Runtime::block`] can't give its thread back, so a blocking
/// call waits on the calling thread instead. It can't be
/// cancelled either, which is what `.timeout()` is for
///
/// ## Cancellation
/// A cancelled task comes down at once, even while it waits on a
/// silent peer, and gives back [`RuntimeError::Cancelled`]. A
/// receive puts back what it had read. A send may already have
/// sent part of its data
///
/// #### Note
/// A socket task still waiting on the network when the runtime
/// shuts down is written off, and reads [`RuntimeError::TaskFailed`]
///
/// [`Listener::accept`]: crate::Listener::accept
/// [`Runtime::block`]: crate::Runtime::block
/// [`RuntimeError::Cancelled`]: crate::RuntimeError::Cancelled
/// [`RuntimeError::TaskFailed`]: crate::RuntimeError::TaskFailed
pub struct Tcp;

impl Tcp {
    /// Opens a connection to `addr`
    ///
    /// ## Behaviour
    /// `addr` is a `"host:port"` or a `SocketAddr`. A name is looked
    /// up when the task runs, and each address it gives is tried in
    /// turn
    ///
    /// ## Returns
    /// The connection. One that is refused comes back as
    /// `CheckError(Some(ECONNREFUSED))`, and a name that can't be
    /// found as [`RuntimeError::BadAddress`]
    ///
    /// [`RuntimeError::BadAddress`]: crate::RuntimeError::BadAddress
    pub fn connect(addr: impl NetAddress) -> ConnectTask {
        ConnectTask::new(addr.target())
    }

    /// Opens a socket that waits for connections on `addr`
    ///
    /// ## Behaviour
    /// Port 0 picks any free port, and
    /// [`Listener::local_addr`] says which
    ///
    /// ## Returns
    /// The listener
    ///
    /// [`Listener::local_addr`]: crate::Listener::local_addr
    pub fn listen(addr: impl NetAddress) -> ListenTask {
        ListenTask::new(addr.target())
    }

    /// Connects to `addr`, sends `data`, and reads everything that
    /// comes back
    ///
    /// ## Behaviour
    /// The answer is read until the other side closes the
    /// connection, which is how a protocol like HTTP/1.0 marks its
    /// end. The connection is closed once the task is done with it
    ///
    /// ## Returns
    /// Everything the other side sent
    ///
    /// #### Note
    /// The whole answer lands in memory at once. A server that
    /// never closes its end keeps this waiting until its timeout
    pub fn request(addr: impl NetAddress, data: impl Into<Arc<[u8]>>) -> RequestTask {
        RequestTask::new(addr.target(), data.into())
    }
}
