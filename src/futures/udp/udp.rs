//! # UDP
//! The constructor every UDP task is started from

use crate::futures::{net::address::NetAddress, udp::udp_task::BindTask};

/// Sends and receives datagrams over UDP
///
/// It doesn't implement `Task`, so a method has to be called on
/// it to get something that does
///
/// ## Behaviour
/// [`Udp::bind`] hands back a [`UdpSocket`], and its methods build
/// the tasks that send and receive on it
///
/// ```ignore
/// let socket = Runtime::block(Udp::bind("127.0.0.1:0"))?;
///
/// Runtime::block(socket.send_to("127.0.0.1:9000", b"ping".as_slice()))?;
/// let (reply, from) = Runtime::block(socket.recv_from().timeout(Duration::from_secs(1)))?;
/// ```
///
/// ## Waiting
/// A spawned receive waiting for a datagram holds no thread, the
/// same as a TCP one. A blocking call waits on the calling thread,
/// and can't be cancelled, which is what `.timeout()` is for
///
/// #### Note
/// A socket task still waiting on the network when the runtime
/// shuts down is written off, and reads [`RuntimeError::TaskFailed`]
///
/// [`UdpSocket`]: crate::UdpSocket
/// [`RuntimeError::TaskFailed`]: crate::RuntimeError::TaskFailed
pub struct Udp;

impl Udp {
    /// Opens a UDP socket bound to `addr`
    ///
    /// ## Behaviour
    /// `addr` is a `"host:port"` or a `SocketAddr`. Port 0 picks any
    /// free port, and [`UdpSocket::local_addr`] says which.
    /// `0.0.0.0` receives on every IPv4 interface
    ///
    /// ## Returns
    /// The socket. It can only send to addresses in its own
    /// family, so one bound to an IPv4 address sends to IPv4 ones
    ///
    /// [`UdpSocket::local_addr`]: crate::UdpSocket::local_addr
    pub fn bind(addr: impl NetAddress) -> BindTask {
        BindTask::new(addr.target())
    }
}
