//! # UDP socket
//! A bound UDP socket, and the tasks it starts

use crate::futures::{
    net::{address::NetAddress, socket::Fd},
    udp::udp_task::{RecvFromTask, SendToTask},
};
use std::{fmt, net::SocketAddr, sync::Arc};

/// What every copy of one socket shares
struct Datagrams {
    /// The socket, closed when the last copy goes
    fd: Fd,

    /// The address it is bound to
    local: SocketAddr,
}

/// A bound UDP socket
///
/// ## Behaviour
/// Comes back from [`Udp::bind`]. There is no connection: each
/// send names where it goes, and each receive says where it came
/// from. Cloning it is cheap and every clone is the same socket
///
/// ```ignore
/// let socket = Runtime::block(Udp::bind("0.0.0.0:0"))?;
///
/// Runtime::block(socket.send_to("10.0.0.5:9000", b"ping".as_slice()))?;
/// let (reply, from) = Runtime::block(socket.recv_from())?;
/// ```
///
/// ## Datagrams
/// Every send is one datagram and every receive takes one,
/// whole. Nothing is merged or split. A datagram can be lost,
/// arrive twice, or arrive out of order, and nothing here
/// notices: that is what UDP is
///
/// ## Closing
/// The socket closes once the last reference to it goes,
/// including a receive still waiting
///
/// #### Note
/// `.repeat()` on a receive keeps only the latest datagram in its
/// slot, so one that isn't taken before the next arrives is lost.
/// Receive in a loop instead
///
/// [`Udp::bind`]: crate::Udp::bind
#[derive(Clone)]
pub struct UdpSocket {
    socket: Arc<Datagrams>,
}

impl UdpSocket {
    /// Takes over a bound socket
    pub(crate) fn new(fd: Fd, local: SocketAddr) -> Self {
        Self {
            socket: Arc::new(Datagrams { fd, local }),
        }
    }

    /// The socket, for handing to a syscall
    #[inline(always)]
    pub(crate) fn fd(&self) -> libc::c_int {
        self.socket.fd.raw()
    }

    /// Sends `data` to `addr` as one datagram
    ///
    /// ## Behaviour
    /// `addr` is a `"host:port"` or a `SocketAddr`. A name is looked
    /// up when the task runs, and the first address it gives in the
    /// same family as this socket is the one sent to
    ///
    /// ## Returns
    /// The number of bytes sent, which is all of them
    ///
    /// #### Note
    /// A datagram too large for the network is refused rather than
    /// split, as `CheckError(Some(EMSGSIZE))`. On macOS the default
    /// limit is 9216 bytes, set by `net.inet.udp.maxdgram`
    pub fn send_to(&self, addr: impl NetAddress, data: impl Into<Arc<[u8]>>) -> SendToTask {
        SendToTask::new(self.clone(), addr.target(), data.into())
    }

    /// Receives the next datagram
    ///
    /// ## Behaviour
    /// Waits for one to arrive without holding a thread
    ///
    /// ## Returns
    /// The whole datagram, and the address it came from. An empty
    /// datagram is a real one, not the socket closing
    pub fn recv_from(&self) -> RecvFromTask {
        RecvFromTask::new(self.clone())
    }

    /// The address it is bound to
    ///
    /// Binding port 0 picks a free port, and this is where to find
    /// out which
    #[inline(always)]
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.local
    }

    /// Lets go of this handle on the socket
    ///
    /// ## Behaviour
    /// The socket closes once nothing else holds it. A receive
    /// still waiting on it runs to its end
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for UdpSocket {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UdpSocket")
            .field("local", &self.socket.local)
            .finish()
    }
}
