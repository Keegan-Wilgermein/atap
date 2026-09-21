//! # UDP socket
//! A bound UDP socket, and the tasks it starts

use crate::{
    RuntimeError,
    futures::{
        net::{
            address::{NetAddress, peer_of},
            socket::{get_option, set_option, set_raw},
        },
        udp::udp_task::{RecvFromTask, SendToTask, UdpConnectTask, UdpRecvTask, UdpSendTask},
    },
    modules::fd::Fd,
};
use std::{
    fmt,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
};

/// A membership request for an IPv4 group
fn v4_request(group: Ipv4Addr, interface: Ipv4Addr) -> libc::ip_mreq {
    libc::ip_mreq {
        imr_multiaddr: libc::in_addr {
            s_addr: u32::from(group).to_be(),
        },
        imr_interface: libc::in_addr {
            s_addr: u32::from(interface).to_be(),
        },
    }
}

/// A membership request for an IPv6 group
fn v6_request(group: Ipv6Addr, interface: u32) -> libc::ipv6_mreq {
    libc::ipv6_mreq {
        ipv6mr_multiaddr: libc::in6_addr {
            s6_addr: group.octets(),
        },
        ipv6mr_interface: interface,
    }
}

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
/// ```no_run
/// # use atap::{Runtime, udp::Udp};
/// # fn main() -> Result<(), atap::RuntimeError> {
/// let socket = Runtime::block(Udp::bind("0.0.0.0:0"))?;
///
/// Runtime::block(socket.send_to("10.0.0.5:9000", b"ping".as_slice()))?;
/// let (reply, from) = Runtime::block(socket.recv_from())?;
/// # Ok(())
/// # }
/// ```
///
/// ## Datagrams
/// Every send is one datagram and every receive takes one,
/// whole. Nothing is merged or split. A datagram can be lost,
/// arrive twice, or arrive out of order
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
/// [`Udp::bind`]: crate::udp::Udp::bind
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

    /// Fixes the one address this socket sends to and hears from
    ///
    /// ## Behaviour
    /// Afterwards [`UdpSocket::send`] and [`UdpSocket::recv`] need
    /// no address, and datagrams from anywhere else are dropped.
    /// Connecting again moves it to a new address
    ///
    /// ## Returns
    /// Nothing, once the kernel has taken the address
    pub fn connect(&self, addr: impl NetAddress) -> UdpConnectTask {
        UdpConnectTask::new(self.clone(), addr.target())
    }

    /// Sends `data` to the connected address as one datagram
    ///
    /// ## Returns
    /// The number of bytes sent. A socket that isn't connected
    /// gives `CheckError(Some(EDESTADDRREQ))`
    pub fn send(&self, data: impl Into<Arc<[u8]>>) -> UdpSendTask {
        UdpSendTask::new(self.clone(), data.into())
    }

    /// Receives the next datagram, which on a connected socket only
    /// comes from the connected address
    ///
    /// ## Returns
    /// The whole datagram
    pub fn recv(&self) -> UdpRecvTask {
        UdpRecvTask::new(self.clone())
    }

    /// The address this socket is connected to
    ///
    /// ## Returns
    /// `CheckError(Some(ENOTCONN))` if it isn't
    pub fn peer_addr(&self) -> Result<SocketAddr, RuntimeError> {
        peer_of(self.fd())
    }

    /// The address it is bound to
    ///
    /// Binding port 0 picks a free port, and this is where to find
    /// out which
    #[inline(always)]
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.local
    }

    /// Lets the socket send to a broadcast address, or stops it
    pub fn set_broadcast(&self, broadcast: bool) -> Result<(), RuntimeError> {
        set_option(
            self.fd(),
            libc::SOL_SOCKET,
            libc::SO_BROADCAST,
            broadcast as libc::c_int,
        )
    }

    /// How many hops a datagram sent from here may take
    pub fn set_ttl(&self, hops: u32) -> Result<(), RuntimeError> {
        let hops = libc::c_int::try_from(hops).map_err(|_| RuntimeError::BadArgument)?;

        let (level, option) = self.hop_option();

        set_option(self.fd(), level, option, hops)
    }

    /// How many hops a datagram sent from here may take
    pub fn ttl(&self) -> Result<u32, RuntimeError> {
        let (level, option) = self.hop_option();

        Ok(get_option(self.fd(), level, option)? as u32)
    }

    /// Where the hop limit lives for this socket's family
    fn hop_option(&self) -> (libc::c_int, libc::c_int) {
        match self.socket.local {
            SocketAddr::V4(_) => (libc::IPPROTO_IP, libc::IP_TTL),
            SocketAddr::V6(_) => (libc::IPPROTO_IPV6, libc::IPV6_UNICAST_HOPS),
        }
    }

    /// Starts hearing datagrams sent to an IPv4 multicast `group`,
    /// on the network `interface` has, or any with `UNSPECIFIED`
    pub fn join_multicast_v4(
        &self,
        group: Ipv4Addr,
        interface: Ipv4Addr,
    ) -> Result<(), RuntimeError> {
        set_raw(
            self.fd(),
            libc::IPPROTO_IP,
            libc::IP_ADD_MEMBERSHIP,
            &v4_request(group, interface),
        )
    }

    /// Stops hearing an IPv4 multicast `group`
    pub fn leave_multicast_v4(
        &self,
        group: Ipv4Addr,
        interface: Ipv4Addr,
    ) -> Result<(), RuntimeError> {
        set_raw(
            self.fd(),
            libc::IPPROTO_IP,
            libc::IP_DROP_MEMBERSHIP,
            &v4_request(group, interface),
        )
    }

    /// Starts hearing datagrams sent to an IPv6 multicast `group`,
    /// on the interface with index `interface`, or any with zero
    pub fn join_multicast_v6(&self, group: Ipv6Addr, interface: u32) -> Result<(), RuntimeError> {
        set_raw(
            self.fd(),
            libc::IPPROTO_IPV6,
            libc::IPV6_JOIN_GROUP,
            &v6_request(group, interface),
        )
    }

    /// Stops hearing an IPv6 multicast `group`
    pub fn leave_multicast_v6(&self, group: Ipv6Addr, interface: u32) -> Result<(), RuntimeError> {
        set_raw(
            self.fd(),
            libc::IPPROTO_IPV6,
            libc::IPV6_LEAVE_GROUP,
            &v6_request(group, interface),
        )
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
