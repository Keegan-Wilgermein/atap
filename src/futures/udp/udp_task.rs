//! # UDP task
//! The tasks `Udp::bind` and a `UdpSocket` return, and
//! everything they do once run

use crate::modules::input::Token;
use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            address::{Target, family, from_raw, local_of, to_raw},
            datagram::{recv_datagram, send_datagram},
            socket::{Options, open},
            step::{Progress, settle, wait_on},
        },
        task::{
            Nothing, Task,
            sealed::{self, Step},
        },
        udp::udp_socket::UdpSocket,
    },
    modules::{int_check::IntCheck, park},
};
use std::{mem, net::SocketAddr, ptr, sync::Arc};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<UdpSocket, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () =
    assert!(mem::size_of::<Result<(Vec<u8>, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD);

/// Opens a UDP socket
///
/// ## Returns
/// The socket, bound to the first address that takes
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct BindTask {
    /// Where to bind
    target: Target,

    /// What the socket is set up with
    options: Options,
}

impl BindTask {
    /// Binds to `target`
    pub(crate) fn new(target: Target) -> Self {
        Self {
            target,
            options: Options::default(),
        }
    }

    /// Lets the socket send to a broadcast address
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn broadcast(mut self, broadcast: bool) -> Self {
        self.options.broadcast = broadcast;
        self
    }

    /// Lets other sockets bind the same port, each set up the same
    /// way, so all of them hear multicast traffic to it
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn reuse_port(mut self, reuse: bool) -> Self {
        self.options.reuse_port = reuse;
        self
    }

    /// Takes only IPv6 traffic on an IPv6 address
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn v6_only(mut self, only: bool) -> Self {
        self.options.v6_only = only;
        self
    }

    /// Binds
    fn bind(&self) -> Result<UdpSocket, RuntimeError> {
        let found = self.target.resolve()?;

        let mut failure = RuntimeError::BadAddress;

        for addr in found {
            match bind_one(&addr, &self.options) {
                Ok(socket) => return Ok(socket),
                Err(error) => failure = error,
            }
        }

        Err(failure)
    }
}

/// Binds a fresh socket to `addr`
fn bind_one(addr: &SocketAddr, options: &Options) -> Result<UdpSocket, RuntimeError> {
    let fd = open(family(addr), libc::SOCK_DGRAM)?;

    options.apply(fd.raw(), addr.is_ipv6())?;
    let (raw, len) = to_raw(addr);

    unsafe {
        libc::bind(
            fd.raw(),
            (&raw as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
            len,
        )
    }
    .check()?;

    // Port 0 was a free one picked by the kernel, so this is the
    // only way to know which
    let local = local_of(fd.raw())?;

    Ok(UdpSocket::new(fd, local))
}

/// Sends one datagram
///
/// ## Returns
/// The number of bytes sent, which is all of them
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct SendToTask {
    /// What to send from
    socket: UdpSocket,

    /// Where to send
    target: Target,

    /// What to send
    data: Arc<[u8]>,

    /// Where this run is sending, once it has been looked up
    dest: Progress<Option<SocketAddr>>,
}

impl SendToTask {
    /// Sends `data` from `socket` to `target`
    pub(crate) fn new(socket: UdpSocket, target: Target, data: Arc<[u8]>) -> Self {
        Self {
            socket,
            target,
            data,
            dest: Progress::default(),
        }
    }

    /// Sends, if the socket has room
    fn advance(&mut self) -> Result<Step<Result<usize, RuntimeError>>, RuntimeError> {
        let dest = match self.dest.0 {
            Some(dest) => dest,
            None => {
                let dest = pick(&self.target, self.socket.local_addr())?;
                self.dest.0 = Some(dest);

                dest
            }
        };

        let fd = self.socket.fd();
        let (raw, len) = to_raw(&dest);

        let sent = send_datagram(
            fd,
            &self.data,
            (&raw as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
            len,
        )?;

        match sent {
            Some(sent) => Ok(Step::Done(Ok(sent))),
            None => wait_on(fd, libc::EVFILT_WRITE),
        }
    }
}

/// The first address `target` names that a socket bound to
/// `local` can send to
///
/// ## Returns
/// `BadAddress` when none of them are in the socket's family
fn pick(target: &Target, local: SocketAddr) -> Result<SocketAddr, RuntimeError> {
    target
        .resolve()?
        .into_iter()
        .find(|addr| addr.is_ipv4() == local.is_ipv4())
        .ok_or(RuntimeError::BadAddress)
}

/// Receives one datagram
///
/// ## Returns
/// The whole datagram, and the address it came from
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct RecvFromTask {
    /// Where to receive
    socket: UdpSocket,

    /// Whether the datagram is left for the next receive
    peek: bool,
}

impl RecvFromTask {
    /// Receives on `socket`
    pub(crate) fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            peek: false,
        }
    }

    /// Looks at the next datagram without taking it, so the next
    /// receive gets it again
    ///
    /// ## Returns
    /// The task
    pub fn peek(mut self) -> Self {
        self.peek = true;
        self
    }

    /// Takes a datagram if one is waiting
    fn advance(
        &mut self,
    ) -> Result<Step<Result<(Vec<u8>, SocketAddr), RuntimeError>>, RuntimeError> {
        let fd = self.socket.fd();

        match recv_datagram(fd, self.peek)? {
            Some((data, storage, _)) => {
                let from = from_raw(&storage).ok_or(RuntimeError::BadAddress)?;

                Ok(Step::Done(Ok((data, from))))
            }

            None => wait_on(fd, libc::EVFILT_READ),
        }
    }
}

/// Fixes the one address a socket sends to and hears from
///
/// ## Returns
/// Nothing, once the kernel has taken the address
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UdpConnectTask {
    /// The socket to fix
    socket: UdpSocket,

    /// Where to
    target: Target,
}

impl UdpConnectTask {
    pub(crate) fn new(socket: UdpSocket, target: Target) -> Self {
        Self { socket, target }
    }

    fn connect(&self) -> Result<(), RuntimeError> {
        let peer = pick(&self.target, self.socket.local_addr())?;
        let (raw, len) = to_raw(&peer);

        loop {
            let done = unsafe {
                libc::connect(
                    self.socket.fd(),
                    (&raw as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
                    len,
                )
            }
            .check();

            match done {
                Ok(_) => return Ok(()),
                Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}
                Err(error) => return Err(error),
            }
        }
    }
}

/// Sends one datagram to the address a socket is connected to
///
/// ## Returns
/// The number of bytes sent, which is all of them
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UdpSendTask {
    socket: UdpSocket,
    data: Arc<[u8]>,
}

impl UdpSendTask {
    pub(crate) fn new(socket: UdpSocket, data: Arc<[u8]>) -> Self {
        Self { socket, data }
    }

    fn advance(&mut self) -> Result<Step<Result<usize, RuntimeError>>, RuntimeError> {
        let fd = self.socket.fd();

        match send_datagram(fd, &self.data, ptr::null(), 0)? {
            Some(sent) => Ok(Step::Done(Ok(sent))),
            None => wait_on(fd, libc::EVFILT_WRITE),
        }
    }
}

/// Receives one datagram from the address a socket is connected to
///
/// ## Returns
/// The whole datagram
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UdpRecvTask {
    socket: UdpSocket,

    /// Whether the datagram is left for the next receive
    peek: bool,
}

impl UdpRecvTask {
    pub(crate) fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            peek: false,
        }
    }

    /// Looks at the next datagram without taking it, so the next
    /// receive gets it again
    ///
    /// ## Returns
    /// The task
    pub fn peek(mut self) -> Self {
        self.peek = true;
        self
    }

    fn advance(&mut self) -> Result<Step<Result<Vec<u8>, RuntimeError>>, RuntimeError> {
        let fd = self.socket.fd();

        match recv_datagram(fd, self.peek)? {
            Some((data, _, _)) => Ok(Step::Done(Ok(data))),
            None => wait_on(fd, libc::EVFILT_READ),
        }
    }
}

impl sealed::Sealed for UdpConnectTask {}
impl sealed::Sealed for UdpSendTask {}
impl sealed::Sealed for UdpRecvTask {}

impl Task for UdpConnectTask {
    type Output = Result<(), RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.connect()
    }

    /// A name lookup blocks, so only a literal address keeps it on
    /// a worker
    fn blocking(&self, _token: Token) -> bool {
        self.target.needs_lookup()
    }
}

impl Task for UdpSendTask {
    type Output = Result<usize, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for UdpRecvTask {
    type Output = Result<Vec<u8>, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl sealed::Sealed for BindTask {}
impl sealed::Sealed for SendToTask {}
impl sealed::Sealed for RecvFromTask {}

impl Task for BindTask {
    type Output = Result<UdpSocket, RuntimeError>;
    type Input = Nothing;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.bind()
    }

    /// A name lookup blocks, so only a literal address keeps it on
    /// a worker
    fn blocking(&self, _token: Token) -> bool {
        self.target.needs_lookup()
    }
}

impl Task for SendToTask {
    type Output = Result<usize, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.dest = Progress::default();
    }

    /// A name lookup blocks, so only a literal address keeps it on
    /// a worker
    fn blocking(&self, _token: Token) -> bool {
        self.target.needs_lookup()
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for RecvFromTask {
    type Output = Result<(Vec<u8>, SocketAddr), RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::futures::net::address::sealed::Sealed;

    /// A send goes to the first address in the socket's own family
    #[test]
    fn a_send_picks_an_address_in_its_own_family() {
        let v4: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let v6: SocketAddr = "[::1]:0".parse().unwrap();

        let to_v4: SocketAddr = "127.0.0.1:9".parse().unwrap();
        let to_v6: SocketAddr = "[::1]:9".parse().unwrap();

        assert_eq!(pick(&"127.0.0.1:9".target(), v4), Ok(to_v4));
        assert_eq!(pick(&"[::1]:9".target(), v6), Ok(to_v6));
        assert_eq!(pick(&"[::1]:9".target(), v4), Err(RuntimeError::BadAddress));
    }
}
