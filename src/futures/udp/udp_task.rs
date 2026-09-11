//! # UDP task
//! The tasks `Udp::bind` and a `UdpSocket` return, and
//! everything they do once run

use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            address::{Target, family, from_raw, local_of, to_raw},
            datagram::{recv_datagram, send_datagram},
            socket::open,
            step::{Clock, Progress, settle},
        },
        task::{
            Task,
            sealed::{self, Step},
        },
        udp::udp_socket::UdpSocket,
    },
    modules::{int_check::IntCheck, park},
};
use std::{mem, net::SocketAddr, sync::Arc, time::Duration};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<UdpSocket, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(
    mem::size_of::<Result<(Vec<u8>, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD
);

/// Opens a UDP socket
///
/// ## Returns
/// The socket, bound to the first address that takes
#[derive(Debug, Clone)]
pub struct BindTask {
    /// Where to bind
    target: Target,

    /// The timeout, which only a name lookup can use up
    clock: Clock,
}

impl BindTask {
    /// Binds to `target`
    pub(crate) fn new(target: Target) -> Self {
        Self {
            target,
            clock: Clock::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Binding never waits, so only a slow name lookup can use
    /// this up. Running out gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Binds
    fn bind(&self) -> Result<UdpSocket, RuntimeError> {
        let found = self.target.resolve()?;

        if self.clock.expired() {
            return Err(RuntimeError::TimedOut);
        }

        let mut failure = RuntimeError::BadAddress;

        for addr in found {
            match bind_one(&addr) {
                Ok(socket) => return Ok(socket),
                Err(error) => failure = error,
            }
        }

        Err(failure)
    }
}

/// Binds a fresh socket to `addr`
fn bind_one(addr: &SocketAddr) -> Result<UdpSocket, RuntimeError> {
    let fd = open(family(addr), libc::SOCK_DGRAM)?;
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
pub struct SendToTask {
    /// What to send from
    socket: UdpSocket,

    /// Where to send
    target: Target,

    /// What to send
    data: Arc<[u8]>,

    /// The timeout
    clock: Clock,

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
            clock: Clock::default(),
            dest: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// A send only waits while the socket has no room, which is
    /// rare. Running out gives [`RuntimeError::TimedOut`], and
    /// nothing was sent
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
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
            None => self.clock.wait(fd, libc::EVFILT_WRITE),
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
pub struct RecvFromTask {
    /// Where to receive
    socket: UdpSocket,

    /// The timeout
    clock: Clock,
}

impl RecvFromTask {
    /// Receives on `socket`
    pub(crate) fn new(socket: UdpSocket) -> Self {
        Self {
            socket,
            clock: Clock::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Running out with nothing having arrived gives
    /// [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Takes a datagram if one is waiting
    fn advance(
        &mut self,
    ) -> Result<Step<Result<(Vec<u8>, SocketAddr), RuntimeError>>, RuntimeError> {
        let fd = self.socket.fd();

        match recv_datagram(fd)? {
            Some((data, storage, _)) => {
                let from = from_raw(&storage).ok_or(RuntimeError::BadAddress)?;

                Ok(Step::Done(Ok((data, from))))
            }

            None => self.clock.wait(fd, libc::EVFILT_READ),
        }
    }
}

impl sealed::Sealed for BindTask {}
impl sealed::Sealed for SendToTask {}
impl sealed::Sealed for RecvFromTask {}

impl Task for BindTask {
    type Output = Result<UdpSocket, RuntimeError>;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.bind()
    }

    fn prepare(&mut self) {
        self.clock.start();
    }

    /// A name lookup blocks, so only a literal address keeps it on
    /// a worker
    fn blocking(&self) -> bool {
        self.target.needs_lookup()
    }
}

impl Task for SendToTask {
    type Output = Result<usize, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.dest = Progress::default();
    }

    /// A name lookup blocks, so only a literal address keeps it on
    /// a worker
    fn blocking(&self) -> bool {
        self.target.needs_lookup()
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for RecvFromTask {
    type Output = Result<(Vec<u8>, SocketAddr), RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
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
