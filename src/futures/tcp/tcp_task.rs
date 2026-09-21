//! # TCP task
//! The tasks the `Tcp` constructors and a `Listener` return,
//! and everything they do once run

use crate::modules::input::{Token, token};
use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            address::{Target, family, from_raw, local_of, peer_of, to_raw},
            exchange::{self, Stage},
            socket::{Options, begin_connect, configure, finished_connecting, open, set_flag},
            step::{Progress, settle, wait_on},
        },
        task::{
            Nothing, Task,
            sealed::{self, Step},
        },
        tcp::connection::{Connection, Listener},
    },
    modules::{fd::Fd, int_check::IntCheck, park},
};
use std::{mem, net::SocketAddr, sync::Arc, time::Duration};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Connection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Listener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () =
    assert!(mem::size_of::<Result<(Connection, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD);

/// Opens a connection
///
/// ## Returns
/// The connection. Each address a name looks up to is tried in
/// turn, and the error from the last one comes back if none of
/// them take
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ConnectTask {
    /// Where to connect
    target: Target,

    /// What the socket is set up with
    options: Options,

    /// How far this run has got
    progress: Progress<Connecting>,
}

/// How far a connect has got
#[derive(Default)]
struct Connecting {
    /// Addresses still to try, the next one last. `None` before
    /// they have been looked up
    left: Option<Vec<SocketAddr>>,

    /// A socket part way through connecting, and where to
    trying: Option<(Fd, SocketAddr)>,

    /// Why the last address tried didn't take
    failure: Option<RuntimeError>,
}

/// How a connect came back when it was started
enum Started {
    /// Connected at once, which a loopback address can do
    Connected(Fd),

    /// Under way, to be finished when the socket is writable
    Waiting(Fd),
}

impl ConnectTask {
    /// Connects to `target`
    pub(crate) fn new(target: Target) -> Self {
        Self {
            target,
            options: Options::default(),
            progress: Progress::default(),
        }
    }

    /// Sends small writes at once rather than waiting to batch them
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn nodelay(mut self, nodelay: bool) -> Self {
        self.options.nodelay = nodelay;
        self
    }

    /// Probes a quiet connection after `idle`, so a peer that has
    /// gone is noticed
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn keepalive(mut self, idle: Duration) -> Self {
        self.options.keepalive = Some(idle);
        self
    }

    /// Carries settings over from a task this one runs inside
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) fn with_options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// Does as much of the connect as can be done without waiting
    fn advance(&mut self) -> Result<Step<Result<Connection, RuntimeError>>, RuntimeError> {
        let state = &mut self.progress.0;

        loop {
            if let Some((fd, addr)) = state.trying.take() {
                match finished_connecting(fd.raw()) {
                    Ok(true) => return Ok(Step::Done(connected(fd, addr))),

                    Ok(false) => {
                        let step = wait_on(fd.raw(), libc::EVFILT_WRITE)?;
                        state.trying = Some((fd, addr));

                        return Ok(step);
                    }

                    // The socket goes, and the next address is tried
                    Err(error) => state.failure = Some(error),
                }

                continue;
            }

            if state.left.is_none() {
                let mut found = self.target.resolve()?;

                // Popped from the end, so reversed to keep the order
                found.reverse();
                state.left = Some(found);
            }

            let Some(addr) = state.left.as_mut().and_then(Vec::pop) else {
                return Err(state.failure.take().unwrap_or(RuntimeError::BadAddress));
            };

            match start_connect(&addr, &self.options) {
                Ok(Started::Connected(fd)) => return Ok(Step::Done(connected(fd, addr))),
                Ok(Started::Waiting(fd)) => state.trying = Some((fd, addr)),
                Err(error) => state.failure = Some(error),
            }
        }
    }
}

/// Starts a connect on a fresh socket
fn start_connect(addr: &SocketAddr, options: &Options) -> Result<Started, RuntimeError> {
    let fd = open(family(addr), libc::SOCK_STREAM)?;

    options.apply(fd.raw(), addr.is_ipv6())?;

    let (raw, len) = to_raw(addr);

    let at_once = begin_connect(
        fd.raw(),
        (&raw as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
        len,
    )?;

    match at_once {
        true => Ok(Started::Connected(fd)),
        false => Ok(Started::Waiting(fd)),
    }
}

/// Wraps a socket that has finished connecting
fn connected(fd: Fd, peer: SocketAddr) -> Result<Connection, RuntimeError> {
    let local = local_of(fd.raw())?;

    Ok(Connection::new(fd, local, peer))
}

/// Opens a socket that waits for connections
///
/// ## Returns
/// The listener, bound to the first address that takes
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct ListenTask {
    /// Where to listen
    target: Target,

    /// What the socket is set up with
    options: Options,
}

impl ListenTask {
    /// Listens on `target`
    pub(crate) fn new(target: Target) -> Self {
        Self {
            target,
            options: Options::default(),
        }
    }

    /// How many connections may wait to be accepted
    ///
    /// ## Behaviour
    /// The kernel's own limit caps it
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn backlog(mut self, backlog: u32) -> Self {
        self.options.backlog = Some(backlog);
        self
    }

    /// Lets other sockets listen on the same port, each set up the
    /// same way
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn reuse_port(mut self, reuse: bool) -> Self {
        self.options.reuse_port = reuse;
        self
    }

    /// Takes only IPv6 connections on an IPv6 address
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn v6_only(mut self, only: bool) -> Self {
        self.options.v6_only = only;
        self
    }

    /// Carries settings over from a task this one runs inside
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) fn with_options(mut self, options: Options) -> Self {
        self.options = options;
        self
    }

    /// The settings the socket is set up with
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) fn options(&self) -> Options {
        self.options
    }

    /// Binds and listens
    fn listen(&self) -> Result<Listener, RuntimeError> {
        let found = self.target.resolve()?;

        let mut failure = RuntimeError::BadAddress;

        for addr in found {
            match bind_listen(&addr, &self.options) {
                Ok(listener) => return Ok(listener),
                Err(error) => failure = error,
            }
        }

        Err(failure)
    }
}

/// Binds a fresh socket to `addr` and starts it listening
fn bind_listen(addr: &SocketAddr, options: &Options) -> Result<Listener, RuntimeError> {
    let fd = open(family(addr), libc::SOCK_STREAM)?;

    options.apply(fd.raw(), addr.is_ipv6())?;

    // So a port that was just in use can be listened on again at
    // once, rather than after the old connections time out
    set_flag(fd.raw(), libc::SO_REUSEADDR)?;

    let (raw, len) = to_raw(addr);

    unsafe {
        libc::bind(
            fd.raw(),
            (&raw as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
            len,
        )
    }
    .check()?;

    unsafe { libc::listen(fd.raw(), options.backlog()) }.check()?;

    // Port 0 was a free one picked by the kernel, so this is the
    // only way to know which
    let local = local_of(fd.raw())?;

    Ok(Listener::new(fd, local))
}

/// Takes the next connection off a listener
///
/// ## Returns
/// The connection, and the address it came from
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct AcceptTask {
    /// Where the connections come from
    listener: Listener,
}

impl AcceptTask {
    /// Accepts from `listener`
    pub(crate) fn new(listener: Listener) -> Self {
        Self { listener }
    }

    /// Takes a connection if one is waiting
    fn advance(
        &mut self,
    ) -> Result<Step<Result<(Connection, SocketAddr), RuntimeError>>, RuntimeError> {
        let fd = self.listener.fd();

        loop {
            let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
            let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

            let accepted = unsafe {
                libc::accept(
                    fd,
                    (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
                    &mut len,
                )
            }
            .check();

            match accepted {
                Ok(raw) => return Ok(Step::Done(adopt(Fd::new(raw), &storage))),

                // Gone again before it could be taken, so the next one
                Err(RuntimeError::CheckError(Some(libc::EINTR | libc::ECONNABORTED))) => {}

                Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => {
                    return wait_on(fd, libc::EVFILT_READ);
                }

                Err(error) => return Err(error),
            }
        }
    }
}

/// Wraps a socket fresh from `accept`
fn adopt(
    fd: Fd,
    storage: &libc::sockaddr_storage,
) -> Result<(Connection, SocketAddr), RuntimeError> {
    configure(fd.raw())?;

    let peer = match from_raw(storage) {
        Some(peer) => peer,
        None => peer_of(fd.raw())?,
    };

    let local = local_of(fd.raw())?;

    Ok((Connection::new(fd, local, peer), peer))
}

/// Connects, sends, and reads the answer to the end
///
/// ## Returns
/// Everything the other side sent before it closed the
/// connection
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct RequestTask {
    /// How it connects
    connect: ConnectTask,

    /// What it sends
    data: Arc<[u8]>,

    /// How far this run has got
    stage: Progress<Stage>,
}

impl RequestTask {
    /// Sends `data` to `target` and reads what comes back
    pub(crate) fn new(target: Target, data: Arc<[u8]>) -> Self {
        Self {
            connect: ConnectTask::new(target),
            data,
            stage: Progress::default(),
        }
    }

    /// Takes the exchange as far as it can go without waiting
    fn advance(&mut self, reactor_id: i32, task_id: usize) -> Step<Result<Vec<u8>, RuntimeError>> {
        exchange::advance(
            &mut self.connect,
            &mut self.stage.0,
            &self.data,
            reactor_id,
            task_id,
        )
    }
}

impl sealed::Sealed for ConnectTask {}
impl sealed::Sealed for ListenTask {}
impl sealed::Sealed for AcceptTask {}
impl sealed::Sealed for RequestTask {}

impl Task for ConnectTask {
    type Output = Result<Connection, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.progress = Progress::default();
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

impl Task for ListenTask {
    type Output = Result<Listener, RuntimeError>;
    type Input = Nothing;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.listen()
    }

    /// A name lookup blocks, so only a literal address keeps it on
    /// a worker
    fn blocking(&self, _token: Token) -> bool {
        self.target.needs_lookup()
    }
}

impl Task for AcceptTask {
    type Output = Result<(Connection, SocketAddr), RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for RequestTask {
    type Output = Result<Vec<u8>, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.connect.progress = Progress::default();
        self.stage = Progress::default();
    }

    /// Whatever the connect says
    fn blocking(&self, _token: Token) -> bool {
        self.connect.blocking(token())
    }

    fn step(&mut self, _token: Token, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        self.advance(reactor_id, task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::futures::net::address::sealed::Sealed;
    use crate::modules::input::token;

    /// Only a task that has to look a name up asks for a sleep
    /// thread. The rest step on a worker and park
    #[test]
    fn only_a_name_lookup_blocks() {
        assert!(!ConnectTask::new("127.0.0.1:80".target()).blocking(token()));
        assert!(ConnectTask::new("localhost:80".target()).blocking(token()));
        assert!(!ListenTask::new("127.0.0.1:0".target()).blocking(token()));
        assert!(ListenTask::new("localhost:0".target()).blocking(token()));
        assert!(!RequestTask::new("[::1]:80".target(), Arc::from(&b""[..])).blocking(token()));
        assert!(RequestTask::new("localhost:80".target(), Arc::from(&b""[..])).blocking(token()));
    }
}
