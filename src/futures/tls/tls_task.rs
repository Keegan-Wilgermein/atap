//! # TLS task
//! The tasks the `Tls` constructors and a `TlsListener` return,
//! and everything they do once run
//!
//! Each is a TCP task with a handshake after it. The TCP part is
//! the TCP task itself, stepped from inside, and the handshake
//! parks between its steps the same way
//!
//! Sending and receiving on a connection are in `net::stream`,
//! shared with TCP and Unix

use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            address::Target,
            step::{Clock, Progress, settle},
            stream::{RecvTask, SendTask},
        },
        task::{
            Task,
            sealed::{self, Step},
        },
        tcp::{
            Connection,
            tcp_task::{AcceptTask, ConnectTask, ListenTask},
        },
        tls::{
            config::{self, tls_error},
            connection::{TlsConnection, TlsListener},
            handshake::handshake,
        },
    },
    modules::park,
};
use rustls::pki_types::ServerName;
use std::{
    mem,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<TlsConnection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<TlsListener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(
    mem::size_of::<Result<(TlsConnection, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD
);

/// The name a server's certificate is checked against
///
/// ## Behaviour
/// `given` wins. Otherwise the host in a `host:port`, or the IP
/// of an address. An IP written as text is still checked as an
/// IP
///
/// ## Returns
/// `BadAddress` when there is nothing usable to check against
fn server_name(target: &Target, given: Option<&str>) -> Result<ServerName<'static>, RuntimeError> {
    let host = match (given, target) {
        (Some(name), _) => name,
        (None, Target::Addr(addr)) => return Ok(ServerName::IpAddress(addr.ip().into())),
        (None, Target::Name(name)) => host_of(name).ok_or(RuntimeError::BadAddress)?,
    };

    if let Ok(ip) = host.parse::<IpAddr>() {
        return Ok(ServerName::IpAddress(ip.into()));
    }

    ServerName::try_from(host.to_owned()).map_err(|_| RuntimeError::BadAddress)
}

/// The host in a `host:port`, without the brackets an IPv6 one
/// is written in
fn host_of(text: &str) -> Option<&str> {
    let (host, _) = text.rsplit_once(':')?;

    let host = host
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(host);

    (!host.is_empty()).then_some(host)
}

/// Opens a TLS connection
///
/// ## Returns
/// The connection, with its handshake done and the server's
/// certificate checked
#[derive(Debug, Clone)]
pub struct TlsConnectTask {
    /// Where to connect, kept for the name it implies
    target: Target,

    /// The name to check the certificate against instead
    server_name: Option<Arc<str>>,

    /// Roots to trust on top of the system's, as PEM
    roots: Option<Arc<[u8]>>,

    /// The timeout, which covers the connect and the handshake
    clock: Clock,

    /// The TCP connect, remade for every run
    connect: ConnectTask,

    /// How far this run has got
    stage: Progress<Connecting>,
}

/// How far a TLS connect has got
#[derive(Default)]
enum Connecting {
    /// Opening the TCP connection
    #[default]
    Tcp,

    /// Running the handshake over it
    Handshaking(Connection, rustls::Connection),
}

impl TlsConnectTask {
    /// Connects to `target`
    pub(crate) fn new(target: Target) -> Self {
        Self {
            connect: ConnectTask::new(target.clone()),
            target,
            server_name: None,
            roots: None,
            clock: Clock::default(),
            stage: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Covers the TCP connect and the handshake together. Running
    /// out gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Checks the certificate against `name` instead of the host
    /// that was connected to
    ///
    /// ## Behaviour
    /// For connecting to an address whose certificate is for a
    /// name, such as `127.0.0.1` serving `localhost`
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn server_name(mut self, name: &str) -> Self {
        self.server_name = Some(Arc::from(name));
        self
    }

    /// Trusts the root certificates in `pem` as well as the
    /// system's
    ///
    /// ## Behaviour
    /// For a private certificate authority, or a test's own. The
    /// system's roots still count
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last. A `pem` with no
    /// certificate in it gives [`RuntimeError::BadCertificate`] when
    /// the task runs
    ///
    /// [`RuntimeError::BadCertificate`]: crate::RuntimeError::BadCertificate
    pub fn trust(mut self, pem: impl AsRef<[u8]>) -> Self {
        self.roots = Some(Arc::from(pem.as_ref()));
        self
    }

    /// Starts a run against a clock that has already started
    fn begin(&mut self, clock: Clock) {
        self.clock = clock;
        self.connect = ConnectTask::new(self.target.clone()).timed(clock);
        self.stage = Progress::default();
    }

    /// A fresh client session for this connect
    fn session(&self) -> Result<rustls::Connection, RuntimeError> {
        let config = match &self.roots {
            Some(pem) => config::client_trusting(pem)?,
            None => config::client()?,
        };

        let name = server_name(&self.target, self.server_name.as_deref())?;
        let session = rustls::ClientConnection::new(config, name).map_err(tls_error)?;

        Ok(session.into())
    }

    /// Takes the connect and the handshake as far as they can go
    /// without waiting
    fn advance(
        &mut self,
        reactor_id: i32,
        task_id: usize,
    ) -> Result<Step<Result<TlsConnection, RuntimeError>>, RuntimeError> {
        loop {
            match mem::take(&mut self.stage.0) {
                Connecting::Tcp => match self.connect.step(reactor_id, task_id) {
                    Step::Done(Ok(tcp)) => {
                        let session = self.session()?;
                        self.stage.0 = Connecting::Handshaking(tcp, session);
                    }

                    Step::Done(Err(error)) => return Err(error),
                    Step::Park(park) => return Ok(Step::Park(park)),
                },

                Connecting::Handshaking(tcp, mut session) => {
                    let fd = tcp.pipe().fd();

                    match handshake(&mut session, fd)? {
                        Some(filter) => {
                            let step = self.clock.wait(fd, filter)?;
                            self.stage.0 = Connecting::Handshaking(tcp, session);

                            return Ok(step);
                        }

                        None => return Ok(Step::Done(Ok(TlsConnection::new(tcp, session)))),
                    }
                }
            }
        }
    }
}

/// Opens a socket that waits for TLS connections
///
/// ## Returns
/// The listener, with its certificate and key loaded
#[derive(Debug, Clone)]
pub struct TlsListenTask {
    /// The TCP listen
    listen: ListenTask,

    /// The certificate chain, as a PEM file
    cert: PathBuf,

    /// The private key, as a PEM file
    key: PathBuf,

    /// The timeout, which only a name lookup can use up
    clock: Clock,
}

impl TlsListenTask {
    /// Listens on `target` with the certificate and key in these
    /// files
    pub(crate) fn new(target: Target, cert: PathBuf, key: PathBuf) -> Self {
        Self {
            listen: ListenTask::new(target),
            cert,
            key,
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

    /// Loads the certificate, then binds and listens
    ///
    /// The files first, so a bad one never opens a socket
    fn listen(&self, reactor_id: i32, task_id: usize) -> Result<TlsListener, RuntimeError> {
        let config = config::server(&self.cert, &self.key)?;
        let tcp = self.listen.execute(reactor_id, task_id)?;

        if self.clock.expired() {
            return Err(RuntimeError::TimedOut);
        }

        Ok(TlsListener::new(tcp, config))
    }
}

/// Takes the next TLS connection off a listener
///
/// ## Returns
/// The connection, with its handshake done, and the address it
/// came from
#[derive(Debug, Clone)]
pub struct TlsAcceptTask {
    /// Where the connections come from
    listener: TlsListener,

    /// The timeout, which covers waiting and the handshake
    clock: Clock,

    /// The TCP accept, remade for every run
    accept: AcceptTask,

    /// How far this run has got
    stage: Progress<Accepting>,
}

/// How far a TLS accept has got
#[derive(Default)]
enum Accepting {
    /// Waiting for a TCP connection
    #[default]
    Tcp,

    /// Running the handshake over one
    Handshaking(Connection, SocketAddr, rustls::Connection),
}

impl TlsAcceptTask {
    /// Accepts from `listener`
    pub(crate) fn new(listener: TlsListener) -> Self {
        Self {
            accept: AcceptTask::new(listener.tcp().clone()),
            listener,
            clock: Clock::default(),
            stage: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Covers waiting for a connection and its handshake together.
    /// Running out gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Takes the accept and the handshake as far as they can go
    /// without waiting
    fn advance(
        &mut self,
        reactor_id: i32,
        task_id: usize,
    ) -> Result<Step<Result<(TlsConnection, SocketAddr), RuntimeError>>, RuntimeError> {
        loop {
            match mem::take(&mut self.stage.0) {
                Accepting::Tcp => match self.accept.step(reactor_id, task_id) {
                    Step::Done(Ok((tcp, peer))) => {
                        let session = rustls::ServerConnection::new(self.listener.config())
                            .map_err(tls_error)?;

                        self.stage.0 = Accepting::Handshaking(tcp, peer, session.into());
                    }

                    Step::Done(Err(error)) => return Err(error),
                    Step::Park(park) => return Ok(Step::Park(park)),
                },

                Accepting::Handshaking(tcp, peer, mut session) => {
                    let fd = tcp.pipe().fd();

                    match handshake(&mut session, fd)? {
                        Some(filter) => {
                            let step = self.clock.wait(fd, filter)?;
                            self.stage.0 = Accepting::Handshaking(tcp, peer, session);

                            return Ok(step);
                        }

                        None => {
                            return Ok(Step::Done(Ok((TlsConnection::new(tcp, session), peer))));
                        }
                    }
                }
            }
        }
    }
}

/// Connects over TLS, sends, and reads the answer to the end
///
/// ## Returns
/// Everything the server sent before it closed the session
#[derive(Debug, Clone)]
pub struct TlsRequestTask {
    /// How it connects
    connect: TlsConnectTask,

    /// What it sends
    data: Arc<[u8]>,

    /// The timeout, which covers the whole exchange
    clock: Clock,

    /// How far this run has got
    stage: Progress<Stage>,
}

/// Where a request is
#[derive(Default)]
enum Stage {
    /// Opening the connection and running the handshake
    #[default]
    Connecting,

    /// Sending the request
    Sending(SendTask),

    /// Reading the answer
    Reading(RecvTask),
}

impl TlsRequestTask {
    /// Sends `data` to `target` and reads what comes back
    pub(crate) fn new(target: Target, data: Arc<[u8]>) -> Self {
        Self {
            connect: TlsConnectTask::new(target),
            data,
            clock: Clock::default(),
            stage: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Covers the whole exchange: connecting, the handshake,
    /// sending, and reading the answer. Running out gives
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

    /// Checks the certificate against `name` instead of the host
    /// that was connected to
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn server_name(mut self, name: &str) -> Self {
        self.connect = self.connect.server_name(name);
        self
    }

    /// Trusts the root certificates in `pem` as well as the
    /// system's
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn trust(mut self, pem: impl AsRef<[u8]>) -> Self {
        self.connect = self.connect.trust(pem);
        self
    }

    /// Takes the exchange as far as it can go without waiting
    fn advance(&mut self, reactor_id: i32, task_id: usize) -> Step<Result<Vec<u8>, RuntimeError>> {
        loop {
            match &mut self.stage.0 {
                Stage::Connecting => match self.connect.step(reactor_id, task_id) {
                    Step::Done(Ok(conn)) => {
                        let send = conn.send(self.data.clone()).timed(self.clock);

                        self.stage.0 = Stage::Sending(send);
                    }

                    Step::Done(Err(error)) => return Step::Done(Err(error)),
                    Step::Park(park) => return Step::Park(park),
                },

                Stage::Sending(send) => match send.step(reactor_id, task_id) {
                    Step::Done(Ok(_)) => {
                        let read = RecvTask::to_end(send.source().clone()).timed(self.clock);

                        self.stage.0 = Stage::Reading(read);
                    }

                    Step::Done(Err(error)) => return Step::Done(Err(error)),
                    Step::Park(park) => return Step::Park(park),
                },

                Stage::Reading(read) => return read.step(reactor_id, task_id),
            }
        }
    }
}

impl sealed::Sealed for TlsConnectTask {}
impl sealed::Sealed for TlsListenTask {}
impl sealed::Sealed for TlsAcceptTask {}
impl sealed::Sealed for TlsRequestTask {}

impl Task for TlsConnectTask {
    type Output = Result<TlsConnection, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        let mut clock = self.clock;
        clock.start();

        self.begin(clock);
    }

    /// Always, since checking a certificate asks macOS, which can go
    /// to the network for revocation and missing intermediates. It
    /// still parks between steps
    fn blocking(&self) -> bool {
        true
    }

    fn step(&mut self, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        settle(self.advance(reactor_id, task_id))
    }
}

impl Task for TlsListenTask {
    type Output = Result<TlsListener, RuntimeError>;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        self.listen(reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
    }

    /// It reads files, and may look a name up
    fn blocking(&self) -> bool {
        true
    }
}

impl Task for TlsAcceptTask {
    type Output = Result<(TlsConnection, SocketAddr), RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.accept = AcceptTask::new(self.listener.tcp().clone()).timed(self.clock);
        self.stage = Progress::default();
    }

    fn step(&mut self, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        settle(self.advance(reactor_id, task_id))
    }
}

impl Task for TlsRequestTask {
    type Output = Result<Vec<u8>, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.connect.begin(self.clock);
        self.stage = Progress::default();
    }

    /// Whatever the connect says
    fn blocking(&self) -> bool {
        self.connect.blocking()
    }

    fn step(&mut self, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        self.advance(reactor_id, task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::futures::net::address::sealed::Sealed;

    /// The name a certificate is checked against, for every way
    /// of saying where to connect
    #[test]
    fn the_server_name_comes_from_where_it_connects() {
        let named = server_name(&"example.com:443".target(), None).unwrap();
        assert_eq!(named, ServerName::try_from("example.com").unwrap());

        let v4 = server_name(&"127.0.0.1:443".target(), None).unwrap();
        assert_eq!(v4, ServerName::IpAddress("127.0.0.1".parse::<IpAddr>().unwrap().into()));

        let v6 = server_name(&"[::1]:443".target(), None).unwrap();
        assert_eq!(v6, ServerName::IpAddress("::1".parse::<IpAddr>().unwrap().into()));

        let given = server_name(&"127.0.0.1:443".target(), Some("localhost")).unwrap();
        assert_eq!(given, ServerName::try_from("localhost").unwrap());
    }

    /// Nothing usable to check against is a bad address
    #[test]
    fn no_usable_name_is_a_bad_address() {
        assert_eq!(
            server_name(&"no port here".target(), None),
            Err(RuntimeError::BadAddress),
        );

        assert_eq!(
            server_name(&"example.com:443".target(), Some("not a name!")),
            Err(RuntimeError::BadAddress),
        );
    }

    /// The host is picked out of a `host:port`, brackets and all
    #[test]
    fn the_host_is_picked_out() {
        assert_eq!(host_of("example.com:443"), Some("example.com"));
        assert_eq!(host_of("[fe80::1]:443"), Some("fe80::1"));
        assert_eq!(host_of(":443"), None);
        assert_eq!(host_of("no port"), None);
    }
}
