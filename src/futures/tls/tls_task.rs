//! # TLS task
//! The tasks the `Tls` constructors and a `TlsListener` return,
//! and everything they do once run

use crate::modules::input::{Token, token};
use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            address::{Target, sealed::Sealed as _},
            exchange::{self, Stage},
            socket::Options,
            step::{Progress, settle, wait_on},
        },
        task::{
            Nothing, Task,
            sealed::{self, Step},
        },
        tcp::{
            Connection,
            tcp_task::{AcceptTask, ConnectTask, ListenTask},
        },
        tls::{
            config::{self, ClientSettings, Keys, ServerSettings, tls_error},
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
    sync::Arc,
    time::Duration,
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<TlsConnection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<TlsListener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () =
    assert!(mem::size_of::<Result<(TlsConnection, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD);

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
#[must_use = "a task does nothing until it is run or spawned"]
pub struct TlsConnectTask {
    /// Where to connect, kept for the name it implies
    target: Target,

    /// The name to check the certificate against instead
    server_name: Option<Arc<str>>,

    /// Roots, protocols and identity beyond the defaults
    settings: ClientSettings,

    /// A connection already open to run the handshake over, for an
    /// upgrade
    over: Option<Connection>,

    /// The TCP connect, remade for every run
    connect: ConnectTask,

    /// What the TCP socket is set up with
    options: Options,

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
            settings: ClientSettings::default(),
            over: None,
            options: Options::default(),
            stage: Progress::default(),
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
        self.settings.roots = Some(Arc::from(pem.as_ref()));
        self
    }

    /// Offers these protocols to the server, most wanted first
    ///
    /// ## Behaviour
    /// The server picks one, and [`TlsConnection::alpn`] says which.
    /// A server that picks none still connects
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`TlsConnection::alpn`]: crate::tls::TlsConnection::alpn
    pub fn alpn<I, P>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        self.settings.alpn = protocols
            .into_iter()
            .map(|protocol| protocol.as_ref().to_vec())
            .collect();
        self
    }

    /// Shows the server this certificate chain, for a server that
    /// asks who is connecting
    ///
    /// ## Behaviour
    /// `cert` and `key` are PEM. Neither is read until the task
    /// runs
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last. A chain or key
    /// that doesn't parse, or don't fit each other, gives
    /// [`RuntimeError::BadCertificate`]
    ///
    /// [`RuntimeError::BadCertificate`]: crate::RuntimeError::BadCertificate
    pub fn identity(mut self, cert: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Self {
        self.settings.identity = Some((Arc::from(cert.as_ref()), Arc::from(key.as_ref())));
        self
    }

    /// Runs the handshake over `conn` rather than connecting
    pub(crate) fn over(conn: Connection) -> Self {
        let mut task = Self::new(conn.peer_addr().target());
        task.over = Some(conn);
        task
    }

    /// Starts a run from the beginning
    fn begin(&mut self) {
        self.connect = ConnectTask::new(self.target.clone()).with_options(self.options);
        self.stage = Progress::default();
    }

    /// A fresh client session for this connect
    fn session(&self) -> Result<rustls::Connection, RuntimeError> {
        let config = config::client_with(&self.settings)?;

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
                Connecting::Tcp => match self.opened(reactor_id, task_id) {
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
                            let step = wait_on(fd, filter)?;
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

impl TlsConnectTask {
    /// The TCP connection, connecting it first unless the task was
    /// handed one
    fn opened(&mut self, reactor_id: i32, task_id: usize) -> Step<Result<Connection, RuntimeError>> {
        match &self.over {
            Some(conn) => Step::Done(Ok(conn.clone())),
            None => self.connect.step(token(), reactor_id, task_id),
        }
    }
}

/// Opens a socket that waits for TLS connections
///
/// ## Returns
/// The listener, with its certificate and key loaded
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct TlsListenTask {
    /// The TCP listen
    listen: ListenTask,

    /// The certificate, key and everything else it serves with
    settings: ServerSettings,
}

impl TlsListenTask {
    /// Listens on `target` with the certificate and key in these
    /// files
    pub(crate) fn new(target: Target, keys: Keys) -> Self {
        Self {
            listen: ListenTask::new(target),
            settings: ServerSettings {
                keys,
                alpn: Arc::from([]),
                client_roots: None,
            },
        }
    }

    /// Accepts these protocols, most wanted first
    ///
    /// ## Behaviour
    /// Each connection gets the first of these the client offered.
    /// A client that offers none still connects. One that offers
    /// only others is refused
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn alpn<I, P>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        self.settings.alpn = protocols
            .into_iter()
            .map(|protocol| protocol.as_ref().to_vec())
            .collect();
        self
    }

    /// Requires every client to show a certificate that chains to
    /// one of the roots in `pem`
    ///
    /// ## Behaviour
    /// A client without one fails its handshake.
    /// [`TlsConnection::peer_certificates`] gives the one it showed
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last. Roots that don't
    /// parse give [`RuntimeError::BadCertificate`] when the task runs
    ///
    /// [`TlsConnection::peer_certificates`]: crate::tls::TlsConnection::peer_certificates
    /// [`RuntimeError::BadCertificate`]: crate::RuntimeError::BadCertificate
    pub fn require_client_cert(mut self, pem: impl AsRef<[u8]>) -> Self {
        self.settings.client_roots = Some(Arc::from(pem.as_ref()));
        self
    }

    /// How many connections may wait to be accepted
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn backlog(mut self, backlog: u32) -> Self {
        let options = Options {
            backlog: Some(backlog),
            ..self.listen.options()
        };

        self.listen = self.listen.with_options(options);
        self
    }

    /// Lets other sockets listen on the same port
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn reuse_port(mut self, reuse: bool) -> Self {
        let options = Options {
            reuse_port: reuse,
            ..self.listen.options()
        };

        self.listen = self.listen.with_options(options);
        self
    }

    /// Takes only IPv6 connections on an IPv6 address
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn v6_only(mut self, only: bool) -> Self {
        let options = Options {
            v6_only: only,
            ..self.listen.options()
        };

        self.listen = self.listen.with_options(options);
        self
    }

    /// Loads the certificate, then binds and listens
    ///
    /// The files first, so a bad one never opens a socket
    fn listen(&self, reactor_id: i32, task_id: usize) -> Result<TlsListener, RuntimeError> {
        let config = config::server_with(&self.settings)?;
        let tcp = self.listen.execute(token(), reactor_id, task_id)?;

        Ok(TlsListener::new(tcp, config))
    }
}

/// Takes the next TLS connection off a listener
///
/// ## Returns
/// The connection, with its handshake done, and the address it
/// came from
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct TlsAcceptTask {
    /// Where the connections come from
    listener: TlsListener,

    /// The TCP accept, remade for every run
    accept: AcceptTask,

    /// A connection already open to run the handshake over, for an
    /// upgrade
    over: Option<Connection>,

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
            over: None,
            stage: Progress::default(),
        }
    }

    /// Runs the handshake over `conn` rather than accepting
    pub(crate) fn over(listener: TlsListener, conn: Connection) -> Self {
        let mut task = Self::new(listener);
        task.over = Some(conn);
        task
    }

    /// The TCP connection, accepting it first unless the task was
    /// handed one
    fn opened(
        &mut self,
        reactor_id: i32,
        task_id: usize,
    ) -> Step<Result<(Connection, SocketAddr), RuntimeError>> {
        match &self.over {
            Some(conn) => Step::Done(Ok((conn.clone(), conn.peer_addr()))),
            None => self.accept.step(token(), reactor_id, task_id),
        }
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
                Accepting::Tcp => match self.opened(reactor_id, task_id) {
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
                            let step = wait_on(fd, filter)?;
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
#[must_use = "a task does nothing until it is run or spawned"]
pub struct TlsRequestTask {
    /// How it connects
    connect: TlsConnectTask,

    /// What it sends
    data: Arc<[u8]>,

    /// How far this run has got
    stage: Progress<Stage>,
}

impl TlsRequestTask {
    /// Sends `data` to `target` and reads what comes back
    pub(crate) fn new(target: Target, data: Arc<[u8]>) -> Self {
        Self {
            connect: TlsConnectTask::new(target),
            data,
            stage: Progress::default(),
        }
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

    /// Offers these protocols to the server, most wanted first
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn alpn<I, P>(mut self, protocols: I) -> Self
    where
        I: IntoIterator<Item = P>,
        P: AsRef<[u8]>,
    {
        self.connect = self.connect.alpn(protocols);
        self
    }

    /// Shows the server this certificate chain, for a server that
    /// asks who is connecting
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn identity(mut self, cert: impl AsRef<[u8]>, key: impl AsRef<[u8]>) -> Self {
        self.connect = self.connect.identity(cert, key);
        self
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

impl sealed::Sealed for TlsConnectTask {}
impl sealed::Sealed for TlsListenTask {}
impl sealed::Sealed for TlsAcceptTask {}
impl sealed::Sealed for TlsRequestTask {}

impl Task for TlsConnectTask {
    type Output = Result<TlsConnection, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.begin();
    }

    /// Always. It still parks between steps
    fn blocking(&self, _token: Token) -> bool {
        true
    }

    fn step(&mut self, _token: Token, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        settle(self.advance(reactor_id, task_id))
    }
}

impl Task for TlsListenTask {
    type Output = Result<TlsListener, RuntimeError>;
    type Input = Nothing;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        self.listen(reactor_id, task_id)
    }

    /// It reads files, and may look a name up
    fn blocking(&self, _token: Token) -> bool {
        true
    }
}

impl Task for TlsAcceptTask {
    type Output = Result<(TlsConnection, SocketAddr), RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.accept = AcceptTask::new(self.listener.tcp().clone());
        self.stage = Progress::default();
    }

    fn step(&mut self, _token: Token, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        settle(self.advance(reactor_id, task_id))
    }
}

impl Task for TlsRequestTask {
    type Output = Result<Vec<u8>, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.connect.begin();
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

    /// The name a certificate is checked against, for every way
    /// of saying where to connect
    #[test]
    fn the_server_name_comes_from_where_it_connects() {
        let named = server_name(&"example.com:443".target(), None).unwrap();
        assert_eq!(named, ServerName::try_from("example.com").unwrap());

        let v4 = server_name(&"127.0.0.1:443".target(), None).unwrap();
        assert_eq!(
            v4,
            ServerName::IpAddress("127.0.0.1".parse::<IpAddr>().unwrap().into())
        );

        let v6 = server_name(&"[::1]:443".target(), None).unwrap();
        assert_eq!(
            v6,
            ServerName::IpAddress("::1".parse::<IpAddr>().unwrap().into())
        );

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
