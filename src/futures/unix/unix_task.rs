//! # Unix task
//! The tasks the `Unix` constructors, a `UnixListener` and a
//! `UnixDatagram` return, and everything they do once run
//!
//! Sending and receiving on a connection are in `net::stream`,
//! shared with TCP

use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            datagram::{recv_datagram, send_datagram},
            socket::{Fd, begin_connect, configure, finished_connecting, open},
            step::{Clock, Progress, settle},
        },
        task::{
            Task,
            sealed::{self, Step},
        },
        unix::{
            path::{Bound, from_raw, to_raw},
            unix_socket::{UnixConnection, UnixDatagram, UnixListener},
        },
    },
    modules::{int_check::IntCheck, park},
};
use std::{
    mem,
    path::{Path, PathBuf},
    ptr,
    sync::Arc,
    time::Duration,
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<UnixConnection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<UnixListener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<UnixDatagram, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(
    mem::size_of::<Result<(Vec<u8>, Option<PathBuf>), RuntimeError>>() <= INLINE_PAYLOAD
);

/// Binds a fresh socket of `kind` to `path`
///
/// ## Returns
/// The socket and its file, which is removed again if anything
/// after the bind fails
fn bind_path(path: &Path, kind: libc::c_int) -> Result<(Fd, Bound), RuntimeError> {
    let (raw, len) = to_raw(path)?;
    let fd = open(libc::AF_UNIX, kind)?;

    unsafe {
        libc::bind(
            fd.raw(),
            (&raw as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
            len,
        )
    }
    .check()?;

    // The file exists from here, so it is this socket's to remove
    Ok((fd, Bound::new(path)))
}

/// Opens a Unix connection
///
/// ## Returns
/// The connection
#[derive(Debug, Clone)]
pub struct UnixConnectTask {
    /// Where to connect
    path: PathBuf,

    /// The timeout
    clock: Clock,

    /// A socket part way through connecting
    trying: Progress<Option<Fd>>,
}

impl UnixConnectTask {
    /// Connects to the socket at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            clock: Clock::default(),
            trying: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// A Unix connect almost always finishes at once, so this is
    /// rarely reached. Running out gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Does as much of the connect as can be done without waiting
    fn advance(&mut self) -> Result<Step<Result<UnixConnection, RuntimeError>>, RuntimeError> {
        let fd = match self.trying.0.take() {
            Some(fd) => fd,

            None => {
                let (raw, len) = to_raw(&self.path)?;
                let fd = open(libc::AF_UNIX, libc::SOCK_STREAM)?;

                let at_once = begin_connect(
                    fd.raw(),
                    (&raw as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
                    len,
                )?;

                if at_once {
                    return Ok(Step::Done(Ok(UnixConnection::new(fd, self.path.clone()))));
                }

                fd
            }
        };

        if finished_connecting(fd.raw())? {
            return Ok(Step::Done(Ok(UnixConnection::new(fd, self.path.clone()))));
        }

        let step = self.clock.wait(fd.raw(), libc::EVFILT_WRITE)?;
        self.trying.0 = Some(fd);

        Ok(step)
    }
}

/// Opens a Unix socket that waits for connections
///
/// ## Returns
/// The listener
#[derive(Debug, Clone)]
pub struct UnixListenTask {
    /// Where to listen
    path: PathBuf,

    /// The timeout, which nothing here can use up
    clock: Clock,
}

impl UnixListenTask {
    /// Listens at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            clock: Clock::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Binding a path never waits, so this can't run out. It is
    /// here so every socket task takes one
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Binds and listens
    fn listen(&self) -> Result<UnixListener, RuntimeError> {
        let (fd, bound) = bind_path(&self.path, libc::SOCK_STREAM)?;

        unsafe { libc::listen(fd.raw(), libc::SOMAXCONN) }.check()?;

        Ok(UnixListener::new(fd, bound))
    }
}

/// Takes the next connection off a Unix listener
///
/// ## Returns
/// The connection
#[derive(Debug, Clone)]
pub struct UnixAcceptTask {
    /// Where the connections come from
    listener: UnixListener,

    /// The timeout
    clock: Clock,
}

impl UnixAcceptTask {
    /// Accepts from `listener`
    pub(crate) fn new(listener: UnixListener) -> Self {
        Self {
            listener,
            clock: Clock::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Running out with nobody having connected gives
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

    /// Takes a connection if one is waiting
    fn advance(&mut self) -> Result<Step<Result<UnixConnection, RuntimeError>>, RuntimeError> {
        let fd = self.listener.fd();

        loop {
            let accepted = unsafe { libc::accept(fd, ptr::null_mut(), ptr::null_mut()) }.check();

            match accepted {
                Ok(raw) => {
                    let conn = Fd::new(raw);
                    configure(conn.raw())?;

                    let path = self.listener.path().to_path_buf();

                    return Ok(Step::Done(Ok(UnixConnection::new(conn, path))));
                }

                // Gone again before it could be taken, so the next one
                Err(RuntimeError::CheckError(Some(libc::EINTR | libc::ECONNABORTED))) => {}

                Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => {
                    return self.clock.wait(fd, libc::EVFILT_READ);
                }

                Err(error) => return Err(error),
            }
        }
    }
}

/// Opens a Unix datagram socket
///
/// ## Returns
/// The socket, bound to its path
#[derive(Debug, Clone)]
pub struct UnixBindTask {
    /// Where to bind
    path: PathBuf,

    /// The timeout, which nothing here can use up
    clock: Clock,
}

impl UnixBindTask {
    /// Binds at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            clock: Clock::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Binding a path never waits, so this can't run out. It is
    /// here so every socket task takes one
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Binds
    fn bind(&self) -> Result<UnixDatagram, RuntimeError> {
        let (fd, bound) = bind_path(&self.path, libc::SOCK_DGRAM)?;

        Ok(UnixDatagram::new(fd, bound))
    }
}

/// Sends one datagram to a path
///
/// ## Returns
/// The number of bytes sent, which is all of them
#[derive(Debug, Clone)]
pub struct UnixSendToTask {
    /// What to send from
    socket: UnixDatagram,

    /// Where to send
    path: PathBuf,

    /// What to send
    data: Arc<[u8]>,

    /// The timeout
    clock: Clock,
}

impl UnixSendToTask {
    /// Sends `data` from `socket` to the socket at `path`
    pub(crate) fn new(socket: UnixDatagram, path: PathBuf, data: Arc<[u8]>) -> Self {
        Self {
            socket,
            path,
            data,
            clock: Clock::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// A Unix datagram send never waits, so this can't run out. It
    /// is here so every socket task takes one
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Sends
    fn send(&self) -> Result<usize, RuntimeError> {
        let (raw, len) = to_raw(&self.path)?;

        let sent = send_datagram(
            self.socket.fd(),
            &self.data,
            (&raw as *const libc::sockaddr_un).cast::<libc::sockaddr>(),
            len,
        )?;

        // Room on a Unix datagram socket is the receiver's, and
        // nothing here would say when it came back, so no room is an
        // answer rather than a wait
        sent.ok_or(RuntimeError::CheckError(Some(libc::EAGAIN)))
    }
}

/// Receives one datagram on a Unix socket
///
/// ## Returns
/// The whole datagram, and the path it came from if the sender
/// has one
#[derive(Debug, Clone)]
pub struct UnixRecvFromTask {
    /// Where to receive
    socket: UnixDatagram,

    /// The timeout
    clock: Clock,
}

impl UnixRecvFromTask {
    /// Receives on `socket`
    pub(crate) fn new(socket: UnixDatagram) -> Self {
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
    ) -> Result<Step<Result<(Vec<u8>, Option<PathBuf>), RuntimeError>>, RuntimeError> {
        let fd = self.socket.fd();

        match recv_datagram(fd)? {
            Some((data, storage, len)) => Ok(Step::Done(Ok((data, from_raw(&storage, len))))),
            None => self.clock.wait(fd, libc::EVFILT_READ),
        }
    }
}

impl sealed::Sealed for UnixConnectTask {}
impl sealed::Sealed for UnixListenTask {}
impl sealed::Sealed for UnixAcceptTask {}
impl sealed::Sealed for UnixBindTask {}
impl sealed::Sealed for UnixSendToTask {}
impl sealed::Sealed for UnixRecvFromTask {}

impl Task for UnixConnectTask {
    type Output = Result<UnixConnection, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.trying = Progress::default();
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for UnixListenTask {
    type Output = Result<UnixListener, RuntimeError>;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.listen()
    }

    fn prepare(&mut self) {
        self.clock.start();
    }
}

impl Task for UnixAcceptTask {
    type Output = Result<UnixConnection, RuntimeError>;

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

impl Task for UnixBindTask {
    type Output = Result<UnixDatagram, RuntimeError>;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.bind()
    }

    fn prepare(&mut self) {
        self.clock.start();
    }
}

impl Task for UnixSendToTask {
    type Output = Result<usize, RuntimeError>;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.send()
    }

    fn prepare(&mut self) {
        self.clock.start();
    }
}

impl Task for UnixRecvFromTask {
    type Output = Result<(Vec<u8>, Option<PathBuf>), RuntimeError>;

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
