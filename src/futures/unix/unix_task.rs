//! # Unix task
//! The tasks the `Unix` constructors, a `UnixListener` and a
//! `UnixDatagram` return, and everything they do once run

use crate::modules::input::Token;
use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            datagram::{recv_datagram, send_datagram},
            socket::{Options, begin_connect, configure, finished_connecting, open},
            step::{Progress, settle, wait_on},
        },
        task::{
            Nothing, Task,
            sealed::{self, Step},
        },
        unix::{
            path::{Bound, from_raw, to_raw},
            unix_socket::{UnixConnection, UnixDatagram, UnixListener},
        },
    },
    modules::{fd::Fd, int_check::IntCheck, park},
};
use std::{
    mem,
    path::{Path, PathBuf},
    ptr,
    sync::Arc,
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<UnixConnection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<UnixListener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<UnixDatagram, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () =
    assert!(mem::size_of::<Result<(Vec<u8>, Option<PathBuf>), RuntimeError>>() <= INLINE_PAYLOAD);

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
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixConnectTask {
    /// Where to connect
    path: PathBuf,

    /// A socket part way through connecting
    trying: Progress<Option<Fd>>,
}

impl UnixConnectTask {
    /// Connects to the socket at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
            trying: Progress::default(),
        }
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

        let step = wait_on(fd.raw(), libc::EVFILT_WRITE)?;
        self.trying.0 = Some(fd);

        Ok(step)
    }
}

/// Opens a Unix socket that waits for connections
///
/// ## Returns
/// The listener
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixListenTask {
    /// Where to listen
    path: PathBuf,

    /// What the socket is set up with
    options: Options,
}

impl UnixListenTask {
    /// Listens at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self {
            path,
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

    /// Binds and listens
    fn listen(&self) -> Result<UnixListener, RuntimeError> {
        let (fd, bound) = bind_path(&self.path, libc::SOCK_STREAM)?;

        unsafe { libc::listen(fd.raw(), self.options.backlog()) }.check()?;

        Ok(UnixListener::new(fd, bound))
    }
}

/// Takes the next connection off a Unix listener
///
/// ## Returns
/// The connection
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixAcceptTask {
    /// Where the connections come from
    listener: UnixListener,
}

impl UnixAcceptTask {
    /// Accepts from `listener`
    pub(crate) fn new(listener: UnixListener) -> Self {
        Self { listener }
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
                    return wait_on(fd, libc::EVFILT_READ);
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
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixBindTask {
    /// Where to bind, or `None` for a socket with no path
    path: Option<PathBuf>,
}

impl UnixBindTask {
    /// Binds at `path`
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    /// Opens a socket bound nowhere
    pub(crate) fn unbound() -> Self {
        Self { path: None }
    }

    /// Binds
    fn bind(&self) -> Result<UnixDatagram, RuntimeError> {
        let (fd, bound) = match &self.path {
            Some(path) => bind_path(path, libc::SOCK_DGRAM)?,
            None => (open(libc::AF_UNIX, libc::SOCK_DGRAM)?, Bound::unbound()),
        };

        Ok(UnixDatagram::new(fd, bound))
    }
}

/// Opens both ends of a connection, with nothing on disk
///
/// ## Returns
/// The two connected ends
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixPairTask;

impl UnixPairTask {
    fn pair(&self) -> Result<(UnixConnection, UnixConnection), RuntimeError> {
        let mut ends = [0; 2];

        unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, ends.as_mut_ptr()) }
            .check()?;

        let (first, second) = (Fd::new(ends[0]), Fd::new(ends[1]));

        configure(first.raw())?;
        configure(second.raw())?;

        Ok((
            UnixConnection::new(first, PathBuf::new()),
            UnixConnection::new(second, PathBuf::new()),
        ))
    }
}

/// Sends one datagram to a path
///
/// ## Returns
/// The number of bytes sent, which is all of them
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixSendToTask {
    /// What to send from
    socket: UnixDatagram,

    /// Where to send
    path: PathBuf,

    /// What to send
    data: Arc<[u8]>,
}

impl UnixSendToTask {
    /// Sends `data` from `socket` to the socket at `path`
    pub(crate) fn new(socket: UnixDatagram, path: PathBuf, data: Arc<[u8]>) -> Self {
        Self { socket, path, data }
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

        // No room is an answer rather than a wait
        sent.ok_or(RuntimeError::CheckError(Some(libc::EAGAIN)))
    }
}

/// Receives one datagram on a Unix socket
///
/// ## Returns
/// The whole datagram, and the path it came from if the sender
/// has one
#[derive(Debug, Clone)]
#[must_use = "a task does nothing until it is run or spawned"]
pub struct UnixRecvFromTask {
    /// Where to receive
    socket: UnixDatagram,
}

impl UnixRecvFromTask {
    /// Receives on `socket`
    pub(crate) fn new(socket: UnixDatagram) -> Self {
        Self { socket }
    }

    /// Takes a datagram if one is waiting
    fn advance(
        &mut self,
    ) -> Result<Step<Result<(Vec<u8>, Option<PathBuf>), RuntimeError>>, RuntimeError> {
        let fd = self.socket.fd();

        match recv_datagram(fd, false)? {
            Some((data, storage, len)) => Ok(Step::Done(Ok((data, from_raw(&storage, len))))),
            None => wait_on(fd, libc::EVFILT_READ),
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
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self, _token: Token) {
        self.trying = Progress::default();
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for UnixListenTask {
    type Output = Result<UnixListener, RuntimeError>;
    type Input = Nothing;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.listen()
    }
}

impl Task for UnixAcceptTask {
    type Output = Result<UnixConnection, RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl sealed::Sealed for UnixPairTask {}

impl Task for UnixPairTask {
    type Output = Result<(UnixConnection, UnixConnection), RuntimeError>;
    type Input = Nothing;

    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.pair()
    }
}

impl Task for UnixBindTask {
    type Output = Result<UnixDatagram, RuntimeError>;
    type Input = Nothing;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.bind()
    }
}

impl Task for UnixSendToTask {
    type Output = Result<usize, RuntimeError>;
    type Input = Nothing;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _token: Token, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.send()
    }
}

impl Task for UnixRecvFromTask {
    type Output = Result<(Vec<u8>, Option<PathBuf>), RuntimeError>;
    type Input = Nothing;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, _token: Token, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn step(&mut self, _token: Token, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}
