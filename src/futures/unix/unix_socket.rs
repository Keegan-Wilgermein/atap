//! # Unix socket
//! Unix connections, listeners and datagram sockets, and the
//! tasks each of them starts

use crate::futures::{
    net::{
        socket::Fd,
        stream::{Pipe, RecvTask, SendTask, Source},
    },
    unix::{
        path::Bound,
        unix_task::{UnixAcceptTask, UnixRecvFromTask, UnixSendToTask},
    },
};
use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

/// What every copy of one connection shares
struct UnixStream {
    /// The socket, and what the last receive read past its end
    pipe: Pipe,

    /// The path it was made to, or accepted on
    path: PathBuf,
}

/// An open Unix connection
///
/// ## Behaviour
/// The same as a TCP [`Connection`], between two programs on this
/// machine. It comes back from [`Unix::connect`] or
/// [`UnixListener::accept`], and its methods build the same send
/// and receive tasks
///
/// ```ignore
/// let conn = Runtime::block(Unix::connect("/tmp/app.sock"))?;
///
/// Runtime::block(conn.send(b"status\n".as_slice()))?;
/// let line = Runtime::block(conn.recv_until(b"\n", 1024))?;
/// ```
///
/// ## Closing
/// The socket closes when the **last** reference to it goes:
/// every clone, every task using it, and every task handle whose
/// output holds one
///
/// #### Note
/// Two receives on one connection at once each get part of what
/// arrives, in no useful order. Run them one after the other
///
/// [`Connection`]: crate::Connection
/// [`Unix::connect`]: crate::Unix::connect
#[derive(Clone)]
pub struct UnixConnection {
    stream: Arc<UnixStream>,
}

impl UnixConnection {
    /// Takes over a connected socket
    pub(crate) fn new(fd: Fd, path: PathBuf) -> Self {
        Self {
            stream: Arc::new(UnixStream {
                pipe: Pipe::new(fd),
                path,
            }),
        }
    }

    /// The stream the send and receive tasks work on
    #[inline(always)]
    pub(crate) fn pipe(&self) -> &Pipe {
        &self.stream.pipe
    }

    /// This connection, as something a stream task can hold
    #[inline(always)]
    fn source(&self) -> Source {
        Source::Unix(self.clone())
    }

    /// Sends every byte of `data`
    ///
    /// ## Behaviour
    /// Waits whenever the connection can't take any more, without
    /// holding a thread
    ///
    /// ## Returns
    /// The number of bytes sent, which is always all of them
    /// whenever this isn't an error
    ///
    /// #### Note
    /// A send that times out or is cancelled may already have sent
    /// part of `data`, and that part can't be taken back
    pub fn send(&self, data: impl Into<Arc<[u8]>>) -> SendTask {
        SendTask::new(self.source(), data.into())
    }

    /// Receives whatever has arrived, up to `max` bytes
    ///
    /// ## Returns
    /// Between one and `max` bytes. **An empty `Vec` means the
    /// other side closed the connection**
    pub fn recv(&self, max: usize) -> RecvTask {
        RecvTask::some(self.source(), max)
    }

    /// Receives exactly `len` bytes
    ///
    /// ## Returns
    /// All `len` of them, or [`RuntimeError::Closed`] if the other
    /// side closed first. Whatever had arrived by then is put back
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_exact(&self, len: usize) -> RecvTask {
        RecvTask::exact(self.source(), len)
    }

    /// Receives up to and including `delimiter`
    ///
    /// ## Returns
    /// Everything up to and including the delimiter.
    /// [`RuntimeError::TooLong`] if `max` bytes go by without it,
    /// and [`RuntimeError::Closed`] if the connection ends first.
    /// Either way what was read is put back
    ///
    /// [`RuntimeError::TooLong`]: crate::RuntimeError::TooLong
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_until(&self, delimiter: &[u8], max: usize) -> RecvTask {
        RecvTask::until(self.source(), Arc::from(delimiter), max)
    }

    /// Receives everything until the other side closes the
    /// connection
    pub fn recv_to_end(&self) -> RecvTask {
        RecvTask::to_end(self.source())
    }

    /// The path this connection was made to, or accepted on
    #[inline(always)]
    pub fn path(&self) -> &Path {
        &self.stream.path
    }

    /// Lets go of this handle on the connection
    ///
    /// ## Behaviour
    /// The socket closes once nothing else holds it. A task still
    /// using it runs to its end, and an output already received
    /// stays readable
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for UnixConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnixConnection")
            .field("path", &self.stream.path)
            .finish()
    }
}

/// What every copy of one listener shares
///
/// The socket is closed before its file is removed, by field
/// order
struct UnixListening {
    /// The socket
    fd: Fd,

    /// The file the bind made
    bound: Bound,
}

/// A Unix socket waiting for connections
///
/// ## Behaviour
/// Comes back from [`Unix::listen`]. Each
/// [`UnixListener::accept`] takes the next connection. Cloning it
/// is cheap and every clone is the same socket
///
/// ## Closing
/// The socket closes once the last reference to it goes, and
/// **its socket file is removed** then too, so the path can be
/// listened on again. A file somebody else has since put at the
/// same path is left alone
///
/// [`Unix::listen`]: crate::Unix::listen
#[derive(Clone)]
pub struct UnixListener {
    socket: Arc<UnixListening>,
}

impl UnixListener {
    /// Takes over a bound, listening socket
    pub(crate) fn new(fd: Fd, bound: Bound) -> Self {
        Self {
            socket: Arc::new(UnixListening { fd, bound }),
        }
    }

    /// The socket, for handing to a syscall
    #[inline(always)]
    pub(crate) fn fd(&self) -> libc::c_int {
        self.socket.fd.raw()
    }

    /// Takes the next connection
    ///
    /// ## Behaviour
    /// Waits for one to arrive without holding a thread
    ///
    /// ## Returns
    /// The connection. A Unix client almost never has a path of its
    /// own, so there is no address to hand back beside it
    pub fn accept(&self) -> UnixAcceptTask {
        UnixAcceptTask::new(self.clone())
    }

    /// The path it is listening on
    #[inline(always)]
    pub fn path(&self) -> &Path {
        self.socket.bound.path()
    }

    /// Lets go of this handle on the listener
    ///
    /// ## Behaviour
    /// The socket closes, and its file is removed, once nothing
    /// else holds it
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for UnixListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnixListener")
            .field("path", &self.path())
            .finish()
    }
}

/// What every copy of one datagram socket shares
///
/// The socket is closed before its file is removed, by field
/// order
struct UnixDatagrams {
    /// The socket
    fd: Fd,

    /// The file the bind made
    bound: Bound,
}

/// A Unix datagram socket, bound to a path
///
/// ## Behaviour
/// Comes back from [`Unix::bind`]. Like a [`UdpSocket`], between
/// programs on this machine: each send names the path it goes
/// to, and each receive says which path it came from
///
/// ```ignore
/// let socket = Runtime::block(Unix::bind("/tmp/me.sock"))?;
///
/// Runtime::block(socket.send_to("/tmp/them.sock", b"ping".as_slice()))?;
/// let (reply, from) = Runtime::block(socket.recv_from())?;
/// ```
///
/// ## Datagrams
/// Every send is one datagram and every receive takes one, whole.
/// Unlike UDP, nothing is lost or reordered on the way
///
/// ## Closing
/// The socket closes once the last reference to it goes, and
/// **its socket file is removed** then too
///
/// #### Note
/// macOS caps a Unix datagram at 2048 bytes by default, set by
/// `net.local.dgram.maxdgram`. A larger one is refused as
/// `CheckError(Some(EMSGSIZE))`
///
/// [`Unix::bind`]: crate::Unix::bind
/// [`UdpSocket`]: crate::UdpSocket
#[derive(Clone)]
pub struct UnixDatagram {
    socket: Arc<UnixDatagrams>,
}

impl UnixDatagram {
    /// Takes over a bound socket
    pub(crate) fn new(fd: Fd, bound: Bound) -> Self {
        Self {
            socket: Arc::new(UnixDatagrams { fd, bound }),
        }
    }

    /// The socket, for handing to a syscall
    #[inline(always)]
    pub(crate) fn fd(&self) -> libc::c_int {
        self.socket.fd.raw()
    }

    /// Sends `data` to the socket bound at `path`, as one datagram
    ///
    /// ## Returns
    /// The number of bytes sent, which is all of them. Nobody bound
    /// at the path is `CheckError(Some(ENOENT))`, or
    /// `ECONNREFUSED` for a file left behind with nobody on it
    ///
    /// #### Note
    /// A receiver with no room left is an error rather than a wait,
    /// since nothing on this socket says when room comes back.
    /// Sending again later is up to the caller
    pub fn send_to(&self, path: impl AsRef<Path>, data: impl Into<Arc<[u8]>>) -> UnixSendToTask {
        UnixSendToTask::new(self.clone(), path.as_ref().to_path_buf(), data.into())
    }

    /// Receives the next datagram
    ///
    /// ## Behaviour
    /// Waits for one to arrive without holding a thread
    ///
    /// ## Returns
    /// The whole datagram, and the path of the socket that sent
    /// it. `None` for a sender that isn't bound to one
    pub fn recv_from(&self) -> UnixRecvFromTask {
        UnixRecvFromTask::new(self.clone())
    }

    /// The path it is bound to
    #[inline(always)]
    pub fn path(&self) -> &Path {
        self.socket.bound.path()
    }

    /// Lets go of this handle on the socket
    ///
    /// ## Behaviour
    /// The socket closes, and its file is removed, once nothing
    /// else holds it
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for UnixDatagram {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UnixDatagram")
            .field("path", &self.path())
            .finish()
    }
}
