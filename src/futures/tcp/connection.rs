//! # Connection
//! An open TCP connection and a listening socket, and the
//! tasks each of them starts

use crate::{
    RuntimeError,
    futures::{
        net::{
            exchange::Sends,
            socket::{get_option, set_keepalive, set_option},
            stream::{FinishTask, Pipe, RecvTask, SendTask, Source},
        },
        tcp::tcp_task::AcceptTask,
    },
    modules::fd::Fd,
};
use std::{fmt, net::SocketAddr, sync::Arc, time::Duration};

/// What every copy of one connection shares
struct Stream {
    /// The socket, and what the last receive read past its end
    pipe: Pipe,

    /// This end's address
    local: SocketAddr,

    /// The other end's address
    peer: SocketAddr,
}

/// An open TCP connection
///
/// ## Behaviour
/// Comes back from [`Tcp::connect`] or [`Listener::accept`].
/// Cloning it is cheap and every clone is the same connection
///
/// Every method that sends or receives builds a task, and nothing
/// happens until that task is run
///
/// ```no_run
/// # use atap::{Runtime, tcp::Tcp};
/// # fn main() -> Result<(), atap::RuntimeError> {
/// let conn = Runtime::block(Tcp::connect("127.0.0.1:6379"))?;
///
/// Runtime::block(conn.send(b"PING\r\n".as_slice()))?;
/// let line = Runtime::block(conn.recv_until(b"\r\n", 512))?;
///
/// conn.close();
/// # Ok(())
/// # }
/// ```
///
/// ## Closing
/// The socket closes when the **last** reference to it goes:
/// every clone, every task using it, and every task handle whose
/// output holds one. [`Connection::close`] only lets go of this
/// one
///
/// #### Note
/// Two receives on one connection at once each get part of what
/// arrives, in no useful order. Run them one after the other
///
/// [`Tcp::connect`]: crate::tcp::Tcp::connect
#[derive(Clone)]
pub struct Connection {
    stream: Arc<Stream>,
}

impl Connection {
    /// Takes over a connected socket
    pub(crate) fn new(fd: Fd, local: SocketAddr, peer: SocketAddr) -> Self {
        Self {
            stream: Arc::new(Stream {
                pipe: Pipe::new(fd),
                local,
                peer,
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
        Source::Tcp(self.clone())
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
    /// ## Behaviour
    /// Waits only if nothing has arrived yet
    ///
    /// ## Returns
    /// Between one and `max` bytes. **An empty `Vec` means the
    /// other side closed the connection**, and nothing more is
    /// coming
    pub fn recv(&self, max: usize) -> RecvTask {
        RecvTask::some(self.source(), max)
    }

    /// Receives exactly `len` bytes
    ///
    /// ## Returns
    /// All `len` of them, or [`RuntimeError::Closed`] if the other
    /// side closed first. Whatever had arrived by then is put back
    /// for the next receive
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_exact(&self, len: usize) -> RecvTask {
        RecvTask::exact(self.source(), len)
    }

    /// Receives up to and including `delimiter`
    ///
    /// ## Behaviour
    /// Never hands back anything past the delimiter. Bytes that
    /// arrived after it are kept for the next receive
    ///
    /// ## Returns
    /// Everything up to and including the delimiter.
    /// [`RuntimeError::TooLong`] if `max` bytes go by without it,
    /// and [`RuntimeError::Closed`] if the connection ends first.
    /// Either way what was read is put back
    ///
    /// ```no_run
    /// # use atap::{Runtime, tcp::Tcp};
    /// # fn main() -> Result<(), atap::RuntimeError> {
    /// # let conn = Runtime::block(Tcp::connect("127.0.0.1:6379"))?;
    /// let line = Runtime::block(conn.recv_until(b"\n", 1024))?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// [`RuntimeError::TooLong`]: crate::RuntimeError::TooLong
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_until(&self, delimiter: &[u8], max: usize) -> RecvTask {
        RecvTask::until(self.source(), Arc::from(delimiter), max)
    }

    /// Receives everything until the other side closes the
    /// connection
    ///
    /// #### Note
    /// All of it lands in memory at once
    pub fn recv_to_end(&self) -> RecvTask {
        RecvTask::to_end(self.source())
    }

    /// This end's address
    #[inline(always)]
    pub fn local_addr(&self) -> SocketAddr {
        self.stream.local
    }

    /// The other end's address
    #[inline(always)]
    pub fn peer_addr(&self) -> SocketAddr {
        self.stream.peer
    }

    /// Sends small writes at once rather than waiting to batch
    /// them, or goes back to batching
    pub fn set_nodelay(&self, nodelay: bool) -> Result<(), RuntimeError> {
        set_option(
            self.pipe().fd(),
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            nodelay as libc::c_int,
        )
    }

    /// Whether small writes go at once
    pub fn nodelay(&self) -> Result<bool, RuntimeError> {
        Ok(get_option(self.pipe().fd(), libc::IPPROTO_TCP, libc::TCP_NODELAY)? != 0)
    }

    /// Probes the connection after `idle` without traffic, or stops
    /// probing with `None`
    pub fn set_keepalive(&self, idle: Option<Duration>) -> Result<(), RuntimeError> {
        set_keepalive(self.pipe().fd(), idle)
    }

    /// Tells the other side this one will send no more
    ///
    /// ## Behaviour
    /// Ends the sending half of the connection. This side can
    /// still receive, and the other side's reads end once it has
    /// everything
    pub fn finish(&self) -> FinishTask {
        FinishTask::new(self.source())
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

impl fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("local", &self.stream.local)
            .field("peer", &self.stream.peer)
            .finish()
    }
}

/// What every copy of one listener shares
struct Listening {
    /// The socket, closed when the last copy goes
    fd: Fd,

    /// The address it is bound to
    local: SocketAddr,
}

/// A socket waiting for connections
///
/// ## Behaviour
/// Comes back from [`Tcp::listen`]. Each [`Listener::accept`]
/// takes the next connection. Cloning it is cheap and every
/// clone is the same socket
///
/// ```no_run
/// # use atap::{Runtime, tcp::Tcp};
/// # fn main() -> Result<(), atap::RuntimeError> {
/// let listener = Runtime::block(Tcp::listen("127.0.0.1:0"))?;
///
/// loop {
///     let (conn, peer) = Runtime::block(listener.accept())?;
///     let line = Runtime::block(conn.recv_until(b"\n", 1024))?;
///     Runtime::block(conn.send(line))?;
/// }
/// # }
/// ```
///
/// ## Closing
/// The same as a [`Connection`]. The socket closes once the last
/// reference to it goes, including an accept still waiting
///
/// #### Note
/// `.repeat()` on an accept keeps only the latest connection in
/// its slot, so one that isn't taken before the next arrives is
/// dropped and closed. Accept in a loop instead
///
/// [`Tcp::listen`]: crate::tcp::Tcp::listen
#[derive(Clone)]
pub struct Listener {
    socket: Arc<Listening>,
}

impl Listener {
    /// Takes over a bound, listening socket
    pub(crate) fn new(fd: Fd, local: SocketAddr) -> Self {
        Self {
            socket: Arc::new(Listening { fd, local }),
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
    /// The connection, and the address it came from
    pub fn accept(&self) -> AcceptTask {
        AcceptTask::new(self.clone())
    }

    /// The address it is bound to
    ///
    /// Listening on port 0 picks a free port, and this is where to
    /// find out which
    #[inline(always)]
    pub fn local_addr(&self) -> SocketAddr {
        self.socket.local
    }

    /// Lets go of this handle on the listener
    ///
    /// ## Behaviour
    /// The socket closes once nothing else holds it
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for Listener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Listener")
            .field("local", &self.socket.local)
            .finish()
    }
}

/// A request sends its bytes the same way `send` does
impl Sends for Connection {
    fn send_all(&self, data: Arc<[u8]>) -> SendTask {
        self.send(data)
    }
}
