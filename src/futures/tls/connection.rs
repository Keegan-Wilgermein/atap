//! # TLS connection
//! A TLS session over a TCP connection, a listener that hands
//! them out, and the tasks each starts

use crate::{
    RuntimeError,
    futures::{
        net::stream::{Io, Pipe, RecvTask, SendTask, Source},
        tcp::{Connection, Listener},
        tls::{
            fd_io::FdIo,
            handshake::{io_error, process, send_pending},
            tls_task::TlsAcceptTask,
        },
    },
};
use rustls::ServerConfig;
use std::{
    fmt,
    io::{self, Read, Write},
    net::SocketAddr,
    sync::{Arc, Mutex, MutexGuard},
};

/// What every copy of one TLS connection shares
struct TlsStream {
    /// The session, which every read and write goes through
    tls: Mutex<rustls::Connection>,

    /// The connection it rides on
    ///
    /// Its leftover buffer holds plaintext here, since nothing
    /// reads the encrypted stream directly
    tcp: Connection,
}

/// Reads a closing connection takes off its socket at most, to
/// leave it empty
const DRAIN_READS: usize = 16;

/// Says goodbye properly before the socket closes, so the other
/// side can tell a finished answer from a cut one
///
/// ## Behaviour
/// Anything the other side already sent is taken off the socket
/// first, such as the session tickets a TLS 1.3 server sends
/// after its handshake, which a side that only ever sends never
/// reads. A socket closed with unread bytes in it is reset rather
/// than closed, and a reset throws away whatever the other side
/// hadn't read yet, this goodbye included
///
/// Best effort. It never waits, a full socket loses the goodbye,
/// and anything arriving after the drain can still cause a reset
impl Drop for TlsStream {
    fn drop(&mut self) {
        let fd = self.tcp.pipe().fd();
        let tls = self.tls.get_mut().unwrap_or_else(|poisoned| poisoned.into_inner());

        drain(tls, fd);

        tls.send_close_notify();
        let _ = send_pending(tls, fd);
    }
}

/// Takes whatever has already arrived off the socket and hands it
/// to the session, without waiting
///
/// Stops at the first read that has nothing, fails, or finds the
/// other side gone, or once the session won't take any more
fn drain(tls: &mut rustls::Connection, fd: libc::c_int) {
    for _ in 0..DRAIN_READS {
        match tls.read_tls(&mut FdIo(fd)) {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }

        if tls.process_new_packets().is_err() {
            return;
        }
    }
}

/// An open TLS connection
///
/// ## Behaviour
/// A TCP [`Connection`] with everything on it encrypted, and the
/// other side's certificate checked. It comes back from
/// [`Tls::connect`] or [`TlsListener::accept`], its handshake
/// already done, and its methods build the same send and receive
/// tasks as a TCP one
///
/// ```ignore
/// let conn = Runtime::block(Tls::connect("example.com:443"))?;
///
/// Runtime::block(conn.send(b"GET / HTTP/1.0\r\nHost: example.com\r\n\r\n".as_slice()))?;
/// let answer = Runtime::block(conn.recv_to_end())?;
/// ```
///
/// ## Closing
/// The same as a TCP one: the socket closes when the last
/// reference to it goes. Just before, the other side is told the
/// session is over, so it can tell the end from a cut
///
/// #### Note
/// A connection the other side drops without saying so ends a
/// receive with [`RuntimeError::Closed`] rather than an answer,
/// since it could be somebody cutting the answer short
///
/// [`Connection`]: crate::Connection
/// [`Tls::connect`]: crate::Tls::connect
/// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
#[derive(Clone)]
pub struct TlsConnection {
    stream: Arc<TlsStream>,
}

impl TlsConnection {
    /// Takes over a session whose handshake is done
    pub(crate) fn new(tcp: Connection, tls: rustls::Connection) -> Self {
        Self {
            stream: Arc::new(TlsStream {
                tls: Mutex::new(tls),
                tcp,
            }),
        }
    }

    /// The stream the send and receive tasks keep their leftovers
    /// and their socket in
    #[inline(always)]
    pub(crate) fn pipe(&self) -> &Pipe {
        self.stream.tcp.pipe()
    }

    /// The session, held for one read or write
    fn session(&self) -> MutexGuard<'_, rustls::Connection> {
        self.stream
            .tls
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// This connection, as something a stream task can hold
    #[inline(always)]
    fn source(&self) -> Source {
        Source::Tls(self.clone())
    }

    /// Decrypts up to `room` bytes onto the end of `into`
    ///
    /// ## Behaviour
    /// Hands back whatever is already decrypted first. Otherwise
    /// sends anything the session owes the other side, then reads
    /// more from the socket, and goes round again
    pub(crate) fn read(&self, into: &mut Vec<u8>, room: usize) -> Result<Io, RuntimeError> {
        let fd = self.pipe().fd();
        let mut tls = self.session();

        // Whether the socket has ended, so the next empty look at the
        // session is the last
        let mut ended = false;

        loop {
            let start = into.len();
            into.resize(start + room, 0);

            let read = tls.reader().read(&mut into[start..]);

            match read {
                Ok(0) => {
                    into.truncate(start);

                    return Ok(Io::Closed);
                }

                Ok(read) => {
                    into.truncate(start + read);

                    return Ok(Io::Moved(read));
                }

                Err(error) => {
                    into.truncate(start);

                    match error.kind() {
                        io::ErrorKind::WouldBlock => {}
                        io::ErrorKind::UnexpectedEof => return Ok(Io::Truncated),
                        _ => return Err(io_error(error)),
                    }
                }
            }

            // The socket ended and the session had nothing more to say
            // about it, which can only be a cut
            if ended {
                return Ok(Io::Truncated);
            }

            // Anything the session owes the other side goes first, since
            // the other side may be waiting on it
            if let Some(filter) = send_pending(&mut tls, fd)? {
                return Ok(Io::Wait(filter));
            }

            match tls.read_tls(&mut FdIo(fd)) {
                Ok(0) => ended = true,
                Ok(_) => {}

                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(Io::Wait(libc::EVFILT_READ));
                }

                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(io_error(error)),
            }

            process(&mut tls, fd)?;
        }
    }

    /// Encrypts as much of `data` as the session will take, and
    /// starts it on its way
    pub(crate) fn write(&self, data: &[u8]) -> Result<Io, RuntimeError> {
        let fd = self.pipe().fd();
        let mut tls = self.session();

        // The session only holds so much, so the last lot goes first
        // to make room
        send_pending(&mut tls, fd)?;

        let put = tls.writer().write(data).map_err(io_error)?;

        send_pending(&mut tls, fd)?;

        // Nothing taken means it is full and the socket is too
        match put {
            0 => Ok(Io::Wait(libc::EVFILT_WRITE)),
            put => Ok(Io::Moved(put)),
        }
    }

    /// Sends everything the session is still holding
    ///
    /// ## Returns
    /// `Moved(0)` once it has all gone
    pub(crate) fn flush(&self) -> Result<Io, RuntimeError> {
        let fd = self.pipe().fd();
        let mut tls = self.session();

        match send_pending(&mut tls, fd)? {
            Some(filter) => Ok(Io::Wait(filter)),
            None => Ok(Io::Moved(0)),
        }
    }

    /// Sends every byte of `data`, encrypted
    ///
    /// ## Returns
    /// The number of bytes sent, which is always all of them. It
    /// finishes once they have left for the socket, not once they
    /// are merely encrypted
    ///
    /// #### Note
    /// A send that times out or is cancelled may already have sent
    /// part of `data`
    pub fn send(&self, data: impl Into<Arc<[u8]>>) -> SendTask {
        SendTask::new(self.source(), data.into())
    }

    /// Receives whatever has arrived, up to `max` bytes
    ///
    /// ## Returns
    /// Between one and `max` bytes. **An empty `Vec` means the
    /// other side closed the session properly**
    pub fn recv(&self, max: usize) -> RecvTask {
        RecvTask::some(self.source(), max)
    }

    /// Receives exactly `len` bytes
    ///
    /// ## Returns
    /// All `len` of them, or [`RuntimeError::Closed`] if the other
    /// side closed first
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_exact(&self, len: usize) -> RecvTask {
        RecvTask::exact(self.source(), len)
    }

    /// Receives up to and including `delimiter`
    ///
    /// ## Returns
    /// Everything up to and including the delimiter, with anything
    /// after it kept for the next receive.
    /// [`RuntimeError::TooLong`] if `max` bytes go by without it
    ///
    /// [`RuntimeError::TooLong`]: crate::RuntimeError::TooLong
    pub fn recv_until(&self, delimiter: &[u8], max: usize) -> RecvTask {
        RecvTask::until(self.source(), Arc::from(delimiter), max)
    }

    /// Receives everything until the other side closes the session
    ///
    /// ## Returns
    /// All of it. [`RuntimeError::Closed`] instead if the other side
    /// dropped the connection without closing the session, since
    /// what arrived may be cut short
    ///
    /// [`RuntimeError::Closed`]: crate::RuntimeError::Closed
    pub fn recv_to_end(&self) -> RecvTask {
        RecvTask::to_end(self.source())
    }

    /// This end's address
    #[inline(always)]
    pub fn local_addr(&self) -> SocketAddr {
        self.stream.tcp.local_addr()
    }

    /// The other end's address
    #[inline(always)]
    pub fn peer_addr(&self) -> SocketAddr {
        self.stream.tcp.peer_addr()
    }

    /// Lets go of this handle on the connection
    ///
    /// ## Behaviour
    /// The session is closed properly, and the socket with it, once
    /// nothing else holds it
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for TlsConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsConnection")
            .field("local", &self.local_addr())
            .field("peer", &self.peer_addr())
            .finish()
    }
}

/// A socket waiting for TLS connections
///
/// ## Behaviour
/// Comes back from [`Tls::listen`]. Each [`TlsListener::accept`]
/// takes the next connection and runs its handshake. Cloning it
/// is cheap and every clone is the same socket
///
/// [`Tls::listen`]: crate::Tls::listen
#[derive(Clone)]
pub struct TlsListener {
    /// The socket connections arrive on
    tcp: Listener,

    /// The certificate and key every handshake is done with
    config: Arc<ServerConfig>,
}

impl TlsListener {
    /// Wraps a listening socket with what its handshakes need
    pub(crate) fn new(tcp: Listener, config: Arc<ServerConfig>) -> Self {
        Self { tcp, config }
    }

    /// The socket underneath
    #[inline(always)]
    pub(crate) fn tcp(&self) -> &Listener {
        &self.tcp
    }

    /// The settings every handshake on it uses
    #[inline(always)]
    pub(crate) fn config(&self) -> Arc<ServerConfig> {
        self.config.clone()
    }

    /// Takes the next connection and runs its handshake
    ///
    /// ## Behaviour
    /// Waits for one without holding a thread
    ///
    /// ## Returns
    /// The connection, and the address it came from. A client whose
    /// handshake fails is an error for this accept only, so accept
    /// again for the next one
    pub fn accept(&self) -> TlsAcceptTask {
        TlsAcceptTask::new(self.clone())
    }

    /// The address it is bound to
    #[inline(always)]
    pub fn local_addr(&self) -> SocketAddr {
        self.tcp.local_addr()
    }

    /// Lets go of this handle on the listener
    ///
    /// ## Behaviour
    /// The socket closes once nothing else holds it
    pub fn close(self) {
        drop(self);
    }
}

impl fmt::Debug for TlsListener {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TlsListener")
            .field("local", &self.local_addr())
            .finish()
    }
}
