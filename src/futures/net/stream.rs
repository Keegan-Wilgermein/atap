//! # Stream
//! Sending and receiving on a byte stream, which a TCP
//! connection and a Unix one share

use crate::{
    RuntimeError,
    constants::{FILE_CHUNK, INLINE_PAYLOAD, STEP_BUDGET},
    futures::{
        net::{
            socket::Fd,
            step::{Clock, Progress, settle},
        },
        task::{
            Task,
            sealed::{self, Step},
        },
        tcp::Connection,
        unix::UnixConnection,
    },
    modules::{int_check::IntCheck, park},
};
use std::{
    mem,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

#[cfg(feature = "tls")]
use crate::futures::tls::TlsConnection;

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Vec<u8>, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<usize, RuntimeError>>() <= INLINE_PAYLOAD);

/// A byte stream's socket, and whatever the last receive read
/// past its end
///
/// Shared by every copy of one connection, and closed with the
/// last of them
pub(crate) struct Pipe {
    /// The socket
    fd: Fd,

    /// Bytes a receive read past what it was asked for, which the
    /// next receive takes first
    leftover: Mutex<Vec<u8>>,
}

impl Pipe {
    /// Takes over a connected socket
    pub(crate) fn new(fd: Fd) -> Self {
        Self {
            fd,
            leftover: Mutex::new(Vec::new()),
        }
    }

    /// The socket, for handing to a syscall
    #[inline(always)]
    pub(crate) fn fd(&self) -> libc::c_int {
        self.fd.raw()
    }

    /// The bytes read past the end of an earlier receive
    pub(crate) fn leftover(&self) -> MutexGuard<'_, Vec<u8>> {
        self.leftover
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// The connection a send or a receive is on
///
/// Holding one keeps the connection open, like any other copy
#[derive(Debug, Clone)]
pub(crate) enum Source {
    /// A TCP connection
    Tcp(Connection),

    /// A Unix one
    Unix(UnixConnection),

    /// A TLS session over a TCP connection
    #[cfg(feature = "tls")]
    Tls(TlsConnection),
}

/// What one read or write on a stream came to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Io {
    /// This many bytes went through
    Moved(usize),

    /// The other side closed cleanly, and everything it sent has
    /// been read
    Closed,

    /// The other side went without closing properly, so what
    /// arrived may have been cut short
    ///
    /// Only TLS can tell the two apart
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    Truncated,

    /// Nothing can go through until the socket is ready for this
    /// filter
    Wait(i16),
}

impl Source {
    /// The stream underneath, whichever kind it is
    ///
    /// For TLS, that of the TCP connection it rides on, whose
    /// leftover buffer holds plaintext
    #[inline(always)]
    fn pipe(&self) -> &Pipe {
        match self {
            Self::Tcp(conn) => conn.pipe(),
            Self::Unix(conn) => conn.pipe(),

            #[cfg(feature = "tls")]
            Self::Tls(conn) => conn.pipe(),
        }
    }

    /// Reads up to `room` bytes onto the end of `into`
    fn read(&self, into: &mut Vec<u8>, room: usize) -> Result<Io, RuntimeError> {
        match self {
            Self::Tcp(_) | Self::Unix(_) => read_raw(self.pipe().fd(), into, room),

            #[cfg(feature = "tls")]
            Self::Tls(conn) => conn.read(into, room),
        }
    }

    /// Writes as much of `data` as will go without waiting
    fn write(&self, data: &[u8]) -> Result<Io, RuntimeError> {
        match self {
            Self::Tcp(_) | Self::Unix(_) => write_raw(self.pipe().fd(), data),

            #[cfg(feature = "tls")]
            Self::Tls(conn) => conn.write(data),
        }
    }

    /// Gets out anything a write left waiting inside
    ///
    /// ## Returns
    /// `Moved(0)` once nothing is left. A plain socket never holds
    /// anything back, so only TLS can have to wait
    fn flush(&self) -> Result<Io, RuntimeError> {
        match self {
            Self::Tcp(_) | Self::Unix(_) => Ok(Io::Moved(0)),

            #[cfg(feature = "tls")]
            Self::Tls(conn) => conn.flush(),
        }
    }
}

/// Reads up to `room` bytes from a plain socket onto the end of
/// a buffer
fn read_raw(fd: libc::c_int, into: &mut Vec<u8>, room: usize) -> Result<Io, RuntimeError> {
    into.reserve(room);

    loop {
        let read = unsafe {
            libc::recv(
                fd,
                into.spare_capacity_mut().as_mut_ptr().cast::<libc::c_void>(),
                room,
                0,
            )
        }
        .check();

        match read {
            Ok(0) => return Ok(Io::Closed),

            Ok(read) => {
                // The kernel just wrote `read` bytes into the reserved
                // capacity
                unsafe { into.set_len(into.len() + read as usize) };

                return Ok(Io::Moved(read as usize));
            }

            Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}
            Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => return Ok(Io::Wait(libc::EVFILT_READ)),
            Err(error) => return Err(error),
        }
    }
}

/// Writes as much of `data` to a plain socket as it will take
fn write_raw(fd: libc::c_int, data: &[u8]) -> Result<Io, RuntimeError> {
    loop {
        let put = unsafe { libc::send(fd, data.as_ptr().cast::<libc::c_void>(), data.len(), 0) }
            .check();

        match put {
            Ok(put) => return Ok(Io::Moved(put as usize)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}

            Err(RuntimeError::CheckError(Some(libc::EAGAIN | libc::ENOBUFS))) => {
                return Ok(Io::Wait(libc::EVFILT_WRITE));
            }

            Err(error) => return Err(error),
        }
    }
}

/// Sends every byte of a buffer
///
/// ## Returns
/// The number of bytes sent, which is always all of them
#[derive(Debug, Clone)]
pub struct SendTask {
    /// Where to send
    source: Source,

    /// What to send
    data: Arc<[u8]>,

    /// The timeout
    clock: Clock,

    /// Bytes this run has sent
    sent: Progress<usize>,
}

impl SendTask {
    /// Sends `data` down `source`
    pub(crate) fn new(source: Source, data: Arc<[u8]>) -> Self {
        Self {
            source,
            data,
            clock: Clock::default(),
            sent: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Running out part way gives [`RuntimeError::TimedOut`], and
    /// whatever was already sent stays sent
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Runs to a clock already started, for a task made of other
    /// tasks
    pub(crate) fn timed(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The connection it sends on
    #[inline(always)]
    pub(crate) fn source(&self) -> &Source {
        &self.source
    }

    /// Sends as much as the connection will take right now
    fn advance(&mut self) -> Result<Step<Result<usize, RuntimeError>>, RuntimeError> {
        let fd = self.source.pipe().fd();
        let mut moved = 0;

        loop {
            let sent = self.sent.0;

            // Handed over isn't sent until nothing is held back inside,
            // or TLS's last records would sit there with nobody to send
            // them
            if sent == self.data.len() {
                return match self.source.flush()? {
                    Io::Wait(filter) => self.clock.wait(fd, filter),
                    _ => Ok(Step::Done(Ok(sent))),
                };
            }

            // Enough for one turn. The socket is still writable, so the
            // park comes straight back
            if moved >= STEP_BUDGET {
                return self.clock.wait(fd, libc::EVFILT_WRITE);
            }

            let want = (self.data.len() - sent).min(FILE_CHUNK);

            match self.source.write(&self.data[sent..sent + want])? {
                Io::Moved(put) => {
                    self.sent.0 += put;
                    moved += put;
                }

                Io::Wait(filter) => return self.clock.wait(fd, filter),

                // A write never reports these, but a closed stream is the
                // only thing they could mean
                Io::Closed | Io::Truncated => {
                    return Err(RuntimeError::CheckError(Some(libc::EPIPE)));
                }
            }
        }
    }
}

/// What a receive is waiting for
#[derive(Debug, Clone)]
enum Want {
    /// Whatever has arrived, up to this many bytes
    Some(usize),

    /// Exactly this many bytes
    Exact(usize),

    /// Up to and including the delimiter, within the limit
    Until(Arc<[u8]>, usize),

    /// Everything until the other side closes
    ToEnd,
}

/// How far a receive has got
#[derive(Default)]
struct Reading {
    /// What this run has read
    got: Vec<u8>,

    /// Whether the connection's leftover bytes have been taken
    started: bool,

    /// How much of `got` has been searched for a delimiter
    searched: usize,
}

/// Receives bytes from a connection
///
/// ## Behaviour
/// Starts with whatever an earlier receive read past its end.
/// One that doesn't succeed, whether it times out, is cancelled,
/// or finds the connection closed, puts back everything it read,
/// so the next receive still gets it
///
/// ## Returns
/// The bytes, shaped by which method built it
#[derive(Debug, Clone)]
pub struct RecvTask {
    /// Where to receive from
    source: Source,

    /// What counts as done
    want: Want,

    /// The timeout
    clock: Clock,

    /// How far this run has got
    progress: Progress<Reading>,
}

impl RecvTask {
    /// Receives whatever has arrived, up to `max`
    pub(crate) fn some(source: Source, max: usize) -> Self {
        Self::new(source, Want::Some(max))
    }

    /// Receives exactly `len` bytes
    pub(crate) fn exact(source: Source, len: usize) -> Self {
        Self::new(source, Want::Exact(len))
    }

    /// Receives up to and including `delimiter`
    pub(crate) fn until(source: Source, delimiter: Arc<[u8]>, max: usize) -> Self {
        Self::new(source, Want::Until(delimiter, max))
    }

    /// Receives until the other side closes
    pub(crate) fn to_end(source: Source) -> Self {
        Self::new(source, Want::ToEnd)
    }

    fn new(source: Source, want: Want) -> Self {
        Self {
            source,
            want,
            clock: Clock::default(),
            progress: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Running out gives [`RuntimeError::TimedOut`], and whatever
    /// had been read is put back for the next receive
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.limit(timeout);
        self
    }

    /// Runs to a clock already started, for a task made of other
    /// tasks
    pub(crate) fn timed(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// Reads as much as there is to read right now
    fn advance(&mut self) -> Result<Step<Result<Vec<u8>, RuntimeError>>, RuntimeError> {
        if !self.progress.0.started {
            self.progress.0.started = true;

            if let Some(done) = self.take_leftover() {
                return Ok(Step::Done(Ok(done)));
            }
        }

        let fd = self.source.pipe().fd();
        let mut moved = 0;

        loop {
            if let Some(done) = self.done()? {
                return Ok(Step::Done(Ok(done)));
            }

            let got = self.progress.0.got.len();

            // Enough for one turn. A `recv` hands back what it has, and
            // the rest park with the socket still readable, so they come
            // straight back
            if moved >= STEP_BUDGET {
                if matches!(self.want, Want::Some(_)) {
                    return Ok(Step::Done(Ok(self.take())));
                }

                return self.clock.wait(fd, libc::EVFILT_READ);
            }

            let room = match &self.want {
                Want::Some(max) => max - got,
                Want::Exact(len) => len - got,
                Want::Until(_, _) | Want::ToEnd => FILE_CHUNK,
            };

            match self.source.read(&mut self.progress.0.got, room.min(FILE_CHUNK))? {
                // The other side closed
                Io::Closed => {
                    return match self.want {
                        Want::Some(_) | Want::ToEnd => Ok(Step::Done(Ok(self.take()))),
                        Want::Exact(_) | Want::Until(_, _) => Err(RuntimeError::Closed),
                    };
                }

                // Possibly cut short on purpose, so never a clean end
                Io::Truncated => return Err(RuntimeError::Closed),

                Io::Moved(read) => moved += read,

                Io::Wait(filter) => {
                    // A `recv` only waits if it has nothing at all
                    if matches!(self.want, Want::Some(_)) && got > 0 {
                        return Ok(Step::Done(Ok(self.take())));
                    }

                    return self.clock.wait(fd, filter);
                }
            }
        }
    }

    /// Takes over whatever an earlier receive read past its end
    ///
    /// ## Returns
    /// The output, when the leftover alone is enough for it
    fn take_leftover(&mut self) -> Option<Vec<u8>> {
        // Nothing asked for is done before it starts
        if let Want::Some(0) | Want::Exact(0) = self.want {
            return Some(Vec::new());
        }

        let mut leftover = self.source.pipe().leftover();

        if leftover.is_empty() {
            return None;
        }

        let limit = match self.want {
            Want::Some(max) => max,
            Want::Exact(len) => len,
            Want::Until(_, _) | Want::ToEnd => usize::MAX,
        };

        let split = limit.min(leftover.len());
        let rest = leftover.split_off(split);
        let taken = mem::replace(&mut *leftover, rest);

        drop(leftover);

        // Something has arrived, which is all a `recv` waits for
        if let Want::Some(_) = self.want {
            return Some(taken);
        }

        self.progress.0.got = taken;

        None
    }

    /// Whether what has been read is enough
    ///
    /// ## Returns
    /// The output if it is. `TooLong` if a delimiter can no longer
    /// be found in time
    fn done(&mut self) -> Result<Option<Vec<u8>>, RuntimeError> {
        let reading = &mut self.progress.0;

        match &self.want {
            Want::Some(max) => Ok((reading.got.len() >= *max).then(|| mem::take(&mut reading.got))),

            Want::Exact(len) => Ok((reading.got.len() >= *len).then(|| mem::take(&mut reading.got))),

            Want::ToEnd => Ok(None),

            Want::Until(delimiter, max) => {
                // Backed up, since a delimiter can straddle two reads
                let from = reading.searched.saturating_sub(delimiter.len().saturating_sub(1));

                let found = match delimiter.is_empty() {
                    true => Some(0),
                    false => reading.got[from..]
                        .windows(delimiter.len())
                        .position(|window| window == &delimiter[..])
                        .map(|at| from + at),
                };

                reading.searched = reading.got.len();

                let Some(at) = found else {
                    return match reading.got.len() >= *max {
                        true => Err(RuntimeError::TooLong),
                        false => Ok(None),
                    };
                };

                let end = at + delimiter.len();

                if end > *max {
                    return Err(RuntimeError::TooLong);
                }

                // Whatever came after the delimiter is the next receive's
                let rest = reading.got.split_off(end);
                let line = mem::take(&mut reading.got);

                put_front(self.source.pipe(), rest);

                Ok(Some(line))
            }
        }
    }

    /// Moves what this run has read out, as its output
    #[inline(always)]
    fn take(&mut self) -> Vec<u8> {
        mem::take(&mut self.progress.0.got)
    }

    /// Hands back everything this run read, for the next receive
    fn put_back(&mut self) {
        let got = self.take();
        put_front(self.source.pipe(), got);
    }
}

/// Puts bytes in front of whatever a connection already had left
/// over
fn put_front(pipe: &Pipe, mut bytes: Vec<u8>) {
    if bytes.is_empty() {
        return;
    }

    let mut leftover = pipe.leftover();

    bytes.extend_from_slice(&leftover);
    *leftover = bytes;
}

/// A receive dropped part way, because it was cancelled or the
/// runtime shut down, leaves what it read for the next one
impl Drop for RecvTask {
    fn drop(&mut self) {
        self.put_back();
    }
}

/// Steps a receive, putting back what it read if it fails
fn settle_recv(read: &mut RecvTask) -> Step<Result<Vec<u8>, RuntimeError>> {
    match read.advance() {
        Ok(step) => step,
        Err(error) => {
            read.put_back();

            Step::Done(Err(error))
        }
    }
}

impl sealed::Sealed for SendTask {}
impl sealed::Sealed for RecvTask {}

impl Task for SendTask {
    type Output = Result<usize, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.sent = Progress::default();
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle(self.advance())
    }
}

impl Task for RecvTask {
    type Output = Result<Vec<u8>, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        // Nothing should be left from the last run, but if it is it
        // belongs to the connection
        self.put_back();

        self.clock.start();
        self.progress = Progress::default();
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        settle_recv(self)
    }
}
