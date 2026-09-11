//! # TCP task
//! The tasks the `Tcp` constructors, a `Connection` and a
//! `Listener` return, and everything they do once run
//!
//! Every task here that waits on a socket does so in steps.
//! A step does what it can without waiting, then parks, and
//! the runtime steps it again once the socket is ready. A
//! spawned one holds no thread while it waits

use crate::{
    RuntimeError,
    constants::{FILE_CHUNK, INLINE_PAYLOAD, STEP_BUDGET},
    futures::{
        task::{
            Task,
            sealed::{self, Park, Step},
        },
        tcp::{
            address::{Target, from_raw, local_of, peer_of, to_raw},
            connection::{Connection, Fd, Listener, configure, open_socket, set_flag},
        },
    },
    modules::{int_check::IntCheck, park},
};
use std::{
    fmt, mem,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Connection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Listener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(
    mem::size_of::<Result<(Connection, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD
);
const _: () = assert!(mem::size_of::<Result<Vec<u8>, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<usize, RuntimeError>>() <= INLINE_PAYLOAD);

/// A task's timeout, and the deadline one run of it works to
#[derive(Debug, Clone, Copy, Default)]
struct Clock {
    /// How long a run may take, if it has a limit
    timeout: Option<Duration>,

    /// When the current run has to be done by
    deadline: Option<Instant>,
}

impl Clock {
    /// Starts the clock on a run
    fn start(&mut self) {
        self.deadline = self
            .timeout
            .and_then(|timeout| Instant::now().checked_add(timeout));
    }

    /// Whether the run is out of time
    fn expired(&self) -> bool {
        self.deadline.is_some_and(|deadline| Instant::now() >= deadline)
    }

    /// Parks on `fd` until it is ready for `filter`, or the
    /// deadline comes
    ///
    /// Out of time already is a timeout instead
    fn wait<T>(&self, fd: libc::c_int, filter: i16) -> Result<Step<T>, RuntimeError> {
        if self.expired() {
            return Err(RuntimeError::TimedOut);
        }

        Ok(Step::Park(Park {
            fd,
            filter,
            deadline: self.deadline,
        }))
    }
}

/// Where one run of a task has got to
///
/// A clone starts from the beginning, since a clone is always a
/// fresh run: the copy a blocking call drives, or the next run
/// of a schedule
#[derive(Default)]
struct Progress<T: Default>(T);

impl<T: Default> Clone for Progress<T> {
    fn clone(&self) -> Self {
        Self::default()
    }
}

impl<T: Default> fmt::Debug for Progress<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("..")
    }
}

/// Turns a step that failed into the step that reports it
#[inline(always)]
fn settle<T>(step: Result<Step<Result<T, RuntimeError>>, RuntimeError>) -> Step<Result<T, RuntimeError>> {
    step.unwrap_or_else(|error| Step::Done(Err(error)))
}

/// Opens a connection
///
/// ## Returns
/// The connection. Each address a name looks up to is tried in
/// turn, and the error from the last one comes back if none of
/// them take
#[derive(Debug, Clone)]
pub struct ConnectTask {
    /// Where to connect
    target: Target,

    /// The timeout, which covers every address tried
    clock: Clock,

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
            clock: Clock::default(),
            progress: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Counted from when the task starts, across every address it
    /// tries. Running out gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// #### Note
    /// A name lookup can't be interrupted, so one that is slow in
    /// itself can run past the timeout before it is noticed
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.timeout = Some(timeout);
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
                        let step = self.clock.wait(fd.raw(), libc::EVFILT_WRITE)?;
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

            if self.clock.expired() {
                return Err(RuntimeError::TimedOut);
            }

            match start_connect(&addr) {
                Ok(Started::Connected(fd)) => return Ok(Step::Done(connected(fd, addr))),
                Ok(Started::Waiting(fd)) => state.trying = Some((fd, addr)),
                Err(error) => state.failure = Some(error),
            }
        }
    }
}

/// Starts a connect on a fresh socket
fn start_connect(addr: &SocketAddr) -> Result<Started, RuntimeError> {
    let fd = open_socket(addr)?;
    let (raw, len) = to_raw(addr);

    let result = unsafe {
        libc::connect(
            fd.raw(),
            (&raw as *const libc::sockaddr_storage).cast::<libc::sockaddr>(),
            len,
        )
    }
    .check();

    match result {
        Ok(_) => Ok(Started::Connected(fd)),

        // Non-blocking, so it carries on without this thread. An
        // interrupted one carries on too
        Err(RuntimeError::CheckError(Some(libc::EINPROGRESS | libc::EINTR))) => {
            Ok(Started::Waiting(fd))
        }

        Err(error) => Err(error),
    }
}

/// Whether a connect started earlier has got anywhere
///
/// ## Returns
/// `true` once connected, `false` while it is still under way,
/// and why it failed if it did
fn finished_connecting(fd: libc::c_int) -> Result<bool, RuntimeError> {
    let mut error: libc::c_int = 0;
    let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;

    unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_ERROR,
            (&mut error as *mut libc::c_int).cast::<libc::c_void>(),
            &mut len,
        )
    }
    .check()?;

    if error != 0 {
        return Err(RuntimeError::CheckError(Some(error)));
    }

    // No error yet isn't the same as connected, since a wake can
    // come before the socket is ready
    match peer_of(fd) {
        Ok(_) => Ok(true),
        Err(RuntimeError::CheckError(Some(libc::ENOTCONN))) => Ok(false),
        Err(error) => Err(error),
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
pub struct ListenTask {
    /// Where to listen
    target: Target,

    /// The timeout, which only a name lookup can use up
    clock: Clock,
}

impl ListenTask {
    /// Listens on `target`
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
        self.clock.timeout = Some(timeout);
        self
    }

    /// Binds and listens
    fn listen(&self) -> Result<Listener, RuntimeError> {
        let found = self.target.resolve()?;

        if self.clock.expired() {
            return Err(RuntimeError::TimedOut);
        }

        let mut failure = RuntimeError::BadAddress;

        for addr in found {
            match bind_listen(&addr) {
                Ok(listener) => return Ok(listener),
                Err(error) => failure = error,
            }
        }

        Err(failure)
    }
}

/// Binds a fresh socket to `addr` and starts it listening
fn bind_listen(addr: &SocketAddr) -> Result<Listener, RuntimeError> {
    let fd = open_socket(addr)?;

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

    unsafe { libc::listen(fd.raw(), libc::SOMAXCONN) }.check()?;

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
pub struct AcceptTask {
    /// Where the connections come from
    listener: Listener,

    /// The timeout
    clock: Clock,
}

impl AcceptTask {
    /// Accepts from `listener`
    pub(crate) fn new(listener: Listener) -> Self {
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
        self.clock.timeout = Some(timeout);
        self
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
                    return self.clock.wait(fd, libc::EVFILT_READ);
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

/// Sends every byte of a buffer
///
/// ## Returns
/// The number of bytes sent, which is always all of them
#[derive(Debug, Clone)]
pub struct SendTask {
    /// Where to send
    conn: Connection,

    /// What to send
    data: Arc<[u8]>,

    /// The timeout
    clock: Clock,

    /// Bytes this run has sent
    sent: Progress<usize>,
}

impl SendTask {
    /// Sends `data` down `conn`
    pub(crate) fn new(conn: Connection, data: Arc<[u8]>) -> Self {
        Self {
            conn,
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
        self.clock.timeout = Some(timeout);
        self
    }

    /// Sends as much as the connection will take right now
    fn advance(&mut self) -> Result<Step<Result<usize, RuntimeError>>, RuntimeError> {
        let fd = self.conn.fd();
        let mut moved = 0;

        loop {
            let sent = self.sent.0;

            if sent == self.data.len() {
                return Ok(Step::Done(Ok(sent)));
            }

            // Enough for one turn. The socket is still writable, so the
            // park comes straight back
            if moved >= STEP_BUDGET {
                return self.clock.wait(fd, libc::EVFILT_WRITE);
            }

            let want = (self.data.len() - sent).min(FILE_CHUNK);

            // `sent` never passes the length
            let from = unsafe { self.data.as_ptr().add(sent) }.cast::<libc::c_void>();

            match unsafe { libc::send(fd, from, want, 0) }.check() {
                Ok(put) => {
                    self.sent.0 += put as usize;
                    moved += put as usize;
                }

                Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}

                Err(RuntimeError::CheckError(Some(libc::EAGAIN | libc::ENOBUFS))) => {
                    return self.clock.wait(fd, libc::EVFILT_WRITE);
                }

                Err(error) => return Err(error),
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
    conn: Connection,

    /// What counts as done
    want: Want,

    /// The timeout
    clock: Clock,

    /// How far this run has got
    progress: Progress<Reading>,
}

impl RecvTask {
    /// Receives whatever has arrived, up to `max`
    pub(crate) fn some(conn: Connection, max: usize) -> Self {
        Self::new(conn, Want::Some(max))
    }

    /// Receives exactly `len` bytes
    pub(crate) fn exact(conn: Connection, len: usize) -> Self {
        Self::new(conn, Want::Exact(len))
    }

    /// Receives up to and including `delimiter`
    pub(crate) fn until(conn: Connection, delimiter: Arc<[u8]>, max: usize) -> Self {
        Self::new(conn, Want::Until(delimiter, max))
    }

    /// Receives until the other side closes
    pub(crate) fn to_end(conn: Connection) -> Self {
        Self::new(conn, Want::ToEnd)
    }

    fn new(conn: Connection, want: Want) -> Self {
        Self {
            conn,
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
        self.clock.timeout = Some(timeout);
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

        let fd = self.conn.fd();
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

            match read_into(fd, &mut self.progress.0.got, room.min(FILE_CHUNK)) {
                // The other side closed
                Ok(0) => {
                    return match self.want {
                        Want::Some(_) | Want::ToEnd => Ok(Step::Done(Ok(self.take()))),
                        Want::Exact(_) | Want::Until(_, _) => Err(RuntimeError::Closed),
                    };
                }

                Ok(read) => moved += read,

                Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}

                Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => {
                    // A `recv` only waits if it has nothing at all
                    if matches!(self.want, Want::Some(_)) && got > 0 {
                        return Ok(Step::Done(Ok(self.take())));
                    }

                    return self.clock.wait(fd, libc::EVFILT_READ);
                }

                Err(error) => return Err(error),
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

        let mut leftover = self.conn.leftover();

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

                put_front(&self.conn, rest);

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
        put_front(&self.conn, got);
    }
}

/// Puts bytes in front of whatever a connection already had left
/// over
fn put_front(conn: &Connection, mut bytes: Vec<u8>) {
    if bytes.is_empty() {
        return;
    }

    let mut leftover = conn.leftover();

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

/// Reads up to `room` bytes onto the end of a buffer
///
/// ## Returns
/// How many arrived. Zero means the other side closed
fn read_into(fd: libc::c_int, into: &mut Vec<u8>, room: usize) -> Result<usize, RuntimeError> {
    into.reserve(room);

    let read = unsafe {
        libc::recv(
            fd,
            into.spare_capacity_mut().as_mut_ptr().cast::<libc::c_void>(),
            room,
            0,
        )
    }
    .check()? as usize;

    // The kernel just wrote `read` bytes into the reserved capacity
    unsafe { into.set_len(into.len() + read) };

    Ok(read)
}

/// Connects, sends, and reads the answer to the end
///
/// ## Returns
/// Everything the other side sent before it closed the
/// connection
#[derive(Debug, Clone)]
pub struct RequestTask {
    /// How it connects
    connect: ConnectTask,

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
    /// Opening the connection
    #[default]
    Connecting,

    /// Sending the request down it
    Sending(SendTask),

    /// Reading the answer
    Reading(RecvTask),
}

impl RequestTask {
    /// Sends `data` to `target` and reads what comes back
    pub(crate) fn new(target: Target, data: Arc<[u8]>) -> Self {
        Self {
            connect: ConnectTask::new(target),
            data,
            clock: Clock::default(),
            stage: Progress::default(),
        }
    }

    /// Gives up once `timeout` has passed
    ///
    /// ## Behaviour
    /// Covers the whole exchange: connecting, sending, and reading
    /// the answer. Running out gives [`RuntimeError::TimedOut`]
    ///
    /// ## Returns
    /// The task. Calling it twice keeps the last
    ///
    /// [`RuntimeError::TimedOut`]: crate::RuntimeError::TimedOut
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.clock.timeout = Some(timeout);
        self
    }

    /// Takes the exchange as far as it can go without waiting
    fn advance(&mut self) -> Step<Result<Vec<u8>, RuntimeError>> {
        loop {
            match &mut self.stage.0 {
                Stage::Connecting => match settle(self.connect.advance()) {
                    Step::Done(Ok(conn)) => {
                        let mut send = conn.send(self.data.clone());
                        send.clock = self.clock;

                        self.stage.0 = Stage::Sending(send);
                    }

                    Step::Done(Err(error)) => return Step::Done(Err(error)),
                    Step::Park(park) => return Step::Park(park),
                },

                Stage::Sending(send) => match settle(send.advance()) {
                    Step::Done(Ok(_)) => {
                        let mut read = send.conn.recv_to_end();
                        read.clock = self.clock;

                        self.stage.0 = Stage::Reading(read);
                    }

                    Step::Done(Err(error)) => return Step::Done(Err(error)),
                    Step::Park(park) => return Step::Park(park),
                },

                Stage::Reading(read) => return settle_recv(read),
            }
        }
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

impl sealed::Sealed for ConnectTask {}
impl sealed::Sealed for ListenTask {}
impl sealed::Sealed for AcceptTask {}
impl sealed::Sealed for SendTask {}
impl sealed::Sealed for RecvTask {}
impl sealed::Sealed for RequestTask {}

impl Task for ConnectTask {
    type Output = Result<Connection, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.progress = Progress::default();
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

impl Task for ListenTask {
    type Output = Result<Listener, RuntimeError>;

    /// Never waits on the socket, so this is the whole task
    fn execute(&self, _reactor_id: i32, _task_id: usize) -> Self::Output {
        self.listen()
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

impl Task for AcceptTask {
    type Output = Result<(Connection, SocketAddr), RuntimeError>;

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

impl Task for RequestTask {
    type Output = Result<Vec<u8>, RuntimeError>;

    /// Waits on this thread, for `Runtime::block`
    fn execute(&self, reactor_id: i32, task_id: usize) -> Self::Output {
        park::drive(self.clone(), reactor_id, task_id)
    }

    fn prepare(&mut self) {
        self.clock.start();
        self.connect.clock = self.clock;
        self.connect.progress = Progress::default();
        self.stage = Progress::default();
    }

    /// Whatever the connect says
    fn blocking(&self) -> bool {
        self.connect.blocking()
    }

    fn step(&mut self, _reactor_id: i32, _task_id: usize) -> Step<Self::Output> {
        self.advance()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::futures::tcp::address::sealed::Sealed;

    /// Only a task that has to look a name up asks for a sleep
    /// thread. The rest step on a worker and park
    #[test]
    fn only_a_name_lookup_blocks() {
        assert!(!ConnectTask::new("127.0.0.1:80".target()).blocking());
        assert!(ConnectTask::new("localhost:80".target()).blocking());
        assert!(!ListenTask::new("127.0.0.1:0".target()).blocking());
        assert!(ListenTask::new("localhost:0".target()).blocking());
        assert!(!RequestTask::new("[::1]:80".target(), Arc::from(&b""[..])).blocking());
        assert!(RequestTask::new("localhost:80".target(), Arc::from(&b""[..])).blocking());
    }

    /// A clone is always a fresh run, whatever the original had
    /// got through
    #[test]
    fn a_clone_starts_from_the_beginning() {
        let progress = Progress(Reading {
            got: vec![1, 2, 3],
            started: true,
            searched: 3,
        });

        let fresh = progress.clone();

        assert!(fresh.0.got.is_empty());
        assert!(!fresh.0.started);
        assert_eq!(fresh.0.searched, 0);
    }
}
