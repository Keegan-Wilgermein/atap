//! # TCP task
//! The tasks the `Tcp` constructors and a `Listener` return,
//! and everything they do once run
//!
//! Every task here that waits on a socket does so in steps.
//! A step does what it can without waiting, then parks, and
//! the runtime steps it again once the socket is ready. A
//! spawned one holds no thread while it waits
//!
//! Sending and receiving on a connection are in `net::stream`,
//! since a Unix connection shares them

use crate::{
    RuntimeError,
    constants::INLINE_PAYLOAD,
    futures::{
        net::{
            address::{Target, family, from_raw, local_of, peer_of, to_raw},
            socket::{Fd, begin_connect, configure, finished_connecting, open, set_flag},
            step::{Clock, Progress, settle},
            stream::{RecvTask, SendTask},
        },
        task::{
            Task,
            sealed::{self, Step},
        },
        tcp::connection::{Connection, Listener},
    },
    modules::{int_check::IntCheck, park},
};
use std::{mem, net::SocketAddr, sync::Arc, time::Duration};

// Anything larger costs a page mapping per task
const _: () = assert!(mem::size_of::<Result<Connection, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(mem::size_of::<Result<Listener, RuntimeError>>() <= INLINE_PAYLOAD);
const _: () = assert!(
    mem::size_of::<Result<(Connection, SocketAddr), RuntimeError>>() <= INLINE_PAYLOAD
);

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
        self.clock.limit(timeout);
        self
    }

    /// Runs to a clock already started, for a task made of other
    /// tasks
    ///
    /// Only TLS composes a TCP task today
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) fn timed(mut self, clock: Clock) -> Self {
        self.clock = clock;
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
    let fd = open(family(addr), libc::SOCK_STREAM)?;
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
        self.clock.limit(timeout);
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
    let fd = open(family(addr), libc::SOCK_STREAM)?;

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
        self.clock.limit(timeout);
        self
    }

    /// Runs to a clock already started, for a task made of other
    /// tasks
    ///
    /// Only TLS composes a TCP task today
    #[cfg_attr(not(feature = "tls"), allow(dead_code))]
    pub(crate) fn timed(mut self, clock: Clock) -> Self {
        self.clock = clock;
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
        self.clock.limit(timeout);
        self
    }

    /// Takes the exchange as far as it can go without waiting
    fn advance(&mut self, reactor_id: i32, task_id: usize) -> Step<Result<Vec<u8>, RuntimeError>> {
        loop {
            match &mut self.stage.0 {
                Stage::Connecting => match settle(self.connect.advance()) {
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

impl sealed::Sealed for ConnectTask {}
impl sealed::Sealed for ListenTask {}
impl sealed::Sealed for AcceptTask {}
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

    fn step(&mut self, reactor_id: i32, task_id: usize) -> Step<Self::Output> {
        self.advance(reactor_id, task_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::futures::net::address::sealed::Sealed;

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
}
