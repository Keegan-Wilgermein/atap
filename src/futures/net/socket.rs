//! # Socket
//! Making sockets, and the settings every socket here is kept
//! in, whichever family it belongs to

use crate::{
    RuntimeError,
    modules::{fd::Fd, int_check::IntCheck},
};
use std::{mem, time::Duration};

/// Makes a socket of `kind` in `domain`, set up the way every
/// socket here is kept
pub(crate) fn open(domain: libc::c_int, kind: libc::c_int) -> Result<Fd, RuntimeError> {
    let fd = Fd::new(unsafe { libc::socket(domain, kind, 0) }.check()?);

    configure(fd.raw())?;

    Ok(fd)
}

/// Puts a socket in the state every one here is kept in
///
/// ## Behaviour
/// Non-blocking, closed on exec, and with no `SIGPIPE`
pub(crate) fn configure(fd: libc::c_int) -> Result<(), RuntimeError> {
    unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) }.check()?;

    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) }.check()?;
    unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) }.check()?;

    set_flag(fd, libc::SO_NOSIGPIPE)
}

/// Turns a socket level option on
pub(crate) fn set_flag(fd: libc::c_int, option: libc::c_int) -> Result<(), RuntimeError> {
    set_option(fd, libc::SOL_SOCKET, option, 1)
}

/// Sets an option that takes an `int`
pub(crate) fn set_option(
    fd: libc::c_int,
    level: libc::c_int,
    option: libc::c_int,
    value: libc::c_int,
) -> Result<(), RuntimeError> {
    set_raw(fd, level, option, &value)
}

/// Sets an option that takes any plain value
pub(crate) fn set_raw<T>(
    fd: libc::c_int,
    level: libc::c_int,
    option: libc::c_int,
    value: &T,
) -> Result<(), RuntimeError> {
    unsafe {
        libc::setsockopt(
            fd,
            level,
            option,
            (value as *const T).cast::<libc::c_void>(),
            mem::size_of::<T>() as libc::socklen_t,
        )
    }
    .check()?;

    Ok(())
}

/// Reads an option that holds an `int`
pub(crate) fn get_option(
    fd: libc::c_int,
    level: libc::c_int,
    option: libc::c_int,
) -> Result<libc::c_int, RuntimeError> {
    let mut value: libc::c_int = 0;
    let mut len = mem::size_of::<libc::c_int>() as libc::socklen_t;

    unsafe {
        libc::getsockopt(
            fd,
            level,
            option,
            (&mut value as *mut libc::c_int).cast::<libc::c_void>(),
            &mut len,
        )
    }
    .check()?;

    Ok(value)
}

/// Turns keepalive probes off, or on after `idle` without traffic
pub(crate) fn set_keepalive(fd: libc::c_int, idle: Option<Duration>) -> Result<(), RuntimeError> {
    let Some(idle) = idle else {
        return set_option(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 0);
    };

    set_option(fd, libc::SOL_SOCKET, libc::SO_KEEPALIVE, 1)?;

    let seconds = idle.as_secs().clamp(1, libc::c_int::MAX as u64) as libc::c_int;

    set_option(fd, libc::IPPROTO_TCP, libc::TCP_KEEPALIVE, seconds)
}

/// What a socket is set up with beyond what every one here gets
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Options {
    /// `TCP_NODELAY`
    pub(crate) nodelay: bool,

    /// Keepalive probes after this long idle
    pub(crate) keepalive: Option<Duration>,

    /// `SO_REUSEPORT`
    pub(crate) reuse_port: bool,

    /// `IPV6_V6ONLY`, for an IPv6 socket
    pub(crate) v6_only: bool,

    /// `SO_BROADCAST`
    pub(crate) broadcast: bool,

    /// How many connections may wait to be accepted
    pub(crate) backlog: Option<u32>,
}

impl Options {
    /// Puts the settings on a fresh socket, before it binds or
    /// connects
    pub(crate) fn apply(&self, fd: libc::c_int, v6: bool) -> Result<(), RuntimeError> {
        if self.nodelay {
            set_option(fd, libc::IPPROTO_TCP, libc::TCP_NODELAY, 1)?;
        }

        if self.keepalive.is_some() {
            set_keepalive(fd, self.keepalive)?;
        }

        if self.reuse_port {
            set_flag(fd, libc::SO_REUSEPORT)?;
        }

        if self.v6_only && v6 {
            set_option(fd, libc::IPPROTO_IPV6, libc::IPV6_V6ONLY, 1)?;
        }

        if self.broadcast {
            set_flag(fd, libc::SO_BROADCAST)?;
        }

        Ok(())
    }

    /// The backlog `listen` is given
    pub(crate) fn backlog(&self) -> libc::c_int {
        self.backlog.map_or(libc::SOMAXCONN, |backlog| {
            backlog.min(libc::c_int::MAX as u32) as libc::c_int
        })
    }
}

/// Starts a connect to a raw address
///
/// ## Returns
/// Whether it connected at once. `false` means it is under way,
/// to be finished when the socket is writable
pub(crate) fn begin_connect(
    fd: libc::c_int,
    addr: *const libc::sockaddr,
    len: libc::socklen_t,
) -> Result<bool, RuntimeError> {
    match unsafe { libc::connect(fd, addr, len) }.check() {
        Ok(_) => Ok(true),

        // Non-blocking, so it carries on without this thread. An
        // interrupted one carries on too
        Err(RuntimeError::CheckError(Some(libc::EINPROGRESS | libc::EINTR))) => Ok(false),

        Err(error) => Err(error),
    }
}

/// Whether a connect started earlier has got anywhere
///
/// ## Returns
/// `true` once connected, `false` while it is still under way,
/// and why it failed if it did
pub(crate) fn finished_connecting(fd: libc::c_int) -> Result<bool, RuntimeError> {
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
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    let peer = unsafe {
        libc::getpeername(
            fd,
            (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
            &mut len,
        )
    }
    .check();

    match peer {
        Ok(_) => Ok(true),
        Err(RuntimeError::CheckError(Some(libc::ENOTCONN))) => Ok(false),
        Err(error) => Err(error),
    }
}
