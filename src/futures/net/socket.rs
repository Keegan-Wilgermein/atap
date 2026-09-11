//! # Socket
//! Making sockets, and the settings every socket here is kept
//! in, whichever family it belongs to

use crate::{RuntimeError, modules::int_check::IntCheck};
use std::mem;

/// An open descriptor that closes itself
///
/// #### Note
/// Closing in `Drop` also keeps errno intact
pub(crate) struct Fd(libc::c_int);

impl Fd {
    /// Takes ownership of a descriptor the kernel just handed out
    #[inline(always)]
    pub(crate) fn new(fd: libc::c_int) -> Self {
        Self(fd)
    }

    /// The number, for handing to a syscall
    #[inline(always)]
    pub(crate) fn raw(&self) -> libc::c_int {
        self.0
    }
}

impl Drop for Fd {
    fn drop(&mut self) {
        unsafe { libc::close(self.0) };
    }
}

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
/// Non-blocking, so a step never waits in a syscall. Closed on
/// exec, so a process task's child doesn't inherit it. And no
/// `SIGPIPE` when the other side has gone, which would take the
/// whole program down
pub(crate) fn configure(fd: libc::c_int) -> Result<(), RuntimeError> {
    unsafe { libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC) }.check()?;

    let flags = unsafe { libc::fcntl(fd, libc::F_GETFL) }.check()?;
    unsafe { libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK) }.check()?;

    set_flag(fd, libc::SO_NOSIGPIPE)
}

/// Turns a socket level option on
pub(crate) fn set_flag(fd: libc::c_int, option: libc::c_int) -> Result<(), RuntimeError> {
    let on: libc::c_int = 1;

    unsafe {
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            option,
            (&on as *const libc::c_int).cast::<libc::c_void>(),
            mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    }
    .check()?;

    Ok(())
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
