//! # Handshake
//! Moving a TLS session's encrypted bytes between rustls and
//! the socket, without ever waiting

use crate::{
    RuntimeError,
    futures::tls::{config::tls_error, fd_io::FdIo},
};
use std::io;

/// Takes a handshake as far as it can go without waiting
///
/// ## Returns
/// `None` once the handshake is done and everything it had to
/// send has gone. Otherwise the filter to park on, `EVFILT_READ`
/// or `EVFILT_WRITE`
pub(crate) fn handshake(tls: &mut rustls::Connection, fd: libc::c_int) -> Result<Option<i16>, RuntimeError> {
    loop {
        if let Some(filter) = send_pending(tls, fd)? {
            return Ok(Some(filter));
        }

        if !tls.is_handshaking() {
            return Ok(None);
        }

        match tls.read_tls(&mut FdIo(fd)) {
            // Gone before the handshake finished
            Ok(0) => return Err(RuntimeError::Closed),

            Ok(_) => process(tls, fd)?,

            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(Some(libc::EVFILT_READ));
            }

            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(io_error(error)),
        }
    }
}

/// Hands whatever rustls has queued to the socket
///
/// ## Returns
/// `None` once it has all gone, or `EVFILT_WRITE` when the socket
/// is full
pub(crate) fn send_pending(tls: &mut rustls::Connection, fd: libc::c_int) -> Result<Option<i16>, RuntimeError> {
    while tls.wants_write() {
        match tls.write_tls(&mut FdIo(fd)) {
            Ok(_) => {}

            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(Some(libc::EVFILT_WRITE));
            }

            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(io_error(error)),
        }
    }

    Ok(None)
}

/// Lets rustls work through what `read_tls` gave it
///
/// A failure queues an alert saying why, which gets one chance to
/// go before the error comes back
pub(crate) fn process(tls: &mut rustls::Connection, fd: libc::c_int) -> Result<(), RuntimeError> {
    if let Err(error) = tls.process_new_packets() {
        let _ = send_pending(tls, fd);

        return Err(tls_error(error));
    }

    Ok(())
}

/// Turns a socket failure into the runtime's
#[inline(always)]
pub(crate) fn io_error(error: io::Error) -> RuntimeError {
    RuntimeError::CheckError(error.raw_os_error())
}
