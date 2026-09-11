//! # Datagram
//! Sending and receiving whole datagrams, for UDP and Unix
//! datagram sockets alike

use crate::{RuntimeError, modules::int_check::IntCheck};
use std::mem;

/// Room a receive makes for one datagram
///
/// Past the largest a UDP datagram can be, so one always arrives
/// whole
const MAX_DATAGRAM: usize = 64 * 1024;

/// Sends one datagram to a raw address
///
/// ## Returns
/// The bytes sent, which is all of them, or `None` when the
/// socket has no room right now
///
/// #### Note
/// A datagram the kernel won't take at all, too large or with
/// nowhere to queue it, is an error rather than a wait. A
/// datagram is allowed to be lost, so sending again is up to
/// the caller
pub(crate) fn send_datagram(
    fd: libc::c_int,
    data: &[u8],
    to: *const libc::sockaddr,
    len: libc::socklen_t,
) -> Result<Option<usize>, RuntimeError> {
    loop {
        let sent = unsafe {
            libc::sendto(
                fd,
                data.as_ptr().cast::<libc::c_void>(),
                data.len(),
                0,
                to,
                len,
            )
        }
        .check();

        match sent {
            Ok(sent) => return Ok(Some(sent as usize)),
            Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}
            Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => return Ok(None),
            Err(error) => return Err(error),
        }
    }
}

/// Takes one datagram off a socket, whole
///
/// ## Returns
/// The bytes, and the raw address they came from with its
/// length. `None` when nothing is waiting
pub(crate) fn recv_datagram(
    fd: libc::c_int,
) -> Result<Option<(Vec<u8>, libc::sockaddr_storage, libc::socklen_t)>, RuntimeError> {
    let mut data: Vec<u8> = Vec::with_capacity(MAX_DATAGRAM);

    loop {
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
        let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

        let got = unsafe {
            libc::recvfrom(
                fd,
                data.spare_capacity_mut().as_mut_ptr().cast::<libc::c_void>(),
                MAX_DATAGRAM,
                0,
                (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
                &mut len,
            )
        }
        .check();

        match got {
            Ok(read) => {
                // The kernel just wrote `read` bytes into the capacity
                unsafe { data.set_len(read as usize) };

                // Most datagrams are small, so the room goes back
                data.shrink_to_fit();

                return Ok(Some((data, storage, len)));
            }

            Err(RuntimeError::CheckError(Some(libc::EINTR))) => {}
            Err(RuntimeError::CheckError(Some(libc::EAGAIN))) => return Ok(None),
            Err(error) => return Err(error),
        }
    }
}
