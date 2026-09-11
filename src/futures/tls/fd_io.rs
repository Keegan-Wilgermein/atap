//! # Fd IO
//! A non-blocking socket as the `Read` and `Write` rustls moves
//! encrypted bytes through

use std::io;

/// A borrowed socket descriptor, read and written without ever
/// waiting
///
/// A socket with nothing to give, or no room, reports
/// `WouldBlock`, which is the signal to park
pub(crate) struct FdIo(pub(crate) libc::c_int);

impl io::Read for FdIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let got = unsafe { libc::recv(self.0, buf.as_mut_ptr().cast::<libc::c_void>(), buf.len(), 0) };

        if got < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(got as usize)
    }
}

impl io::Write for FdIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let put = unsafe { libc::send(self.0, buf.as_ptr().cast::<libc::c_void>(), buf.len(), 0) };

        if put < 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(put as usize)
    }

    /// Every write goes straight to the kernel
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
