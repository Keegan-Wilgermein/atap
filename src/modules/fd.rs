//! # Fd
//! An open descriptor that closes itself, for every family that
//! opens one: files, pipes and sockets alike

/// An open descriptor that closes itself
///
/// #### Note
/// Closing in `Drop` also keeps errno intact, since the guard
/// drops after the error value has been built
#[derive(Debug)]
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
