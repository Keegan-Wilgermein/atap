//! # Path
//! A Unix socket's address, which is a path, in the form the
//! kernel takes it, and the socket file a bind leaves behind

use crate::RuntimeError;
use std::{
    ffi::{CString, OsString},
    mem,
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{self, Path, PathBuf},
};

/// Where the path starts inside a `sockaddr_un`
const PATH_OFFSET: usize = mem::offset_of!(libc::sockaddr_un, sun_path);

/// A path in the form the kernel takes a Unix socket address in,
/// and how much of the structure it fills
///
/// ## Returns
/// `BadPath` for an empty path, one with a zero byte in it, or
/// one longer than 103 bytes, which is all the kernel has room
/// for
pub(crate) fn to_raw(path: &Path) -> Result<(libc::sockaddr_un, libc::socklen_t), RuntimeError> {
    let bytes = path.as_os_str().as_bytes();
    let mut raw: libc::sockaddr_un = unsafe { mem::zeroed() };

    // One byte is kept for the zero on the end
    if bytes.is_empty() || bytes.contains(&0) || bytes.len() >= raw.sun_path.len() {
        return Err(RuntimeError::BadPath);
    }

    for (slot, byte) in raw.sun_path.iter_mut().zip(bytes) {
        *slot = *byte as libc::c_char;
    }

    let len = PATH_OFFSET + bytes.len() + 1;

    raw.sun_len = len as u8;
    raw.sun_family = libc::AF_UNIX as libc::sa_family_t;

    Ok((raw, len as libc::socklen_t))
}

/// Reads a path back out of an address the kernel filled in
///
/// ## Returns
/// `None` for a socket that never bound one, which is what most
/// senders are
pub(crate) fn from_raw(storage: &libc::sockaddr_storage, len: libc::socklen_t) -> Option<PathBuf> {
    let len = len as usize;

    if len <= PATH_OFFSET || storage.ss_family as libc::c_int != libc::AF_UNIX {
        return None;
    }

    // The storage is larger and more aligned than any address
    let raw = unsafe { &*(storage as *const libc::sockaddr_storage).cast::<libc::sockaddr_un>() };

    let room = (len - PATH_OFFSET).min(raw.sun_path.len());

    let bytes: Vec<u8> = raw.sun_path[..room]
        .iter()
        .take_while(|byte| **byte != 0)
        .map(|byte| *byte as u8)
        .collect();

    if bytes.is_empty() {
        return None;
    }

    Some(PathBuf::from(OsString::from_vec(bytes)))
}

/// A socket file a bind made, which is removed when the socket
/// that made it goes
///
/// ## Behaviour
/// Only removed if it is still the same file. One that somebody
/// else has since put at the same path is left alone
pub(crate) struct Bound {
    /// The path as it was given, for reporting back
    path: PathBuf,

    /// The same path made absolute when it was bound, so a change
    /// of working directory can't aim the removal somewhere else
    absolute: PathBuf,

    /// The file's device and inode when it was made
    identity: Option<(libc::dev_t, libc::ino_t)>,
}

impl Bound {
    /// Takes charge of the socket file a bind just made at `path`
    pub(crate) fn new(path: &Path) -> Self {
        let absolute = path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
        let identity = identity(&absolute);

        Self {
            path: path.to_path_buf(),
            absolute,
            identity,
        }
    }

    /// The path the socket was bound to
    #[inline(always)]
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for Bound {
    fn drop(&mut self) {
        let Some(made) = self.identity else {
            return;
        };

        if identity(&self.absolute) != Some(made) {
            return;
        }

        if let Some(path) = c_path(&self.absolute) {
            unsafe { libc::unlink(path.as_ptr()) };
        }
    }
}

/// A path in the form `lstat` and `unlink` take
fn c_path(path: &Path) -> Option<CString> {
    CString::new(path.as_os_str().as_bytes()).ok()
}

/// Which file is at `path` right now, as its device and inode
///
/// A symbolic link is itself, not whatever it points at
fn identity(path: &Path) -> Option<(libc::dev_t, libc::ino_t)> {
    let path = c_path(path)?;
    let mut stat: libc::stat = unsafe { mem::zeroed() };

    if unsafe { libc::lstat(path.as_ptr(), &mut stat) } != 0 {
        return None;
    }

    Some((stat.st_dev, stat.st_ino))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Copies an address into the storage the kernel fills in, the
    /// way a receive hands it back
    fn stored(raw: &libc::sockaddr_un) -> libc::sockaddr_storage {
        let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };

        unsafe {
            std::ptr::write(
                (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr_un>(),
                *raw,
            )
        };

        storage
    }

    /// A path survives the trip into the kernel's form and back
    #[test]
    fn a_path_round_trips() {
        let path = Path::new("/tmp/atap.sock");
        let (raw, len) = to_raw(path).unwrap();

        assert_eq!(len as usize, PATH_OFFSET + path.as_os_str().len() + 1);
        assert_eq!(from_raw(&stored(&raw), len), Some(path.to_path_buf()));
    }

    /// A sender that never bound a path reads as having none
    #[test]
    fn an_unbound_sender_has_no_path() {
        let storage: libc::sockaddr_storage = unsafe { mem::zeroed() };

        assert_eq!(from_raw(&storage, PATH_OFFSET as libc::socklen_t), None);
    }

    /// Paths the kernel can't take are refused before they reach it
    #[test]
    fn an_unusable_path_is_a_bad_path() {
        let long = "/".repeat(104);

        assert!(matches!(to_raw(Path::new("")), Err(RuntimeError::BadPath)));
        assert!(matches!(to_raw(Path::new("a\0b")), Err(RuntimeError::BadPath)));
        assert!(matches!(to_raw(Path::new(&long)), Err(RuntimeError::BadPath)));
        assert!(to_raw(Path::new(&long[..103])).is_ok());
    }
}
