//! # C Path
//! A path in the form the kernel takes one

use std::{ffi::CString, os::unix::ffi::OsStrExt, path::Path};

/// Turns a path into the form the kernel takes
///
/// ## Returns
/// `None` when the path has a zero byte in it, since passing
/// the part before it would act on a different file
pub(crate) fn c_path(path: impl AsRef<Path>) -> Option<CString> {
    CString::new(path.as_ref().as_os_str().as_bytes()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A path with a zero byte in it doesn't convert
    #[test]
    fn a_path_with_a_zero_byte_does_not_convert() {
        assert!(c_path("a\0b").is_none(), "a zero byte must not convert");
        assert!(c_path("ab").is_some(), "an ordinary path must convert");
    }
}
