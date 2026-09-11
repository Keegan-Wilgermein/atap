//! # Mapping
//! Anonymous memory straight from the kernel, at addresses
//! that never move

use std::{
    ptr,
    sync::atomic::{AtomicUsize, Ordering},
};

/// The page size, or 0 before it has been asked for
static PAGE_SIZE: AtomicUsize = AtomicUsize::new(0);

/// The size of a page, asking the kernel on first use
#[inline(always)]
pub(crate) fn page_size() -> usize {
    let cached = PAGE_SIZE.load(Ordering::Relaxed);

    if cached != 0 {
        return cached;
    }

    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    PAGE_SIZE.store(size, Ordering::Relaxed);

    size
}

/// Rounds a length up to a whole number of pages
#[inline(always)]
pub(crate) fn round_up(len: usize) -> usize {
    let page = page_size();

    len.div_ceil(page) * page
}

/// Maps `len` bytes of zeroed, readable, writable memory
///
/// ## Returns
/// The base address, or null if the kernel refuses
pub(crate) fn alloc(len: usize) -> *mut u8 {
    let len = round_up(len);

    let base = unsafe {
        libc::mmap(
            ptr::null_mut(), // Let the kernel pick the address
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_ANON | libc::MAP_PRIVATE, // Not backed by a file, not shared
            -1,                                 // No descriptor to back it with
            0,
        )
    };

    if base == libc::MAP_FAILED {
        return ptr::null_mut();
    }

    base.cast::<u8>()
}

/// Gives a mapping back to the kernel
///
/// `len` must be the same length that was passed to `alloc`
pub(crate) fn free(base: *mut u8, len: usize) {
    if base.is_null() {
        return;
    }

    unsafe { libc::munmap(base.cast::<libc::c_void>(), round_up(len)) };
}

/// Gives the pages behind a mapping back without unmapping it
///
/// ## Returns
/// Whether the kernel took them
///
/// ## Behaviour
/// The range stays mapped, and reads as zeros once the kernel
/// has reclaimed it
///
/// ## Safety
/// Nothing in the range may be in use. The kernel can zero it
/// at any point after this, though a write before it does
/// cancels the reclaim for that page
pub(crate) unsafe fn release(base: *mut u8, len: usize) -> bool {
    if base.is_null() || len == 0 {
        return false;
    }

    unsafe { libc::madvise(base.cast::<libc::c_void>(), len, libc::MADV_FREE) == 0 }
}
