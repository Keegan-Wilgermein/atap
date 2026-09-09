//! # Mapping
//! Anonymous memory taken straight from the kernel
//!
//! Task slots and table blocks both need an address that
//! never moves for as long as they live. A `Vec` can't
//! promise that, it reallocates the moment it grows, and
//! the whole design rests on a listener being able to hold
//! an address and still find its data there later
//!
//! The mappings are `MAP_PRIVATE` because every thread in
//! a process already shares one address space, so a private
//! anonymous mapping is the same address for all of them.
//! `MAP_SHARED` would only start to matter across a `fork`,
//! and it would have to pair with
//! `OS_SYNC_WAIT_ON_ADDRESS_SHARED` on every wait

use std::{
    ptr,
    sync::atomic::{AtomicUsize, Ordering},
};

/// The page size, or 0 before it has been asked for
///
/// Cached because every allocation rounds against it and
/// `sysconf` is a call this crate can just not make twice
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

/// Rounds a length up to the next whole page
///
/// `mmap` hands out whole pages whatever is asked for, so
/// the rounded length is the real length and the one that
/// has to come back to `free`
#[inline(always)]
pub(crate) fn round_up(len: usize) -> usize {
    let page = page_size();

    len.div_ceil(page) * page
}

/// Maps `len` bytes of zeroed, readable, writable memory
///
/// ## Returns
/// The base address, or null if the kernel refuses
///
/// #### Note
/// The pages come back zeroed, which every type stored in
/// them relies on being a valid empty state
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
