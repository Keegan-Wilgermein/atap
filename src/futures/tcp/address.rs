//! # Address
//! Where a socket task points, and the form the kernel takes
//! an address in

use crate::{RuntimeError, modules::int_check::IntCheck};
use std::{
    mem,
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6, ToSocketAddrs},
    ptr,
    sync::Arc,
};

/// Where a socket task connects or listens, as it was given
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// An address already, so nothing needs looking up
    Addr(SocketAddr),

    /// A `host:port` still to be looked up
    Name(Arc<str>),
}

impl Target {
    /// Reads a `host:port`, keeping it as an address when it
    /// already is one
    ///
    /// Parsing only. Nothing is looked up until the task runs
    fn parse(text: &str) -> Self {
        match text.parse::<SocketAddr>() {
            Ok(addr) => Self::Addr(addr),
            Err(_) => Self::Name(Arc::from(text)),
        }
    }

    /// Whether finding the addresses means a name lookup, which
    /// blocks
    #[inline(always)]
    pub(crate) fn needs_lookup(&self) -> bool {
        matches!(self, Self::Name(_))
    }

    /// Every address this names, in the order to try them
    ///
    /// ## Returns
    /// `BadAddress` when it doesn't parse or the lookup finds
    /// nothing
    pub(crate) fn resolve(&self) -> Result<Vec<SocketAddr>, RuntimeError> {
        let name = match self {
            Self::Addr(addr) => return Ok(vec![*addr]),
            Self::Name(name) => name,
        };

        let found: Vec<SocketAddr> = name
            .to_socket_addrs()
            .map_err(|_| RuntimeError::BadAddress)?
            .collect();

        if found.is_empty() {
            return Err(RuntimeError::BadAddress);
        }

        Ok(found)
    }
}

/// Stops `TcpAddress` being implemented outside the crate
///
/// `Target` is crate private, which is the point: nothing outside can
/// name `target`, so nothing outside can implement it
#[allow(private_interfaces)]
pub(crate) mod sealed {
    use super::Target;

    /// Implemented for everything a socket task can be pointed at
    pub trait Sealed {
        /// What the task is pointed at
        fn target(self) -> Target;
    }
}

/// Anything a socket task can be pointed at
///
/// `"host:port"` as a `&str` or a `String`, or a `SocketAddr`.
/// A name is looked up when the task runs, never when it is
/// built
#[allow(private_bounds)]
pub trait TcpAddress: sealed::Sealed {}

#[allow(private_interfaces)]
impl sealed::Sealed for &str {
    fn target(self) -> Target {
        Target::parse(self)
    }
}

#[allow(private_interfaces)]
impl sealed::Sealed for String {
    fn target(self) -> Target {
        Target::parse(&self)
    }
}

#[allow(private_interfaces)]
impl sealed::Sealed for &String {
    fn target(self) -> Target {
        Target::parse(self)
    }
}

#[allow(private_interfaces)]
impl sealed::Sealed for SocketAddr {
    fn target(self) -> Target {
        Target::Addr(self)
    }
}

impl TcpAddress for &str {}
impl TcpAddress for String {}
impl TcpAddress for &String {}
impl TcpAddress for SocketAddr {}

/// The address family a socket for `addr` is made in
#[inline(always)]
pub(crate) fn family(addr: &SocketAddr) -> libc::c_int {
    match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    }
}

/// An address in the form the kernel takes, and how much of
/// the storage it fills
pub(crate) fn to_raw(addr: &SocketAddr) -> (libc::sockaddr_storage, libc::socklen_t) {
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let at = (&mut storage as *mut libc::sockaddr_storage).cast::<u8>();

    let len = match addr {
        SocketAddr::V4(v4) => {
            let raw = libc::sockaddr_in {
                sin_len: mem::size_of::<libc::sockaddr_in>() as u8,
                sin_family: libc::AF_INET as libc::sa_family_t,
                sin_port: v4.port().to_be(),
                sin_addr: libc::in_addr {
                    s_addr: u32::from_ne_bytes(v4.ip().octets()),
                },
                sin_zero: [0; 8],
            };

            // The storage is larger and more aligned than any address
            unsafe { ptr::write(at.cast::<libc::sockaddr_in>(), raw) };

            mem::size_of::<libc::sockaddr_in>()
        }

        SocketAddr::V6(v6) => {
            let raw = libc::sockaddr_in6 {
                sin6_len: mem::size_of::<libc::sockaddr_in6>() as u8,
                sin6_family: libc::AF_INET6 as libc::sa_family_t,
                sin6_port: v6.port().to_be(),
                sin6_flowinfo: v6.flowinfo(),
                sin6_addr: libc::in6_addr {
                    s6_addr: v6.ip().octets(),
                },
                sin6_scope_id: v6.scope_id(),
            };

            unsafe { ptr::write(at.cast::<libc::sockaddr_in6>(), raw) };

            mem::size_of::<libc::sockaddr_in6>()
        }
    };

    (storage, len as libc::socklen_t)
}

/// Reads an address back out of what the kernel filled in
///
/// ## Returns
/// `None` for a family that isn't an internet one
pub(crate) fn from_raw(storage: &libc::sockaddr_storage) -> Option<SocketAddr> {
    let at = (storage as *const libc::sockaddr_storage).cast::<u8>();

    match storage.ss_family as libc::c_int {
        libc::AF_INET => {
            let raw = unsafe { &*at.cast::<libc::sockaddr_in>() };

            Some(SocketAddr::V4(SocketAddrV4::new(
                Ipv4Addr::from(raw.sin_addr.s_addr.to_ne_bytes()),
                u16::from_be(raw.sin_port),
            )))
        }

        libc::AF_INET6 => {
            let raw = unsafe { &*at.cast::<libc::sockaddr_in6>() };

            Some(SocketAddr::V6(SocketAddrV6::new(
                Ipv6Addr::from(raw.sin6_addr.s6_addr),
                u16::from_be(raw.sin6_port),
                raw.sin6_flowinfo,
                raw.sin6_scope_id,
            )))
        }

        _ => None,
    }
}

/// The address this end of a socket is bound to
pub(crate) fn local_of(fd: libc::c_int) -> Result<SocketAddr, RuntimeError> {
    name_of(fd, libc::getsockname)
}

/// The address at the other end of a connected socket
///
/// `ENOTCONN` while a connect is still under way
pub(crate) fn peer_of(fd: libc::c_int) -> Result<SocketAddr, RuntimeError> {
    name_of(fd, libc::getpeername)
}

/// Asks the kernel for one of a socket's two addresses
fn name_of(
    fd: libc::c_int,
    ask: unsafe extern "C" fn(libc::c_int, *mut libc::sockaddr, *mut libc::socklen_t) -> libc::c_int,
) -> Result<SocketAddr, RuntimeError> {
    let mut storage: libc::sockaddr_storage = unsafe { mem::zeroed() };
    let mut len = mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;

    unsafe {
        ask(
            fd,
            (&mut storage as *mut libc::sockaddr_storage).cast::<libc::sockaddr>(),
            &mut len,
        )
    }
    .check()?;

    from_raw(&storage).ok_or(RuntimeError::BadAddress)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An IPv4 address survives the trip into the kernel's form
    /// and back
    #[test]
    fn a_v4_address_round_trips() {
        let addr: SocketAddr = "192.168.1.20:8080".parse().unwrap();
        let (raw, len) = to_raw(&addr);

        assert_eq!(len as usize, mem::size_of::<libc::sockaddr_in>());
        assert_eq!(from_raw(&raw), Some(addr));
    }

    /// So does an IPv6 one, scope and all
    #[test]
    fn a_v6_address_round_trips() {
        let addr = SocketAddr::V6(SocketAddrV6::new(
            "fe80::1:2:3:4".parse().unwrap(),
            443,
            7,
            3,
        ));
        let (raw, len) = to_raw(&addr);

        assert_eq!(len as usize, mem::size_of::<libc::sockaddr_in6>());
        assert_eq!(from_raw(&raw), Some(addr));
    }

    /// A literal is an address straight away, and only a name is
    /// left to look up
    #[test]
    fn only_a_name_needs_looking_up() {
        assert!(!Target::parse("127.0.0.1:80").needs_lookup());
        assert!(!Target::parse("[::1]:80").needs_lookup());
        assert!(Target::parse("localhost:80").needs_lookup());
    }

    /// Something that isn't an address at all fails when it is
    /// looked up, as `BadAddress`
    #[test]
    fn a_nonsense_address_is_a_bad_address() {
        assert_eq!(
            Target::parse("no port here").resolve(),
            Err(RuntimeError::BadAddress),
        );
    }
}
