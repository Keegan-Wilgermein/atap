//! UDP task tests
//!
//! Everything runs over loopback, on sockets bound to port 0

mod common;

use atap::{
    Runtime, RuntimeError,
    udp::{Udp, UdpSocket},
};
use common::{until_started, within};
use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A socket on a free loopback port
fn socket() -> UdpSocket {
    let _ = Runtime::init();

    Runtime::block(Udp::bind("127.0.0.1:0")).expect("a loopback socket must bind")
}

/// A datagram arrives whole, with the address it came from
#[test]
fn a_datagram_arrives_with_its_sender() {
    let a = socket();
    let b = socket();

    let sent = Runtime::block(a.send_to(b.local_addr(), b"hello".as_slice())).unwrap();
    assert_eq!(sent, 5);

    let (data, from) = within(b.recv_from(), PATIENCE).unwrap();

    assert_eq!(data, b"hello");
    assert_eq!(from, a.local_addr());
}

/// Two sends are two receives, never one merged one
#[test]
fn datagrams_keep_their_boundaries() {
    let a = socket();
    let b = socket();

    Runtime::block(a.send_to(b.local_addr(), b"one".as_slice())).unwrap();
    Runtime::block(a.send_to(b.local_addr(), b"two".as_slice())).unwrap();

    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, b"one");
    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, b"two");
}

/// An empty datagram is a real one, and arrives as one
#[test]
fn an_empty_datagram_is_a_real_one() {
    let a = socket();
    let b = socket();

    assert_eq!(
        Runtime::block(a.send_to(b.local_addr(), b"".as_slice())).unwrap(),
        0
    );

    let (data, from) = within(b.recv_from(), PATIENCE).unwrap();

    assert!(data.is_empty());
    assert_eq!(from, a.local_addr());
}

/// A datagram of several kilobytes still arrives in one piece
///
/// Kept under the 9216 bytes macOS allows by default
#[test]
fn a_large_datagram_arrives_whole() {
    let a = socket();
    let b = socket();

    let data: Vec<u8> = (0..8000).map(|at| (at % 251) as u8).collect();

    Runtime::block(a.send_to(b.local_addr(), data.clone())).unwrap();

    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, data);
}

/// A spawned receive parks until a datagram comes, then finishes
#[test]
fn a_spawned_receive_waits_for_a_datagram() {
    let a = socket();
    let b = socket();

    let reading = Runtime::task(b.recv_from()).spawn();
    until_started(&reading, PATIENCE);

    assert!(
        reading.is_running(),
        "a receive waiting on the network reads as running, got {:?}",
        reading.state(),
    );

    Runtime::block(a.send_to(b.local_addr(), b"late".as_slice())).unwrap();

    let (data, from) = reading
        .take_with_timeout(PATIENCE)
        .expect("the receive must settle once a datagram is there")
        .expect("the receive must succeed");

    assert_eq!(data, b"late");
    assert_eq!(from, a.local_addr());
}

/// A blocking receive with nothing coming gives up at its
/// timeout
#[test]
fn a_blocking_receive_times_out() {
    let b = socket();

    let started = Instant::now();
    let got = within(b.recv_from(), Duration::from_millis(100));
    let took = started.elapsed();

    assert_eq!(got, Err(RuntimeError::TimedOut));
    assert!(
        took >= Duration::from_millis(100),
        "gave up early, after {took:?}"
    );
    assert!(
        took < Duration::from_secs(2),
        "gave up late, after {took:?}"
    );
}

/// So does a spawned one
#[test]
fn a_spawned_receive_times_out() {
    let b = socket();

    let handle = Runtime::task(b.recv_from())
        .timeout(Duration::from_millis(100))
        .spawn();

    assert_eq!(
        handle.take_with_timeout(PATIENCE),
        Err(RuntimeError::TimedOut),
    );
}

/// Cancelling a parked receive settles it at once
#[test]
fn a_parked_receive_can_be_cancelled() {
    let b = socket();

    let reading = Runtime::task(b.recv_from()).spawn();
    until_started(&reading, PATIENCE);

    reading.clone().cancel();

    assert_eq!(
        reading.join_with_timeout(PATIENCE),
        Err(RuntimeError::Cancelled),
    );
}

/// A name is looked up when the task runs, and the address used
/// is the one in the socket's own family
#[test]
fn a_name_is_looked_up_in_the_sockets_family() {
    let a = socket();
    let b = socket();
    let port = b.local_addr().port();

    // `localhost` can give `::1` first, which an IPv4 socket can't
    // send to
    Runtime::block(a.send_to(format!("localhost:{port}"), b"named".as_slice()))
        .expect("localhost has an IPv4 address to send to");

    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, b"named");
}

/// Something that isn't an address fails as one
#[test]
fn a_nonsense_address_is_a_bad_address() {
    let a = socket();

    assert_eq!(
        Runtime::block(a.send_to("not an address", b"x".as_slice())),
        Err(RuntimeError::BadAddress),
    );
}

/// An address in the other family can't be sent to, and says so
/// rather than leaving it to the kernel
#[test]
fn an_address_in_the_other_family_is_a_bad_address() {
    let a = socket();

    assert_eq!(
        Runtime::block(a.send_to("[::1]:9", b"x".as_slice())),
        Err(RuntimeError::BadAddress),
    );
}

/// A receive spawned before a close still completes, since it
/// holds the socket too
#[test]
fn a_receive_in_flight_outlives_a_close() {
    let a = socket();
    let b = socket();
    let to = b.local_addr();

    let reading = Runtime::task(b.recv_from()).spawn();
    until_started(&reading, PATIENCE);

    b.close();
    Runtime::block(a.send_to(to, b"still".as_slice())).unwrap();

    assert_eq!(
        reading.take_with_timeout(PATIENCE).unwrap().unwrap().0,
        b"still"
    );
}

/// Broadcast, hop limit and multicast settings take
#[test]
fn socket_settings_take() {
    let _ = Runtime::init();

    let socket = Runtime::block(Udp::bind("0.0.0.0:0").broadcast(true)).unwrap();

    socket.set_broadcast(false).unwrap();

    socket.set_ttl(7).unwrap();
    assert_eq!(socket.ttl(), Ok(7));

    let group = Ipv4Addr::new(239, 255, 42, 99);

    socket
        .join_multicast_v4(group, Ipv4Addr::UNSPECIFIED)
        .unwrap();
    socket
        .leave_multicast_v4(group, Ipv4Addr::UNSPECIFIED)
        .unwrap();

    assert_eq!(socket.set_ttl(u32::MAX), Err(RuntimeError::BadArgument));

    let v6 = Runtime::block(Udp::bind("[::1]:0").v6_only(true)).unwrap();

    v6.set_ttl(3).unwrap();
    assert_eq!(v6.ttl(), Ok(3));
}

/// Two sockets share a port when both ask to
#[test]
fn reused_udp_ports_are_shared() {
    let _ = Runtime::init();

    let first = Runtime::block(Udp::bind("127.0.0.1:0").reuse_port(true)).unwrap();
    let second = Runtime::block(Udp::bind(first.local_addr()).reuse_port(true));

    assert!(second.is_ok(), "the port was not shared: {second:?}");
}

/// A connected socket sends and receives with no address, and only
/// hears its peer
#[test]
fn a_connected_socket_needs_no_address() {
    let a = socket();
    let b = socket();
    let stranger = socket();

    assert_eq!(
        a.peer_addr(),
        Err(RuntimeError::CheckError(Some(libc::ENOTCONN)))
    );
    assert_eq!(
        Runtime::block(a.send(b"nowhere".as_slice())),
        Err(RuntimeError::CheckError(Some(libc::EDESTADDRREQ)))
    );

    Runtime::block(a.connect(b.local_addr())).unwrap();
    Runtime::block(b.connect(a.local_addr())).unwrap();

    assert_eq!(a.peer_addr(), Ok(b.local_addr()));

    // Sent first, so a filter that let it through would give it first
    Runtime::block(stranger.send_to(b.local_addr(), b"not for you".as_slice())).unwrap();
    Runtime::block(a.send(b"for b".as_slice())).unwrap();

    assert_eq!(within(b.recv(), PATIENCE), Ok(b"for b".to_vec()));

    Runtime::block(b.send(b"back".as_slice())).unwrap();

    assert_eq!(within(a.recv(), PATIENCE), Ok(b"back".to_vec()));
}

/// A peek leaves the datagram for the next receive
#[test]
fn a_peek_leaves_the_datagram() {
    let a = socket();
    let b = socket();

    Runtime::block(a.send_to(b.local_addr(), b"twice".as_slice())).unwrap();

    let (peeked, from) = within(b.recv_from().peek(), PATIENCE).unwrap();

    assert_eq!(peeked, b"twice");
    assert_eq!(from, a.local_addr());

    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, b"twice");

    Runtime::block(b.connect(a.local_addr())).unwrap();
    Runtime::block(a.send_to(b.local_addr(), b"again".as_slice())).unwrap();

    assert_eq!(within(b.recv().peek(), PATIENCE), Ok(b"again".to_vec()));
    assert_eq!(within(b.recv(), PATIENCE), Ok(b"again".to_vec()));
}
