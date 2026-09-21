//! Unix socket task tests
//!
//! Every socket file lives in `/tmp`, named for the process and
//! the test, so tests running at once never share one

mod common;

use atap::{
    Runtime, RuntimeError,
    unix::{Unix, UnixConnection, UnixListener},
};
use common::{until_started, within};
use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::Duration,
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A socket path of this test's own, with nothing left at it
/// from an earlier run
fn sock(name: &str) -> PathBuf {
    let _ = Runtime::init();

    let path = PathBuf::from(format!("/tmp/atap-{}-{name}.sock", process::id()));
    let _ = fs::remove_file(&path);

    path
}

/// Whether anything is at `path`, without following a link
fn present(path: &PathBuf) -> bool {
    fs::symlink_metadata(path).is_ok()
}

/// A listener, and both ends of one connection to it, the
/// connecting end first
fn pair(name: &str) -> (UnixListener, UnixConnection, UnixConnection) {
    let path = sock(name);
    let listener = Runtime::block(Unix::listen(&path)).expect("a Unix listener must open");

    let accepting = Runtime::task(listener.accept()).spawn();
    let client = Runtime::block(Unix::connect(&path)).expect("a Unix socket must connect");

    let server = accepting
        .take_with_timeout(PATIENCE)
        .expect("the accept must settle")
        .expect("the accept must succeed");

    (listener, client, server)
}

/// Bytes go both ways over a Unix connection, with the same send
/// and receive tasks as TCP
#[test]
fn a_unix_connection_carries_bytes_both_ways() {
    let (listener, client, server) = pair("both");

    assert_eq!(client.path(), listener.path());
    assert_eq!(server.path(), listener.path());

    Runtime::block(client.send(b"ping".as_slice())).unwrap();
    assert_eq!(Runtime::block(server.recv_exact(4)).unwrap(), b"ping");

    Runtime::block(server.send(b"pong".as_slice())).unwrap();
    assert_eq!(Runtime::block(client.recv_exact(4)).unwrap(), b"pong");
}

/// A delimited receive leaves the rest for the next one, and
/// reading to the end stops at the close, as over TCP
#[test]
fn a_unix_connection_reads_like_a_tcp_one() {
    let (_listener, client, server) = pair("reads");

    Runtime::block(client.send(b"one\ntwo\nrest".as_slice())).unwrap();
    client.close();

    assert_eq!(
        Runtime::block(server.recv_until(b"\n", 64)).unwrap(),
        b"one\n"
    );
    assert_eq!(
        Runtime::block(server.recv_until(b"\n", 64)).unwrap(),
        b"two\n"
    );
    assert_eq!(Runtime::block(server.recv_to_end()).unwrap(), b"rest");
}

/// A spawned accept parks until somebody connects
#[test]
fn a_spawned_accept_waits_for_a_connection() {
    let path = sock("accept");
    let listener = Runtime::block(Unix::listen(&path)).unwrap();

    let accepting = Runtime::task(listener.accept()).spawn();
    until_started(&accepting, PATIENCE);

    assert!(
        accepting.is_running(),
        "an accept waiting on the socket reads as running, got {:?}",
        accepting.state(),
    );

    let _client = Runtime::block(Unix::connect(&path)).unwrap();

    assert!(accepting.take_with_timeout(PATIENCE).unwrap().is_ok());
}

/// A spawned receive on a silent connection gives up at its
/// timeout
#[test]
fn a_unix_receive_times_out() {
    let (_listener, _client, server) = pair("timeout");

    let handle = Runtime::task(server.recv(64))
        .timeout(Duration::from_millis(100))
        .spawn();

    assert_eq!(
        handle.take_with_timeout(PATIENCE),
        Err(RuntimeError::TimedOut),
    );
}

/// The socket file is there while any handle to the listener is,
/// and goes with the last one, so the path can be used again
#[test]
fn the_socket_file_goes_with_the_last_listener() {
    let path = sock("file");

    let listener = Runtime::block(Unix::listen(&path)).unwrap();
    assert!(present(&path), "listening makes a socket file");

    assert_eq!(
        Runtime::block(Unix::listen(&path)).map(|_| ()),
        Err(RuntimeError::CheckError(Some(libc::EADDRINUSE))),
        "a path in use can't be listened on twice",
    );

    let copy = listener.clone();
    listener.close();
    assert!(present(&path), "a copy still holds the listener");

    copy.close();
    assert!(!present(&path), "the last handle going removes the file");

    let again = Runtime::block(Unix::listen(&path)).expect("the path is free again");
    again.close();
}

/// A file somebody else put at the path since is not the
/// listener's to remove
#[test]
fn a_file_put_in_its_place_is_left_alone() {
    let path = sock("replaced");

    let listener = Runtime::block(Unix::listen(&path)).unwrap();

    fs::remove_file(&path).unwrap();
    fs::write(&path, "not a socket").unwrap();

    listener.close();

    assert_eq!(
        fs::read_to_string(&path).unwrap(),
        "not a socket",
        "a closing listener removed a file it didn't make",
    );

    fs::remove_file(&path).unwrap();
}

/// Connecting to a path with nothing at it is the kernel's
/// `ENOENT`
#[test]
fn connecting_to_nothing_is_not_found() {
    let path = sock("missing");

    assert_eq!(
        Runtime::block(Unix::connect(&path)).map(|_| ()),
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
    );
}

/// A path longer than the kernel has room for is refused before
/// it gets there
#[test]
fn a_path_too_long_is_a_bad_path() {
    let _ = Runtime::init();

    let path = format!("/tmp/{}", "x".repeat(200));

    assert_eq!(
        Runtime::block(Unix::listen(&path)).map(|_| ()),
        Err(RuntimeError::BadPath),
    );
}

/// A datagram arrives whole, with the path of the socket that
/// sent it
#[test]
fn a_unix_datagram_arrives_with_its_sender() {
    let a = Runtime::block(Unix::bind(sock("dgram-a"))).unwrap();
    let b = Runtime::block(Unix::bind(sock("dgram-b"))).unwrap();

    assert_eq!(
        Runtime::block(a.send_to(b.path(), b"hi".as_slice())).unwrap(),
        2
    );

    let (data, from) = within(b.recv_from(), PATIENCE).unwrap();

    assert_eq!(data, b"hi");
    assert_eq!(from.as_deref(), Some(a.path()));
}

/// Two sends are two receives
#[test]
fn unix_datagrams_keep_their_boundaries() {
    let a = Runtime::block(Unix::bind(sock("bounds-a"))).unwrap();
    let b = Runtime::block(Unix::bind(sock("bounds-b"))).unwrap();

    Runtime::block(a.send_to(b.path(), b"one".as_slice())).unwrap();
    Runtime::block(a.send_to(b.path(), b"two".as_slice())).unwrap();

    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, b"one");
    assert_eq!(within(b.recv_from(), PATIENCE).unwrap().0, b"two");
}

/// A spawned datagram receive parks until one comes
#[test]
fn a_spawned_datagram_receive_waits() {
    let a = Runtime::block(Unix::bind(sock("wait-a"))).unwrap();
    let b = Runtime::block(Unix::bind(sock("wait-b"))).unwrap();

    let reading = Runtime::task(b.recv_from()).spawn();
    until_started(&reading, PATIENCE);

    assert!(reading.is_running(), "the receive is parked");

    Runtime::block(a.send_to(b.path(), b"late".as_slice())).unwrap();

    assert_eq!(
        reading.take_with_timeout(PATIENCE).unwrap().unwrap().0,
        b"late"
    );
}

/// A datagram receive with nothing coming gives up at its
/// timeout
#[test]
fn a_unix_datagram_receive_times_out() {
    let b = Runtime::block(Unix::bind(sock("quiet"))).unwrap();

    assert_eq!(
        within(b.recv_from(), Duration::from_millis(100)),
        Err(RuntimeError::TimedOut),
    );
}

/// Sending to a path nobody is bound at is an error, not a
/// datagram lost in silence
#[test]
fn sending_to_nobody_is_an_error() {
    let a = Runtime::block(Unix::bind(sock("lonely"))).unwrap();

    assert_eq!(
        Runtime::block(a.send_to(sock("nobody"), b"x".as_slice())),
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
    );
}

/// A datagram socket's file goes with its last handle too
#[test]
fn the_datagram_socket_file_goes_with_it() {
    let path = sock("dgram-file");

    let socket = Runtime::block(Unix::bind(&path)).unwrap();
    assert!(present(&path), "binding makes a socket file");

    socket.close();
    assert!(!present(&path), "the last handle going removes the file");
}

/// Finishing a Unix connection ends the other side's read
#[test]
fn finishing_a_unix_connection_ends_the_other_read() {
    let (_listener, client, server) = pair("finish");

    Runtime::block(client.send(b"done".as_slice())).unwrap();
    Runtime::block(client.finish()).unwrap();

    assert_eq!(Runtime::block(server.recv_to_end()), Ok(b"done".to_vec()));
}

/// A listener with a short backlog still takes connections
#[test]
fn a_unix_listener_takes_a_backlog() {
    let _ = Runtime::init();

    let listener = Runtime::block(Unix::listen(sock("backlog")).backlog(2)).unwrap();
    let accepting = Runtime::task(listener.accept()).spawn();

    let client = Runtime::block(Unix::connect(listener.path())).unwrap();
    let server = accepting.take_with_timeout(PATIENCE).unwrap().unwrap();

    Runtime::block(client.send(b"in".as_slice())).unwrap();
    assert_eq!(Runtime::block(server.recv(8)), Ok(b"in".to_vec()));
}

/// A pair talks both ways with nothing on disk, and each end knows
/// who the other is
#[test]
fn a_pair_talks_both_ways() {
    let _ = Runtime::init();

    let (left, right) = Runtime::block(Unix::pair()).expect("a pair must open");

    assert_eq!(left.path(), Path::new(""));

    Runtime::block(left.send(b"ping".as_slice())).unwrap();
    assert_eq!(within(right.recv(16), PATIENCE), Ok(b"ping".to_vec()));

    Runtime::block(right.send(b"pong".as_slice())).unwrap();
    assert_eq!(within(left.recv(16), PATIENCE), Ok(b"pong".to_vec()));

    let them = left.peer_credentials().unwrap();

    assert_eq!(them.uid(), unsafe { libc::geteuid() });
    assert_eq!(them.gid(), unsafe { libc::getegid() });
}

/// An unbound socket can send but has no path of its own
#[test]
fn an_unbound_socket_sends() {
    let _ = Runtime::init();

    let receiver = Runtime::block(Unix::bind(sock("unbound-to"))).unwrap();
    let sender = Runtime::block(Unix::unbound()).unwrap();

    assert_eq!(sender.path(), Path::new(""));

    Runtime::block(sender.send_to(receiver.path(), b"anonymous".as_slice())).unwrap();

    let (data, from) = within(receiver.recv_from(), PATIENCE).unwrap();

    assert_eq!(data, b"anonymous");
    assert_eq!(from, None, "an unbound sender has no path to reply to");
}
