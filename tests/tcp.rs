//! TCP task tests
//!
//! Everything runs over loopback, against listeners on port 0

use atap::{Connection, Listener, Runtime, RuntimeError, Tcp, TaskHandle};
use std::{
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A listener on a free loopback port
fn listener() -> Listener {
    Runtime::init();

    Runtime::block(Tcp::listen("127.0.0.1:0")).expect("a loopback listener must open")
}

/// Both ends of one loopback connection, the connecting end first
fn pair() -> (Connection, Connection) {
    let listener = listener();
    let accepting = Runtime::task(listener.accept()).spawn();

    let client =
        Runtime::block(Tcp::connect(listener.local_addr())).expect("loopback must connect");

    let (server, _) = accepting
        .take_with_timeout(PATIENCE)
        .expect("the accept must settle")
        .expect("the accept must succeed");

    (client, server)
}

/// Waits until a spawned task is parked or running, so what the
/// test does next lands while it waits
fn until_started<T>(handle: &TaskHandle<T>) {
    let deadline = Instant::now() + PATIENCE;

    while handle.is_pending() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    // Long enough for it to reach its park
    thread::sleep(Duration::from_millis(20));
}

/// The two ends of a connection agree on who is who
#[test]
fn both_ends_agree_on_their_addresses() {
    let (client, server) = pair();

    assert_eq!(client.peer_addr(), server.local_addr());
    assert_eq!(client.local_addr(), server.peer_addr());
}

/// What one side sends, the other receives
#[test]
fn a_send_arrives_at_the_other_end() {
    let (client, server) = pair();

    let sent = Runtime::block(client.send(b"hello".as_slice())).expect("the send must work");
    assert_eq!(sent, 5);

    let got = Runtime::block(server.recv(64)).expect("the receive must work");
    assert_eq!(got, b"hello");
}

/// A spawned receive parks until data comes, then finishes
#[test]
fn a_spawned_receive_waits_for_data() {
    let (client, server) = pair();

    let reading = Runtime::task(server.recv_exact(10)).spawn();
    until_started(&reading);

    assert!(
        reading.is_running(),
        "a receive waiting on the network reads as running, got {:?}",
        reading.state(),
    );

    Runtime::block(client.send(b"01234".as_slice())).unwrap();
    thread::sleep(Duration::from_millis(20));

    assert!(!reading.settled(), "half of what it wants isn't enough");

    Runtime::block(client.send(b"56789".as_slice())).unwrap();

    let got = reading
        .take_with_timeout(PATIENCE)
        .expect("the receive must settle once everything is there")
        .expect("the receive must succeed");

    assert_eq!(got, b"0123456789");
}

/// A delimited receive stops at its delimiter, and the bytes
/// after it go to the next receive
#[test]
fn a_delimited_receive_leaves_the_rest() {
    let (client, server) = pair();

    Runtime::block(client.send(b"one\ntwo\nthree".as_slice())).unwrap();

    // All in one read, so the rest has to be carried over
    thread::sleep(Duration::from_millis(20));

    assert_eq!(Runtime::block(server.recv_until(b"\n", 64)).unwrap(), b"one\n");
    assert_eq!(Runtime::block(server.recv_until(b"\n", 64)).unwrap(), b"two\n");
    assert_eq!(Runtime::block(server.recv(64)).unwrap(), b"three");
}

/// A delimiter split across two sends is still found
#[test]
fn a_delimiter_can_straddle_two_reads() {
    let (client, server) = pair();

    let reading = Runtime::task(server.recv_until(b"\r\n", 64)).spawn();
    until_started(&reading);

    Runtime::block(client.send(b"line\r".as_slice())).unwrap();
    thread::sleep(Duration::from_millis(20));
    Runtime::block(client.send(b"\nnext".as_slice())).unwrap();

    let got = reading.take_with_timeout(PATIENCE).unwrap().unwrap();
    assert_eq!(got, b"line\r\n");
    assert_eq!(Runtime::block(server.recv(64)).unwrap(), b"next");
}

/// Running out of room before the delimiter is `TooLong`, and
/// what was read is still there
#[test]
fn a_delimiter_past_the_limit_is_too_long() {
    let (client, server) = pair();

    Runtime::block(client.send(b"far too long a line\n".as_slice())).unwrap();
    thread::sleep(Duration::from_millis(20));

    assert_eq!(
        Runtime::block(server.recv_until(b"\n", 8)),
        Err(RuntimeError::TooLong),
    );

    assert_eq!(
        Runtime::block(server.recv_until(b"\n", 64)).unwrap(),
        b"far too long a line\n",
        "a receive that fails puts back what it read",
    );
}

/// Reading to the end stops when the other side closes
#[test]
fn reading_to_the_end_stops_at_the_close() {
    let (client, server) = pair();

    let reading = Runtime::task(server.recv_to_end()).spawn();

    Runtime::block(client.send(b"all of ".as_slice())).unwrap();
    Runtime::block(client.send(b"this".as_slice())).unwrap();
    client.close();

    let got = reading.take_with_timeout(PATIENCE).unwrap().unwrap();
    assert_eq!(got, b"all of this");
}

/// An empty receive is the other side having closed
#[test]
fn an_empty_receive_means_the_other_side_closed() {
    let (client, server) = pair();

    client.close();

    assert_eq!(Runtime::block(server.recv(64)).unwrap(), b"");
}

/// A connection closing part way through an exact receive is
/// `Closed`
#[test]
fn closing_part_way_through_an_exact_receive_is_closed() {
    let (client, server) = pair();

    Runtime::block(client.send(b"abc".as_slice())).unwrap();
    client.close();

    assert_eq!(
        Runtime::block(server.recv_exact(10)),
        Err(RuntimeError::Closed),
    );
}

/// Several megabytes each way, which is more than one step and
/// more than the socket buffers hold
#[test]
fn a_large_transfer_arrives_whole() {
    let (client, server) = pair();

    let data: Vec<u8> = (0..8 * 1024 * 1024).map(|at| (at % 251) as u8).collect();

    let reading = Runtime::task(server.recv_exact(data.len())).spawn();
    let sending = Runtime::task(client.send(data.clone())).spawn();

    let sent = sending.take_with_timeout(PATIENCE).unwrap().unwrap();
    let got = reading.take_with_timeout(PATIENCE).unwrap().unwrap();

    assert_eq!(sent, data.len());
    assert!(got == data, "every byte arrives, in order");
}

/// A send and a receive can wait on one connection at once
/// without taking each other's wake
#[test]
fn a_send_and_a_receive_can_wait_on_one_socket() {
    let (client, server) = pair();

    let reading = Runtime::task(client.recv_exact(4)).spawn();
    until_started(&reading);

    Runtime::block(client.send(b"ping".as_slice())).unwrap();
    assert_eq!(Runtime::block(server.recv_exact(4)).unwrap(), b"ping");

    Runtime::block(server.send(b"pong".as_slice())).unwrap();
    assert_eq!(reading.take_with_timeout(PATIENCE).unwrap().unwrap(), b"pong");
}

/// Two accepts parked on one listener each get a connection,
/// rather than one replacing the other's watch
#[test]
fn two_accepts_can_wait_on_one_listener() {
    let listener = listener();

    let first = Runtime::task(listener.accept()).spawn();
    let second = Runtime::task(listener.accept()).spawn();
    until_started(&first);
    until_started(&second);

    let _one = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();
    let _two = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();

    assert!(first.take_with_timeout(PATIENCE).unwrap().is_ok());
    assert!(second.take_with_timeout(PATIENCE).unwrap().is_ok());
}

/// A name is looked up when the task runs, and every address it
/// gives is tried until one takes
#[test]
fn a_name_is_looked_up_and_tried() {
    let listener = listener();
    let port = listener.local_addr().port();

    let accepting = Runtime::task(listener.accept()).spawn();

    // `localhost` can give `::1` first, which nothing is listening on
    let conn = Runtime::block(Tcp::connect(format!("localhost:{port}")))
        .expect("one of localhost's addresses must take");

    assert_eq!(conn.peer_addr().port(), port);
    assert!(accepting.take_with_timeout(PATIENCE).unwrap().is_ok());
}

/// Nobody listening is a refusal from the kernel
#[test]
fn nobody_listening_is_refused() {
    let port = {
        let listener = listener();
        listener.local_addr().port()
    };

    assert_eq!(
        Runtime::block(Tcp::connect(format!("127.0.0.1:{port}")).timeout(PATIENCE)).map(|_| ()),
        Err(RuntimeError::CheckError(Some(libc::ECONNREFUSED))),
    );
}

/// Something that isn't an address fails as one
#[test]
fn a_nonsense_address_is_a_bad_address() {
    Runtime::init();

    assert_eq!(
        Runtime::block(Tcp::connect("not an address")).map(|_| ()),
        Err(RuntimeError::BadAddress),
    );
}

/// A blocking receive on a silent connection gives up at its
/// timeout
#[test]
fn a_blocking_receive_times_out() {
    let (_client, server) = pair();

    let started = Instant::now();
    let got = Runtime::block(server.recv(64).timeout(Duration::from_millis(100)));
    let took = started.elapsed();

    assert_eq!(got, Err(RuntimeError::TimedOut));
    assert!(took >= Duration::from_millis(100), "gave up early, after {took:?}");
    assert!(took < Duration::from_secs(2), "gave up late, after {took:?}");
}

/// So does a spawned one, parked with nothing holding its thread
#[test]
fn a_spawned_receive_times_out() {
    let (_client, server) = pair();

    let started = Instant::now();
    let handle = Runtime::task(server.recv(64).timeout(Duration::from_millis(100))).spawn();
    let got = handle.take_with_timeout(PATIENCE).expect("the timeout must settle it");
    let took = started.elapsed();

    assert_eq!(got, Err(RuntimeError::TimedOut));
    assert!(took >= Duration::from_millis(100), "gave up early, after {took:?}");
    assert!(took < Duration::from_secs(2), "gave up late, after {took:?}");
}

/// A receive that times out part way puts back what it had,
/// so nothing is lost
#[test]
fn a_timed_out_receive_keeps_its_bytes() {
    let (client, server) = pair();

    Runtime::block(client.send(b"abc".as_slice())).unwrap();

    assert_eq!(
        Runtime::block(server.recv_exact(10).timeout(Duration::from_millis(50))),
        Err(RuntimeError::TimedOut),
    );

    assert_eq!(Runtime::block(server.recv(64)).unwrap(), b"abc");
}

/// Cancelling a parked receive settles it at once, without
/// waiting for the silent peer
#[test]
fn a_parked_receive_can_be_cancelled() {
    let (client, server) = pair();

    let reading = Runtime::task(server.recv_exact(10)).spawn();
    until_started(&reading);

    Runtime::block(client.send(b"part".as_slice())).unwrap();
    thread::sleep(Duration::from_millis(20));

    reading.clone().cancel();

    assert_eq!(
        reading.join_with_timeout(PATIENCE),
        Err(RuntimeError::Cancelled),
    );

    assert_eq!(
        Runtime::block(server.recv(64)).unwrap(),
        b"part",
        "a cancelled receive puts back what it had read",
    );
}

/// A repeating send runs again with its progress reset, and
/// every run's bytes arrive
#[test]
fn a_repeating_send_sends_every_run() {
    let (client, server) = pair();

    let handle = Runtime::task(client.send(b"xy".as_slice()))
        .repeat()
        .count(3)
        .every(Duration::from_millis(10))
        .spawn();

    assert_eq!(Runtime::block(server.recv_exact(6)).unwrap(), b"xyxyxy");

    let deadline = Instant::now() + PATIENCE;

    while !handle.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    assert!(handle.is_finished(), "a bounded repeat ends");
}

/// A scheduled send parks and publishes through its series like
/// any other run
#[test]
fn a_scheduled_send_sends_every_run() {
    let (client, server) = pair();

    let handle = Runtime::task(client.send(b"ab".as_slice()))
        .at_rate(Duration::from_millis(20))
        .count(3)
        .spawn();

    assert_eq!(Runtime::block(server.recv_exact(6)).unwrap(), b"ababab");

    let deadline = Instant::now() + PATIENCE;

    while !handle.is_finished() && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    assert!(handle.is_finished(), "a bounded schedule ends");
    assert_eq!(handle.join().unwrap(), Ok(2), "the last run's output is published");
}

/// A request connects, sends, and reads everything back
#[test]
fn a_request_reads_the_whole_answer() {
    let listener = listener();
    let addr = listener.local_addr();

    let server = thread::spawn(move || {
        let (conn, _) = Runtime::block(listener.accept()).unwrap();
        let asked = Runtime::block(conn.recv_until(b"\r\n\r\n", 1024)).unwrap();

        Runtime::block(conn.send(b"HTTP/1.0 200 OK\r\n\r\nhi".as_slice())).unwrap();

        asked
    });

    let reply = Runtime::block(
        Tcp::request(addr, b"GET / HTTP/1.0\r\n\r\n".as_slice()).timeout(PATIENCE),
    )
    .expect("the request must be answered");

    assert_eq!(reply, b"HTTP/1.0 200 OK\r\n\r\nhi");
    assert_eq!(server.join().unwrap(), b"GET / HTTP/1.0\r\n\r\n");
}

/// Closing a connection doesn't take away anything already
/// received from it
#[test]
fn an_output_outlives_closing_its_connection() {
    let (client, server) = pair();

    let reading = Runtime::task(server.recv(64)).spawn();
    Runtime::block(client.send(b"kept".as_slice())).unwrap();

    reading.wait().unwrap();
    server.close();

    assert_eq!(reading.join().unwrap().unwrap(), b"kept");
}

/// A receive spawned before a close still completes, since it
/// holds the connection too
#[test]
fn a_receive_in_flight_outlives_a_close() {
    let (client, server) = pair();

    let reading = Runtime::task(server.recv(64)).spawn();
    until_started(&reading);

    server.close();
    Runtime::block(client.send(b"late".as_slice())).unwrap();

    assert_eq!(reading.take_with_timeout(PATIENCE).unwrap().unwrap(), b"late");
}

/// The socket only closes when the last handle to it goes, and
/// a task handle whose output holds one counts
#[test]
fn the_socket_closes_with_its_last_handle() {
    let listener = listener();

    let accepting = Runtime::task(listener.accept()).spawn();
    let client = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();

    let (server, _) = accepting.join_with_timeout(PATIENCE).unwrap().unwrap();

    let reading = Runtime::task(client.recv_to_end()).spawn();
    until_started(&reading);

    // The accept's slot still holds a copy
    server.close();
    thread::sleep(Duration::from_millis(50));

    assert!(
        !reading.settled(),
        "the other side still sees the connection open while a handle holds it",
    );

    drop(accepting);

    assert_eq!(
        reading.take_with_timeout(PATIENCE).unwrap().unwrap(),
        b"",
        "the last handle going closes the socket",
    );
}
