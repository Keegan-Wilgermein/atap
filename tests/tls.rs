//! TLS task tests
//!
//! Only built with `--features tls`. Every test mints its own
//! throwaway certificate authority and a `localhost` certificate
//! from it, serves with `Tls::listen`, and connects with that
//! authority trusted on top of the system's

#![cfg(feature = "tls")]

mod common;

use atap::{
    Runtime, RuntimeError,
    tcp::Tcp,
    tls::{Tls, TlsConnectTask, TlsConnection, TlsListener},
};
use common::{certs, until_started, within};
use std::{
    thread,
    time::{Duration, Instant},
};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A TLS listener on a free loopback port, and the authority its
/// certificate came from
fn server(name: &str) -> (TlsListener, String) {
    let _ = Runtime::init();

    let certs = certs(name);
    let listener = Runtime::block(Tls::listen("127.0.0.1:0", &certs.cert, &certs.key))
        .expect("a TLS listener must open with a fresh certificate");

    (listener, certs.ca)
}

/// A connect to `listener` that checks for `localhost` and trusts
/// the test's authority
fn connect_to(listener: &TlsListener, ca: &str) -> TlsConnectTask {
    Tls::connect(listener.local_addr())
        .server_name("localhost")
        .trust(ca.as_bytes())
}

/// A listener, and both ends of one TLS connection to it, the
/// connecting end first
fn pair(name: &str) -> (TlsListener, TlsConnection, TlsConnection) {
    let (listener, ca) = server(name);

    let accepting = Runtime::task(listener.accept()).timeout(PATIENCE).spawn();
    let client = Runtime::block(connect_to(&listener, &ca)).expect("the handshake must succeed");

    let (server, _) = accepting
        .take_with_timeout(PATIENCE)
        .expect("the accept must settle")
        .expect("the server's side of the handshake must succeed");

    (listener, client, server)
}

/// Bytes go both ways, encrypted, with the same send and receive
/// tasks as TCP
#[test]
fn a_tls_connection_carries_bytes_both_ways() {
    let (_listener, client, server) = pair("both");

    assert_eq!(client.peer_addr(), server.local_addr());

    Runtime::block(client.send(b"ping".as_slice())).unwrap();
    assert_eq!(Runtime::block(server.recv_exact(4)).unwrap(), b"ping");

    Runtime::block(server.send(b"pong".as_slice())).unwrap();
    assert_eq!(Runtime::block(client.recv_exact(4)).unwrap(), b"pong");
}

/// A delimited receive leaves the rest for the next one
#[test]
fn a_delimited_tls_receive_leaves_the_rest() {
    let (_listener, client, server) = pair("lines");

    Runtime::block(client.send(b"one\ntwo\nthree".as_slice())).unwrap();

    assert_eq!(
        Runtime::block(server.recv_until(b"\n", 64)).unwrap(),
        b"one\n"
    );
    assert_eq!(
        Runtime::block(server.recv_until(b"\n", 64)).unwrap(),
        b"two\n"
    );
    assert_eq!(Runtime::block(server.recv_exact(5)).unwrap(), b"three");
}

/// Closing the session properly ends a read to the end cleanly
#[test]
fn a_clean_close_ends_a_read_to_the_end() {
    let (_listener, client, server) = pair("close");

    let reading = Runtime::task(server.recv_to_end())
        .timeout(PATIENCE)
        .spawn();

    Runtime::block(client.send(b"all of ".as_slice())).unwrap();
    Runtime::block(client.send(b"this".as_slice())).unwrap();
    client.close();

    assert_eq!(
        reading.take_with_timeout(PATIENCE).unwrap().unwrap(),
        b"all of this"
    );
}

/// Megabytes each way, far past what the session buffers, so the
/// sends have to wait for room and flush their last records
#[test]
fn a_large_tls_transfer_arrives_whole() {
    let (_listener, client, server) = pair("large");

    let data: Vec<u8> = (0..8 * 1024 * 1024).map(|at| (at % 251) as u8).collect();

    for (from, to) in [(&client, &server), (&server, &client)] {
        let reading = Runtime::task(to.recv_exact(data.len()))
            .timeout(PATIENCE)
            .spawn();
        let sending = Runtime::task(from.send(data.clone()))
            .timeout(PATIENCE)
            .spawn();

        assert_eq!(
            sending.take_with_timeout(PATIENCE).unwrap().unwrap(),
            data.len()
        );

        let got = reading.take_with_timeout(PATIENCE).unwrap().unwrap();
        assert!(got == data, "every byte arrives, in order");
    }
}

/// A request runs the handshake, sends, and reads everything back
#[test]
fn a_tls_request_reads_the_whole_answer() {
    let (listener, ca) = server("request");
    let addr = listener.local_addr();

    let serving = thread::spawn(move || {
        let (conn, _) = within(listener.accept(), PATIENCE).unwrap();
        let asked = Runtime::block(conn.recv_until(b"\r\n\r\n", 1024)).unwrap();

        Runtime::block(conn.send(b"HTTP/1.0 200 OK\r\n\r\nhi".as_slice())).unwrap();

        asked
    });

    let reply = within(
        Tls::request(addr, b"GET / HTTP/1.0\r\n\r\n".as_slice())
            .server_name("localhost")
            .trust(ca.as_bytes()),
        PATIENCE,
    )
    .expect("the request must be answered");

    assert_eq!(reply, b"HTTP/1.0 200 OK\r\n\r\nhi");
    assert_eq!(serving.join().unwrap(), b"GET / HTTP/1.0\r\n\r\n");
}

/// A certificate for a different name is refused
#[test]
fn the_wrong_name_is_a_bad_certificate() {
    let (listener, ca) = server("name");

    // The server's half fails too, and isn't what is being tested
    let _accepting = Runtime::task(listener.accept()).timeout(PATIENCE).spawn();

    let got = within(
        Tls::connect(listener.local_addr())
            .server_name("not-localhost")
            .trust(ca.as_bytes()),
        PATIENCE,
    );

    assert_eq!(got.map(|_| ()), Err(RuntimeError::BadCertificate));
}

/// A certificate from an authority nobody trusts is refused
#[test]
fn an_untrusted_authority_is_a_bad_certificate() {
    let (listener, _ca) = server("untrusted");

    let _accepting = Runtime::task(listener.accept()).timeout(PATIENCE).spawn();

    let got = within(
        Tls::connect(listener.local_addr()).server_name("localhost"),
        PATIENCE,
    );

    assert_eq!(got.map(|_| ()), Err(RuntimeError::BadCertificate));
}

/// A server that never answers the handshake is given up on at
/// the timeout
#[test]
fn a_silent_server_times_the_handshake_out() {
    let _ = Runtime::init();

    // Plain TCP, so the connect lands and the handshake is ignored
    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();

    let started = Instant::now();
    let got = within(
        Tls::connect(listener.local_addr()).server_name("localhost"),
        Duration::from_millis(200),
    );
    let took = started.elapsed();

    assert_eq!(got.map(|_| ()), Err(RuntimeError::TimedOut));
    assert!(
        took < Duration::from_secs(5),
        "gave up late, after {took:?}"
    );
}

/// A server that answers with something other than TLS fails the
/// handshake
#[test]
fn a_server_that_does_not_speak_tls_fails() {
    let _ = Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();
    let addr = listener.local_addr();

    let serving = thread::spawn(move || {
        let (conn, _) = Runtime::block(listener.accept()).unwrap();
        let _ = Runtime::block(conn.recv(1024));
        let _ = Runtime::block(conn.send(b"HTTP/1.0 400 Bad Request\r\n\r\n".as_slice()));
    });

    let got = within(Tls::connect(addr).server_name("localhost"), PATIENCE);

    assert_eq!(got.map(|_| ()), Err(RuntimeError::TlsFailed));

    serving.join().unwrap();
}

/// Cancelling a parked TLS receive settles it at once
#[test]
fn a_parked_tls_receive_can_be_cancelled() {
    let (_listener, _client, server) = pair("cancel");

    let reading = Runtime::task(server.recv(64)).spawn();
    until_started(&reading, PATIENCE);

    assert!(reading.is_running(), "the receive is parked");

    reading.clone().cancel();

    assert_eq!(
        reading.join_with_timeout(PATIENCE),
        Err(RuntimeError::Cancelled),
    );
}

/// A TLS receive with nothing coming gives up at its timeout
#[test]
fn a_tls_receive_times_out() {
    let (_listener, _client, server) = pair("quiet");

    assert_eq!(
        within(server.recv(64), Duration::from_millis(100)),
        Err(RuntimeError::TimedOut),
    );
}

/// Finishing a TLS connection says goodbye, so the other side's read
/// ends cleanly, and the reply still comes back
#[test]
fn finishing_a_tls_connection_ends_the_other_read() {
    let (_listener, client, server) = pair("finish");

    let reading = Runtime::task(server.recv_to_end()).spawn();

    Runtime::block(client.send(b"request".as_slice())).unwrap();
    Runtime::block(client.finish()).unwrap();

    assert_eq!(
        reading.join_with_timeout(PATIENCE),
        Ok(Ok(b"request".to_vec()))
    );

    Runtime::block(server.send(b"answer".as_slice())).unwrap();
    Runtime::block(server.finish()).unwrap();

    assert_eq!(
        within(client.recv_to_end(), PATIENCE),
        Ok(b"answer".to_vec())
    );
}


/// A listener made from certificates in memory serves like one made
/// from files, and both sides agree on a protocol
#[test]
fn alpn_is_agreed_over_an_in_memory_certificate() {
    let _ = Runtime::init();

    let certs = certs("alpn");
    let listener = Runtime::block(
        Tls::listen_pem("127.0.0.1:0", &certs.cert_pem, &certs.key_pem).alpn(["h2", "http/1.1"]),
    )
    .expect("a listener from memory must open");

    let accepting = Runtime::task(listener.accept()).spawn();

    let client = within(
        Tls::connect(listener.local_addr())
            .server_name("localhost")
            .trust(certs.ca.as_bytes())
            .alpn(["http/1.1"]),
        PATIENCE,
    )
    .expect("the handshake must succeed");

    let (server, _) = accepting.take_with_timeout(PATIENCE).unwrap().unwrap();

    assert_eq!(client.alpn(), Some(b"http/1.1".to_vec()));
    assert_eq!(server.alpn(), Some(b"http/1.1".to_vec()));

    assert!(!client.peer_certificates().is_empty(), "the server showed a certificate");
    assert!(server.peer_certificates().is_empty(), "the client showed none");
}

/// A server that requires client certificates takes one from its
/// authority and refuses a client without one
#[test]
fn a_client_certificate_can_be_required() {
    let _ = Runtime::init();

    let certs = certs("mutual");
    let listener = Runtime::block(
        Tls::listen("127.0.0.1:0", &certs.cert, &certs.key)
            .require_client_cert(certs.ca.as_bytes()),
    )
    .unwrap();

    let accepting = Runtime::task(listener.accept()).spawn();

    let client = within(
        Tls::connect(listener.local_addr())
            .server_name("localhost")
            .trust(certs.ca.as_bytes())
            .identity(&certs.client_cert, &certs.client_key),
        PATIENCE,
    )
    .expect("a client with a certificate must connect");

    let (server, _) = accepting.take_with_timeout(PATIENCE).unwrap().unwrap();

    Runtime::block(client.send(b"who".as_slice())).unwrap();
    assert_eq!(within(server.recv(8), PATIENCE), Ok(b"who".to_vec()));
    assert_eq!(server.peer_certificates().len(), 1);

    // Without one, one side or the other gives up
    let refusing = Runtime::task(listener.accept()).spawn();

    let bare = within(
        Tls::connect(listener.local_addr())
            .server_name("localhost")
            .trust(certs.ca.as_bytes()),
        PATIENCE,
    );

    let refused = refusing.take_with_timeout(PATIENCE).expect("the accept must settle");

    let failed = match (&bare, &refused) {
        (Err(_), _) | (_, Err(_)) => true,
        (Ok(conn), Ok(_)) => {
            let _ = Runtime::block(conn.send(b"x".as_slice()));
            within(conn.recv(8), PATIENCE).is_err()
        }
    };

    assert!(failed, "a client without a certificate got through");

    // A key that isn't PEM is refused before anything is sent
    assert_eq!(
        within(
            Tls::connect(listener.local_addr())
                .server_name("localhost")
                .identity(&certs.client_cert, b"not a key"),
            PATIENCE,
        )
        .map(|_| ()),
        Err(RuntimeError::BadCertificate)
    );
}

/// A connection that starts in the clear can switch to TLS
#[test]
fn a_plain_connection_can_be_upgraded() {
    let _ = Runtime::init();

    let certs = certs("upgrade");
    let tls = Runtime::block(Tls::listen_pem("127.0.0.1:0", &certs.cert_pem, &certs.key_pem)).unwrap();
    let plain = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();

    let accepting = Runtime::task(plain.accept()).spawn();
    let client_tcp = Runtime::block(Tcp::connect(plain.local_addr())).unwrap();
    let (server_tcp, _) = accepting.take_with_timeout(PATIENCE).unwrap().unwrap();

    Runtime::block(client_tcp.send(b"STARTTLS\n".as_slice())).unwrap();
    assert_eq!(
        within(server_tcp.recv_until(b"\n", 64), PATIENCE),
        Ok(b"STARTTLS\n".to_vec())
    );

    let serving = Runtime::task(tls.upgrade(server_tcp)).spawn();

    let client = within(
        Tls::upgrade(client_tcp)
            .server_name("localhost")
            .trust(certs.ca.as_bytes()),
        PATIENCE,
    )
    .expect("the upgrade must handshake");

    let (server, _) = serving.take_with_timeout(PATIENCE).unwrap().unwrap();

    Runtime::block(client.send(b"secret".as_slice())).unwrap();
    assert_eq!(within(server.recv(16), PATIENCE), Ok(b"secret".to_vec()));
}

/// A certificate in memory that isn't one is refused
#[test]
fn a_bad_in_memory_certificate_is_refused() {
    let _ = Runtime::init();

    assert_eq!(
        Runtime::block(Tls::listen_pem("127.0.0.1:0", "nothing", "here")).map(|_| ()),
        Err(RuntimeError::BadCertificate)
    );
}
