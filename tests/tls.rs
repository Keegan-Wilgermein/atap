//! TLS task tests
//!
//! Only built with `--features tls`. Every test mints its own
//! throwaway certificate authority and a `localhost` certificate
//! from it, serves with `Tls::listen`, and connects with that
//! authority trusted on top of the system's

#![cfg(feature = "tls")]

use atap::{
    Runtime, RuntimeError, TaskHandle, Tcp, Tls, TlsConnectTask, TlsConnection, TlsListener,
};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use std::{
    fs,
    path::PathBuf,
    process, thread,
    time::{Duration, Instant},
};
use time::{Duration as Span, OffsetDateTime};

/// How long a test waits for something that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// A test's certificate authority, and the server certificate and
/// key it issued, written where `Tls::listen` can read them
struct Certs {
    /// The authority, as PEM, for the client to trust
    ca: String,

    /// The server's certificate
    cert: PathBuf,

    /// The server's private key
    key: PathBuf,
}

/// Mints a certificate authority and a `localhost` certificate
/// signed by it
///
/// Valid from a day ago to a day from now. macOS refuses server
/// certificates valid for too long, and wants the name in the
/// subject alternative names and server auth in the extended key
/// usage
fn certs(name: &str) -> Certs {
    let now = OffsetDateTime::now_utc();

    let ca_key = KeyPair::generate().unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();

    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    ca.distinguished_name.push(DnType::CommonName, "atap test authority");
    ca.not_before = now - Span::days(1);
    ca.not_after = now + Span::days(1);

    let ca_cert = ca.self_signed(&ca_key).unwrap();
    let issuer = Issuer::new(ca, ca_key);

    let leaf_key = KeyPair::generate().unwrap();
    let mut leaf = CertificateParams::new(vec!["localhost".to_string()]).unwrap();

    leaf.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    leaf.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    leaf.distinguished_name.push(DnType::CommonName, "localhost");
    leaf.not_before = now - Span::days(1);
    leaf.not_after = now + Span::days(1);

    let leaf_cert = leaf.signed_by(&leaf_key, &issuer).unwrap();

    let dir = std::env::temp_dir().join(format!("atap-tls-{}-{name}", process::id()));
    fs::create_dir_all(&dir).unwrap();

    let cert = dir.join("cert.pem");
    let key = dir.join("key.pem");

    fs::write(&cert, leaf_cert.pem()).unwrap();
    fs::write(&key, leaf_key.serialize_pem()).unwrap();

    Certs {
        ca: ca_cert.pem(),
        cert,
        key,
    }
}

/// A TLS listener on a free loopback port, and the authority its
/// certificate came from
fn server(name: &str) -> (TlsListener, String) {
    Runtime::init();

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
        .timeout(PATIENCE)
}

/// A listener, and both ends of one TLS connection to it, the
/// connecting end first
fn pair(name: &str) -> (TlsListener, TlsConnection, TlsConnection) {
    let (listener, ca) = server(name);

    let accepting = Runtime::task(listener.accept().timeout(PATIENCE)).spawn();
    let client = Runtime::block(connect_to(&listener, &ca)).expect("the handshake must succeed");

    let (server, _) = accepting
        .take_with_timeout(PATIENCE)
        .expect("the accept must settle")
        .expect("the server's side of the handshake must succeed");

    (listener, client, server)
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

    assert_eq!(Runtime::block(server.recv_until(b"\n", 64)).unwrap(), b"one\n");
    assert_eq!(Runtime::block(server.recv_until(b"\n", 64)).unwrap(), b"two\n");
    assert_eq!(Runtime::block(server.recv_exact(5)).unwrap(), b"three");
}

/// Closing the session properly ends a read to the end cleanly
#[test]
fn a_clean_close_ends_a_read_to_the_end() {
    let (_listener, client, server) = pair("close");

    let reading = Runtime::task(server.recv_to_end().timeout(PATIENCE)).spawn();

    Runtime::block(client.send(b"all of ".as_slice())).unwrap();
    Runtime::block(client.send(b"this".as_slice())).unwrap();
    client.close();

    assert_eq!(reading.take_with_timeout(PATIENCE).unwrap().unwrap(), b"all of this");
}

/// Megabytes each way, far past what the session buffers, so the
/// sends have to wait for room and flush their last records
#[test]
fn a_large_tls_transfer_arrives_whole() {
    let (_listener, client, server) = pair("large");

    let data: Vec<u8> = (0..8 * 1024 * 1024).map(|at| (at % 251) as u8).collect();

    for (from, to) in [(&client, &server), (&server, &client)] {
        let reading = Runtime::task(to.recv_exact(data.len()).timeout(PATIENCE)).spawn();
        let sending = Runtime::task(from.send(data.clone()).timeout(PATIENCE)).spawn();

        assert_eq!(sending.take_with_timeout(PATIENCE).unwrap().unwrap(), data.len());

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
        let (conn, _) = Runtime::block(listener.accept().timeout(PATIENCE)).unwrap();
        let asked = Runtime::block(conn.recv_until(b"\r\n\r\n", 1024)).unwrap();

        Runtime::block(conn.send(b"HTTP/1.0 200 OK\r\n\r\nhi".as_slice())).unwrap();

        asked
    });

    let reply = Runtime::block(
        Tls::request(addr, b"GET / HTTP/1.0\r\n\r\n".as_slice())
            .server_name("localhost")
            .trust(ca.as_bytes())
            .timeout(PATIENCE),
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
    let _accepting = Runtime::task(listener.accept().timeout(PATIENCE)).spawn();

    let got = Runtime::block(
        Tls::connect(listener.local_addr())
            .server_name("not-localhost")
            .trust(ca.as_bytes())
            .timeout(PATIENCE),
    );

    assert_eq!(got.map(|_| ()), Err(RuntimeError::BadCertificate));
}

/// A certificate from an authority nobody trusts is refused
#[test]
fn an_untrusted_authority_is_a_bad_certificate() {
    let (listener, _ca) = server("untrusted");

    let _accepting = Runtime::task(listener.accept().timeout(PATIENCE)).spawn();

    let got = Runtime::block(
        Tls::connect(listener.local_addr())
            .server_name("localhost")
            .timeout(PATIENCE),
    );

    assert_eq!(got.map(|_| ()), Err(RuntimeError::BadCertificate));
}

/// A server that never answers the handshake is given up on at
/// the timeout
#[test]
fn a_silent_server_times_the_handshake_out() {
    Runtime::init();

    // Plain TCP, so the connect lands and the handshake is ignored
    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();

    let started = Instant::now();
    let got = Runtime::block(
        Tls::connect(listener.local_addr())
            .server_name("localhost")
            .timeout(Duration::from_millis(200)),
    );
    let took = started.elapsed();

    assert_eq!(got.map(|_| ()), Err(RuntimeError::TimedOut));
    assert!(took < Duration::from_secs(5), "gave up late, after {took:?}");
}

/// A server that answers with something other than TLS fails the
/// handshake
#[test]
fn a_server_that_does_not_speak_tls_fails() {
    Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();
    let addr = listener.local_addr();

    let serving = thread::spawn(move || {
        let (conn, _) = Runtime::block(listener.accept()).unwrap();
        let _ = Runtime::block(conn.recv(1024));
        let _ = Runtime::block(conn.send(b"HTTP/1.0 400 Bad Request\r\n\r\n".as_slice()));
    });

    let got = Runtime::block(
        Tls::connect(addr)
            .server_name("localhost")
            .timeout(PATIENCE),
    );

    assert_eq!(got.map(|_| ()), Err(RuntimeError::TlsFailed));

    serving.join().unwrap();
}

/// Cancelling a parked TLS receive settles it at once
#[test]
fn a_parked_tls_receive_can_be_cancelled() {
    let (_listener, _client, server) = pair("cancel");

    let reading = Runtime::task(server.recv(64)).spawn();
    until_started(&reading);

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
        Runtime::block(server.recv(64).timeout(Duration::from_millis(100))),
        Err(RuntimeError::TimedOut),
    );
}
