//! TLS against a real server over the internet
//!
//! Only built with `--features tls`, and ignored by default since
//! it needs the network. Run it with
//! `cargo test --features tls --test tls_remote -- --ignored --nocapture`

#![cfg(feature = "tls")]

use atap::{Runtime, Tls};
use std::time::Duration;

/// The site, and the file on it
const HOST: &str = "www.req-audio.com";
const PATH: &str = "/version.json";

/// Asks the site for its version file over HTTPS and prints what
/// comes back
#[test]
#[ignore = "needs the network"]
fn the_site_answers_with_its_version_over_tls() {
    Runtime::init();

    let request = format!(
        "GET {PATH} HTTP/1.0\r\nHost: {HOST}\r\nUser-Agent: atap-test\r\nConnection: close\r\n\r\n"
    );

    let reply = Runtime::block(
        Tls::request(format!("{HOST}:443"), request.into_bytes()).timeout(Duration::from_secs(10)),
    )
    .expect("the site must answer over TLS");

    let text = String::from_utf8_lossy(&reply);

    println!("---- https://{HOST}{PATH} ----\n{text}\n----");

    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("an HTTP answer has a head and a body");

    let status = head.lines().next().unwrap_or_default();

    assert!(
        status.starts_with("HTTP/1.") && status.split_whitespace().nth(1) == Some("200"),
        "expected a 200, got {status:?}",
    );

    assert!(body.contains("\"version\""), "the version file carries a version, got {body:?}");
}
