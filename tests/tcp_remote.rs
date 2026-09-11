//! TCP against a real server over the internet
//!
//! Ignored by default, since it needs the network. Run it with
//! `cargo test --test tcp_remote -- --ignored --nocapture`
//!
//! #### Note
//! Plain HTTP only, since the TCP tasks don't speak TLS. The
//! site redirects HTTP to HTTPS, so a redirect is a pass: the
//! request still went out over a real connection and a whole
//! answer came back

use atap::{Runtime, Tcp};
use std::time::Duration;

/// The site, and the file on it
const HOST: &str = "www.req-audio.com";
const PATH: &str = "/version.json";

/// Asks the site for its version file and prints what comes back
#[test]
#[ignore = "needs the network"]
fn the_site_answers_a_request_for_its_version() {
    Runtime::init();

    let request = format!(
        "GET {PATH} HTTP/1.0\r\nHost: {HOST}\r\nUser-Agent: atap-test\r\nConnection: close\r\n\r\n"
    );

    let reply = Runtime::block(
        Tcp::request(format!("{HOST}:80"), request.into_bytes()).timeout(Duration::from_secs(10)),
    )
    .expect("the site must answer");

    let text = String::from_utf8_lossy(&reply);

    println!("---- {HOST}{PATH} ----\n{text}\n----");

    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("an HTTP answer has a head and a body");

    let status = head.lines().next().unwrap_or_default();

    assert!(status.starts_with("HTTP/1."), "not an HTTP answer: {status:?}");

    match status.split_whitespace().nth(1) {
        Some("200") => assert!(
            body.contains("\"version\""),
            "a 200 from the version file carries a version, got {body:?}",
        ),

        Some("301" | "302" | "307" | "308") => {
            let location = head
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("location").then(|| value.trim())
                })
                .expect("a redirect says where to");

            assert_eq!(location, format!("https://{HOST}{PATH}"));
        }

        other => panic!("unexpected status {other:?} in {status:?}"),
    }
}
