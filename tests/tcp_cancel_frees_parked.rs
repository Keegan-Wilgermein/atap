//! Its own binary, since it asserts on the number of live tasks
//! in the whole process

mod common;

use atap::{Connection, Runtime, Tcp};
use common::report;
use std::{
    thread,
    time::{Duration, Instant},
};

/// Receives parked and then cancelled
const WAITING: usize = 100;

/// How long the test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(10);

/// Cancelling parked receives frees their slots straight away,
/// and lets go of their connections, so a socket nothing else
/// holds closes
#[test]
fn cancelling_parked_receives_frees_them() {
    Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();

    let clients: Vec<Connection> = (0..WAITING)
        .map(|_| Runtime::block(Tcp::connect(listener.local_addr())).unwrap())
        .collect();

    let servers: Vec<Connection> = (0..WAITING)
        .map(|_| Runtime::block(listener.accept()).unwrap().0)
        .collect();

    let before = Runtime::workers().live();

    // Each receive holds its own copy of its connection, and the
    // test's copies go, so the receives are the last holders
    let handles: Vec<_> = servers
        .into_iter()
        .map(|server| Runtime::task(server.recv(16)).spawn())
        .collect();

    thread::sleep(Duration::from_millis(100));
    report("receives parked");

    assert!(
        handles.iter().all(|handle| handle.is_running()),
        "every receive is parked",
    );

    let parked = Runtime::workers().live();

    assert!(
        parked >= before + WAITING,
        "{} receives parked, but live only went from {} to {}",
        WAITING,
        before,
        parked,
    );

    for handle in handles {
        handle.cancel();
    }

    let deadline = Instant::now() + PATIENCE;

    while Runtime::workers().live() > before && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }

    report("receives cancelled");

    assert_eq!(
        Runtime::workers().live(),
        before,
        "cancelled parked receives still hold their slots",
    );

    // Their connections went with them, so the other ends see
    // them close
    for client in &clients {
        let got = Runtime::block(client.recv(16).timeout(PATIENCE));

        assert_eq!(got, Ok(Vec::new()), "a cancelled receive let go of its socket");
    }
}
