//! Its own binary, since it asserts on the whole pool and any
//! other test running beside it would show up in the counts

mod common;

use atap::{Connection, Runtime, Tcp};
use common::{cores, report};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Connections held open at once, each with a receive parked on it
const WAITING: usize = 500;

/// How long the test waits for anything that ought to be quick
const PATIENCE: Duration = Duration::from_secs(20);

/// Lets the process hold two descriptors per connection, since
/// the default soft limit is 256
fn raise_descriptor_limit() {
    let mut limit = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };

    unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) };

    let wanted = (WAITING as libc::rlim_t) * 2 + 256;
    limit.rlim_cur = limit.rlim_cur.max(wanted.min(limit.rlim_max));

    unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &limit) };
}

/// Hundreds of receives waiting on silent connections hold no
/// worker and no sleep thread, and every one of them still
/// finishes once its data arrives
#[test]
fn parked_receives_hold_no_thread() {
    raise_descriptor_limit();
    Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();

    let pairs: Vec<(Connection, Connection)> = (0..WAITING)
        .map(|_| {
            let client = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();
            let (server, _) = Runtime::block(listener.accept()).unwrap();

            (client, server)
        })
        .collect();

    let handles: Vec<_> = pairs
        .iter()
        .map(|(_, server)| Runtime::task(server.recv(16)).spawn())
        .collect();

    // Past every one reaching its park, and past enough manager
    // ticks for the pool to have grown if it was going to
    thread::sleep(Duration::from_millis(300));

    let stats = Runtime::workers();
    report("receives parked");

    let parked = handles.iter().filter(|handle| handle.is_running()).count();

    println!(
        "{} receives waiting: {} parked, {} workers busy, {} sleep threads ({} busy), cap {}",
        WAITING,
        parked,
        stats.busy(),
        stats.sleep_threads(),
        stats.sleep_busy(),
        cores() * 8,
    );

    assert_eq!(parked, WAITING, "every receive is waiting on the network");
    assert_eq!(stats.busy(), 0, "parked receives held {} workers", stats.busy());
    assert_eq!(
        stats.sleep_busy(),
        0,
        "parked receives held {} sleep threads",
        stats.sleep_busy(),
    );
    assert_eq!(stats.backlog(), 0, "parked receives left {} queued", stats.backlog());

    for (client, _) in &pairs {
        Runtime::block(client.send(b"x".as_slice())).unwrap();
    }

    let deadline = Instant::now() + PATIENCE;

    for handle in handles {
        let left = deadline.saturating_duration_since(Instant::now());

        let got = handle
            .take_with_timeout(left)
            .expect("every parked receive wakes once its data is there")
            .expect("every receive succeeds");

        assert_eq!(got, b"x");
    }
}
