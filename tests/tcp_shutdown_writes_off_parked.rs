//! Its own binary, since shutting down stops the runtime for
//! every test in the process

use atap::{Runtime, RuntimeError, Tcp};
use std::{
    thread,
    time::{Duration, Instant},
};

/// A shutdown doesn't wait on a receive parked on a silent
/// peer. It writes it off, and the runtime starts again after
#[test]
fn shutdown_writes_off_parked_receives() {
    Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();
    let client = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();
    let (server, _) = Runtime::block(listener.accept()).unwrap();

    let reading = Runtime::task(server.recv(16)).spawn();
    thread::sleep(Duration::from_millis(50));

    assert!(reading.is_running(), "the receive is parked");

    let started = Instant::now();
    Runtime::shutdown();
    let took = started.elapsed();

    println!("shutdown with a receive parked took {took:?}");

    assert!(
        took < Duration::from_secs(2),
        "shutdown waited {took:?} on a receive that holds no thread",
    );

    assert_eq!(reading.join(), Err(RuntimeError::TaskFailed));

    // Up again, and the same connection still works
    Runtime::init();

    Runtime::block(client.send(b"again".as_slice())).unwrap();

    let got = Runtime::task(server.recv(16))
        .spawn()
        .take_with_timeout(Duration::from_secs(10))
        .expect("a receive after the restart settles")
        .expect("a receive after the restart succeeds");

    assert_eq!(got, b"again");
}
