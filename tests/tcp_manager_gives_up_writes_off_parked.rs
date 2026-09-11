//! Its own binary, since the manager faults it injects are
//! process wide
//!
//! #### Note
//! The panics printed as the manager unwinds are the test
//! working

use atap::{Runtime, RuntimeError, Tcp};
use std::{thread, time::Duration};

/// A manager that gives up for good writes off every receive
/// parked on its queue, since nothing is left to wake them, and
/// gives their slots back
#[test]
fn a_manager_that_gives_up_writes_off_parked_receives() {
    Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();
    let client = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();
    let (server, _) = Runtime::block(listener.accept()).unwrap();

    let before = Runtime::workers().live();

    let reading = Runtime::task(server.recv(16)).spawn();
    thread::sleep(Duration::from_millis(50));

    assert!(reading.is_running(), "the receive is parked");

    // More than the restart limit, so the supervisor gives up
    Runtime::inject_manager_faults(16);
    thread::sleep(Duration::from_secs(1));

    assert_eq!(
        reading.join_with_timeout(Duration::from_secs(5)),
        Err(RuntimeError::TaskFailed),
        "a receive parked on a queue that closed is written off",
    );

    drop(reading);

    assert!(
        Runtime::workers().live() <= before,
        "the written off receive still holds its slot",
    );

    // A blocking call needs no manager, so the connection is still
    // usable from one
    Runtime::block(client.send(b"still".as_slice())).unwrap();
    assert_eq!(Runtime::block(server.recv(16)).unwrap(), b"still");
}
