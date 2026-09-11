//! Its own binary, since the manager faults it injects are
//! process wide
//!
//! #### Note
//! The panics printed as the manager unwinds are the test
//! working

use atap::{Runtime, Tcp};
use std::{thread, time::Duration};

/// A receive parked across manager restarts still wakes, both
/// for data that arrives while the manager is coming back and
/// for data that arrives after
#[test]
fn a_parked_receive_survives_manager_restarts() {
    Runtime::init();

    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).unwrap();
    let client = Runtime::block(Tcp::connect(listener.local_addr())).unwrap();
    let (server, _) = Runtime::block(listener.accept()).unwrap();

    let early = Runtime::task(server.recv_exact(5)).spawn();
    thread::sleep(Duration::from_millis(50));

    assert!(early.is_running(), "the receive is parked");

    // Under the restart limit, so it comes back every time
    Runtime::inject_manager_faults(3);

    // Lands while the manager is going down and coming back
    Runtime::block(client.send(b"early".as_slice())).unwrap();

    assert_eq!(
        early
            .take_with_timeout(Duration::from_secs(5))
            .expect("a receive parked across a restart still wakes")
            .expect("and still succeeds"),
        b"early",
    );

    // Three deaths and their backoffs, with room to spare
    thread::sleep(Duration::from_millis(400));

    let late = Runtime::task(server.recv_exact(4)).spawn();
    thread::sleep(Duration::from_millis(50));

    Runtime::block(client.send(b"late".as_slice())).unwrap();

    assert_eq!(
        late.take_with_timeout(Duration::from_secs(5))
            .expect("a receive parked after the restarts wakes")
            .expect("and succeeds"),
        b"late",
    );
}
