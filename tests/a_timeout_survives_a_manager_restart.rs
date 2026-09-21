//! # Timeout recovery
//! A manager that dies holding timeouts doesn't take the runs
//! they limit with it

use atap::{Runtime, RuntimeError, sleep::Sleep};
use std::{thread, time::Duration};

/// Every timed run still times out after the manager dies on the
/// batches carrying their timers
#[test]
fn a_timeout_survives_a_manager_restart() {
    let _ = Runtime::init();

    let runs = 48;

    // Spread over the window the faults land in
    let handles: Vec<_> = (0..runs)
        .map(|at| {
            Runtime::task(Sleep::sleep(Duration::from_secs(30)))
                .timeout(Duration::from_millis(40 + at))
                .spawn()
        })
        .collect();

    thread::sleep(Duration::from_millis(38));

    // Under the restart limit, so it comes back every time
    Runtime::inject_manager_faults(3);

    for (index, handle) in handles.iter().enumerate() {
        assert_eq!(
            handle.join_with_timeout(Duration::from_secs(10)),
            Err(RuntimeError::TimedOut),
            "run {index} outlived its timeout, so its timer went down with the manager",
        );
    }

    println!("{runs} timeouts survived the manager dying on three batches");
}
