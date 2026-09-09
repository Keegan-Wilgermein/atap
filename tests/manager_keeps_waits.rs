//! # Manager wake recovery
//! A manager that dies holding wakes doesn't take the tasks
//! waiting on them with it
//!
//! A file of its own, and not only because killing the manager
//! is process-wide. The supervisor counts deaths inside a
//! window and gives up once there have been enough, so a file
//! that kills it in two separate tests is a file where the
//! second test decides the runtime is finished

use atap::{Runtime, RuntimeError, Sleep, TaskHandle};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Waits survive the manager dying with them in its hands
///
/// ## The failure this is for
/// A wake is gone from the kernel the moment `kevent` returns
/// it. A manager that comes apart between that syscall and
/// putting the task back on the pool has taken the only copy,
/// and the task it belonged to stops where it stands — holding
/// a slot, with a handle that settles for nobody
///
/// ## Why there are so many of them
/// The fault is injected where the loss happens, but which
/// wakes are in the batch it dies holding is the kernel's
/// business rather than this test's. Enough waits on a short
/// enough interval and a batch is overwhelmingly likely to have
/// at least one in it — and the assertion is that *every* one
/// of them carries on, so a single dropped wake fails it
#[test]
fn a_manager_dying_on_a_batch_loses_no_waits() {
    Runtime::init();

    let waits = 32;
    let interval = Duration::from_millis(5);

    let handles: Vec<_> = (0..waits)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).repeat().every(interval).spawn())
        .collect();

    // Every one of them going before anything is done to the
    // manager, so a stall afterwards is the manager and not a
    // series that never started
    for handle in &handles {
        take_a_run(handle, 0);
    }

    // Well under the restart limit, so the supervisor is
    // expected to bring it back every time. Three batches taken
    // down, out of a few hundred that go past in that window
    Runtime::inject_manager_faults(3);

    // The deaths, their backoffs, and room for the recovered
    // waits to come round again
    thread::sleep(Duration::from_millis(500));

    // The whole test. A wait whose wake went down with the
    // manager never comes back on its own, so a series that is
    // still producing runs is one whose wait was put back
    for (index, handle) in handles.iter().enumerate() {
        take_a_run(handle, index);
    }

    println!("{waits} waits survived the manager dying on three batches");

    for handle in handles {
        handle.cancel();
    }
}

/// Waits for the next run of a repeating task and takes it
fn take_a_run(handle: &TaskHandle<Duration>, index: usize) -> Duration {
    let waited = Instant::now();

    loop {
        match handle.clone().take() {
            Ok(slept) => return slept,

            // The last run's output has been taken and the next
            // one hasn't landed yet
            Err(RuntimeError::AlreadyTaken) => {}

            Err(error) => panic!("wait {index} came back with {error:?}"),
        }

        assert!(
            waited.elapsed() < Duration::from_secs(5),
            "wait {index} stopped producing runs, so its wake went down with the manager \
             and nothing put it back",
        );

        thread::sleep(Duration::from_millis(1));
    }
}
