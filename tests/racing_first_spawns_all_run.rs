//! # Racing first spawns
//! Threads racing to start the pool's first sleep threads all get
//! their tasks run

use atap::{
    Runtime,
    sleep::{Sleep, SleepMode},
};
use std::{
    sync::{Arc, Barrier},
    thread,
    time::Duration,
};

/// Every one of many blocking tasks spawned at the same moment, into
/// a pool with no sleep threads yet, runs
#[test]
fn racing_first_spawns_all_run() {
    let _ = Runtime::init();

    let threads = 32;
    let start = Arc::new(Barrier::new(threads));

    let spawners: Vec<_> = (0..threads)
        .map(|_| {
            let start = Arc::clone(&start);

            thread::spawn(move || {
                start.wait();

                Runtime::task(Sleep::sleep(Duration::from_millis(1)).mode(SleepMode::Relaxed))
                    .spawn()
            })
        })
        .collect();

    for (index, spawner) in spawners.into_iter().enumerate() {
        let handle = spawner.join().expect("every spawner finishes");

        assert!(
            handle.join().is_ok(),
            "spawn {index} was refused while other threads were starting the pool"
        );
    }
}
