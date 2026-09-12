mod common;

use atap::{Runtime, Sleep};
use std::time::Duration;

/// `Runtime::workers` shows queued work while the pool is busy
/// with it
#[test]
fn backlog_is_visible_while_running() {
    Runtime::init();

    let tasks = 200_000;

    let handles: Vec<_> = (0..tasks)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(50))).spawn())
        .collect();

    // Asked while the pool is still working through them
    let mut seen_backlog = false;
    let mut seen_busy = false;

    for _ in 0..1000 {
        let stats = Runtime::workers();

        println!("{:#?}", stats);

        seen_backlog |= stats.backlog() > 0;
        seen_busy |= stats.busy() > 0;

        if seen_backlog && seen_busy {
            break;
        }
    }

    for handle in handles {
        handle.join().expect("every task finishes");
    }

    assert!(seen_backlog, "never saw any work queued across the pool");
    assert!(seen_busy, "never saw a worker inside a task");
}
