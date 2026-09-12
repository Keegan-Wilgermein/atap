mod common;

use atap::{Runtime, Sleep};
use common::max_rss;
use std::time::Duration;

/// A million live tasks fit in the memory their slots should cost
#[test]
fn many_concurrent_tasks() {
    Runtime::init();

    let tasks = 1_000_000;
    let mut handle_list = Vec::with_capacity(tasks);

    let mut avg = 0.0;

    let baseline = max_rss();

    for _ in 0..tasks {
        let handle = Runtime::task(Sleep::sleep(Duration::from_nanos(500))).spawn();

        handle_list.push(handle);
    }

    // Read while every task is still live and holding its slot
    let peak = max_rss();
    let slots = Runtime::workers().peak_slots();

    for handle in handle_list {
        if let Ok(time) = handle.join() {
            avg += time.as_nanos() as f32;
        }
    }

    avg /= tasks as f32;

    let growth = peak.saturating_sub(baseline);

    println!("Average time: {}", avg);
    println!(
        "{} live tasks cost {} bytes, {} each",
        tasks,
        growth,
        growth / tasks,
    );
    println!("table handed out {} slots", slots);

    assert!(
        slots >= tasks,
        "{} live handles but the table only handed out {} slots, \
         so slots are being recycled while handles still hold them",
        tasks,
        slots,
    );

    // A slot owning its own page would be roughly 16GB here
    assert!(
        growth < 512 * 1024 * 1024,
        "{} live tasks took {} bytes, which is page per slot territory",
        tasks,
        growth,
    );
}
