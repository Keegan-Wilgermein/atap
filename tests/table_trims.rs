mod common;

use atap::{Runtime, Sleep};
use common::report;
use std::time::Duration;

/// A trim shrinks an empty table without lowering its peak,
/// and the table still works afterwards
#[test]
fn gives_the_table_back() {
    Runtime::init();

    let passes = 8;

    // Held all at once so the table grows, then dropped so it is
    // at its largest with nothing in it
    let peak: Vec<_> = (0..200_000)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn())
        .collect();

    for handle in peak {
        handle.join().expect("every task in the peak finishes");
    }

    report("peak built and released");

    let before = Runtime::workers();

    assert!(
        before.live() <= before.peak_slots(),
        "{} live tasks in a table that has only ever handed out {} slots",
        before.live(),
        before.peak_slots(),
    );

    let mut released = 0;
    let mut done = 0;

    // A pass gives back at most a fifth, and an automatic trim
    // already under way turns one away, so a few extra attempts
    // are allowed
    for _ in 0..passes * 4 {
        if done >= passes {
            break;
        }

        if let Ok(bytes) = Runtime::trim() {
            released += bytes;
            done += 1;
        }
    }

    let after = Runtime::workers();

    println!(
        "{} passes gave back {} bytes, table {} -> {} slots (peak {} -> {}), {} live",
        done, released, before.slots(), after.slots(), before.peak_slots(), after.peak_slots(), after.live(),
    );

    report("trimmed");

    assert!(done > 0, "the table refused to give anything back");

    assert!(
        after.slots() < before.slots(),
        "the table stayed at {} slots",
        after.slots(),
    );

    // A trim must not lower the peak
    assert_eq!(
        after.peak_slots(), before.peak_slots(),
        "trimming took the peak from {} to {}",
        before.peak_slots(), after.peak_slots(),
    );

    // Never below the hundred slots it always keeps
    assert!(
        after.slots() >= 100,
        "the table trimmed itself down to {} slots",
        after.slots(),
    );

    // Straight back into the range that was just handed over
    let handles: Vec<_> = (0..200_000)
        .map(|_| Runtime::task(Sleep::sleep(Duration::from_nanos(1), true)).spawn())
        .collect();

    for handle in handles {
        handle
            .join()
            .expect("the table still works after giving pages back");
    }

    println!("200000 tasks ran through the trimmed table");
}
