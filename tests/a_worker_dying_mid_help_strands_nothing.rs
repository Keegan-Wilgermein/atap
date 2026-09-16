//! # A worker dying while it helps
//! Workers go down part way through running tasks inside tasks, and
//! nothing is left waiting on what they were holding
//!
//! Kills pool threads, which every other test in the process would
//! feel, so it has a binary to itself
//!
//! #### Note
//! Every injected death prints as it unwinds. That is the test
//! working

mod common;

use atap::{Compute, Runtime, RuntimeError};
use common::{cores, report, settles};
use std::{
    thread,
    time::{Duration, Instant},
};

/// Longer than any root here takes, so a root still unsettled at the
/// end of it was stranded
const PATIENCE: Duration = Duration::from_secs(30);

/// A tree of computes `depth` levels deep, each joining both halves
/// below it
///
/// A half that went down with a thread comes back as an error rather
/// than a panic, so the tree reports it all the way up
fn split(depth: u32) -> Result<u64, RuntimeError> {
    if depth == 0 {
        // Long enough that a death lands with plenty of leaves running
        thread::sleep(Duration::from_micros(200));

        return Ok(1);
    }

    let left = Runtime::task(Compute::compute(move |()| split(depth - 1))).spawn();
    let right = Runtime::task(Compute::compute(move |()| split(depth - 1))).spawn();

    let left = left.join().and_then(|inner| inner);
    let right = right.join().and_then(|inner| inner);

    Ok(left? + right?)
}

/// Every root settles with its full answer or an error, however many
/// workers die under it, and the pool comes back afterwards
#[test]
fn a_worker_dying_mid_help_strands_nothing() {
    let _ = Runtime::init();

    let depth = 9;
    let whole = 1u64 << depth;

    let before = Runtime::workers();
    let mut failed_roots = 0;
    let mut injected = 0usize;

    report("before");

    for round in 0..4 {
        let roots_count = cores().max(4);

        let roots: Vec<_> = (0..roots_count)
            .map(|_| Runtime::task(Compute::compute(move |()| split(depth))).spawn())
            .collect();

        // Well under way, with workers deep inside helping
        thread::sleep(Duration::from_millis(15));

        // Half the workers in the early rounds, and every one of them in
        // the later ones
        let running = Runtime::workers().len();
        let deaths = match round < 2 {
            true => (running / 2).max(1),
            false => running,
        };

        injected += deaths;

        Runtime::inject_thread_deaths(deaths as u32, 0);

        let waited = Instant::now();
        let mut whole_roots = 0;
        let mut round_failed = 0;

        for root in roots {
            let left = PATIENCE.saturating_sub(waited.elapsed());

            match root.join_with_timeout(left) {
                Ok(Ok(leaves)) => {
                    assert_eq!(
                        leaves, whole,
                        "a root that didn't fail came back with {} leaves of {}",
                        leaves, whole,
                    );

                    whole_roots += 1;
                }

                // Something under it went down with a thread
                Ok(Err(RuntimeError::TaskFailed)) | Err(RuntimeError::TaskFailed) => {
                    round_failed += 1;
                }

                Err(RuntimeError::NotReady) => panic!(
                    "a root was still waiting {:?} after {} workers died under it, pool {}",
                    PATIENCE,
                    deaths,
                    Runtime::workers(),
                ),

                other => panic!("a root came back with {:?}", other.map(|inner| inner.ok())),
            }
        }

        failed_roots += round_failed;

        // Deaths still owed would land on the next round's workers
        Runtime::inject_thread_deaths(0, 0);

        assert!(
            settles(|| Runtime::workers().recovering() == 0),
            "{} dead threads were never recovered",
            Runtime::workers().recovering(),
        );

        println!(
            "round {}: {} of {} workers killed, {} roots whole and {} failed, settled in {:?}",
            round,
            deaths,
            running,
            whole_roots,
            round_failed,
            waited.elapsed(),
        );

        report("after the round");
    }

    let after = Runtime::workers();

    println!(
        "{} deaths recorded against {} asked for, {} roots failed across every round",
        after.deaths() - before.deaths(),
        injected,
        failed_roots,
    );

    assert!(
        after.deaths() > before.deaths(),
        "no death was ever recorded"
    );

    // Every worker died in the later rounds, with every one of them
    // holding part of a tree
    assert!(
        failed_roots > 0,
        "every worker died mid tree and not one root noticed, so nothing was really killed"
    );

    // The pool it came back to still does the whole job
    let clean = Runtime::task(Compute::compute(move |()| split(depth))).spawn();

    assert_eq!(clean.join_with_timeout(PATIENCE), Ok(Ok(whole)));

    assert!(
        settles(|| Runtime::healthy()),
        "the runtime wasn't healthy after its workers died: {:?}",
        Runtime::status(),
    );

    report("finished");
}
