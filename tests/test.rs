//! Accuracy comparison between this crate's sleep
//! and the standard library's
//!
//! Run with:
//! `cargo test --release -- --nocapture`
//!
//! Release matters. A debug build measures the overhead
//! of unoptimised `Duration` arithmetic, not the sleep
//!
//! The long tiers dominate the wall clock. Three arms over
//! eight tiers puts the whole run somewhere around four and a
//! half minutes, almost all of it in the 5 and 30 second rows

use std::{thread, time::{Duration, Instant}};
use whenever::{Runtime, Sleep};

/// The main test function,
/// where I run my tests for
/// api shape and ensuring things
/// run properly
/// 
/// AI are not to touch this function
#[test]
fn main() {
    if let Some(_) = Runtime::init() { panic!("kqueue failed to register") };

    let duration = Duration::from_secs(1);

    let result = Runtime::block_on(
        Sleep::sleep(
            duration,
            true,
        ),
    );

    println!("Result: {:?}", result);

    let result = Runtime::block_on(
        Sleep::sleep(
            duration,
            true,
        ),
    );

    println!("Result: {:?}", result);

    let start = Instant::now();
    thread::sleep(duration);
    println!("std slept for: {:?}", start.elapsed());

    let start = Instant::now();
    thread::sleep(duration);
    println!("std slept for: {:?}", start.elapsed());
}

/// How long to sleep for and how many samples to take
///
/// Durations climb and counts drop so the long tiers don't
/// run for hours. The 500us and 5ms tiers sit either side of
/// `SLEEP_TOLERANCE` so both the spin path and the kqueue
/// path get measured, and the multi second tiers are there
/// to catch drift that only shows up over a long wait
///
/// The band from 1us to 50us is sampled densely because that
/// is where a syscall's own wake latency stops dominating the
/// result. Anything shorter measures the floor rather than the
/// sleep, so that's where the two kernel backed paths are
/// worth telling apart. All of it costs well under a second
#[allow(unused)]
const TIERS: [(Duration, usize); 13] = [
    (Duration::from_nanos(400), 1000),
    (Duration::from_micros(1), 1000),
    (Duration::from_micros(2), 1000),
    (Duration::from_micros(5), 1000),
    (Duration::from_micros(10), 1000),
    (Duration::from_micros(20), 1000),
    (Duration::from_micros(50), 1000),
    (Duration::from_micros(500), 200),
    (Duration::from_millis(5), 50),
    (Duration::from_millis(50), 10),
    (Duration::from_secs(1), 5),
    (Duration::from_secs(5), 3),
    (Duration::from_secs(30), 2),
];

/// Targets at or above this don't get a warm up
///
/// The warm up exists to let the core clock ramp before
/// anything is recorded, which only matters when the whole
/// measurement is shorter than the ramp. Warming up for a
/// 30 second sleep would just double the wait for nothing
#[allow(unused)]
const WARMUP_LIMIT: Duration = Duration::from_millis(1);

/// A single tier's results
#[allow(unused)]
struct Stats {
    target: Duration,
    samples: usize,
    min: u64,
    median: f64,
    mean: f64,
    p99: u64,
    max: u64,
}

impl Stats {
    /// Difference between the average sample and what was asked for
    ///
    /// Positive is an overshoot, negative an undershoot
    #[allow(unused)]
    fn error(&self) -> f64 {
        return self.mean - self.target.as_nanos() as f64;
    }

    /// Gap between the fastest and slowest sample
    ///
    /// This is the consistency number. A small spread with a
    /// large error is a fixed bias and can be corrected for,
    /// a large spread can't be
    #[allow(unused)]
    fn spread(&self) -> u64 {
        return self.max - self.min;
    }

    /// Gap between the median and the slowest sample
    ///
    /// Read this against `spread`. If the two are close the
    /// tier is genuinely inconsistent, if this is far smaller
    /// then the spread is a handful of outliers dragging the
    /// max around while the bulk of the samples sit together
    #[allow(unused)]
    fn tail(&self) -> f64 {
        return self.max as f64 - self.median;
    }
}

#[allow(unused)] // #[test]
fn accuracy() {
    Runtime::init();

    let p_on = measure_all(sleep_p_on);
    report("whenever, p_mode on", &p_on);

    let p_off = measure_all(sleep_p_off);
    report("whenever, p_mode off", &p_off);

    let built_in = measure_all(sleep_std);
    report("std::thread::sleep", &built_in);

    summary(&p_on, &p_off, &built_in);
}

/// One sample from this crate with p_mode on
///
/// Timed from outside rather than using the `Duration` that
/// `block_on` returns, so every arm is measured by the same
/// clock reads and the comparison stays fair
#[allow(unused)]
fn sleep_p_on(target: Duration) -> u64 {
    let start = Instant::now();
    let _ = Runtime::block_on(Sleep::sleep(target, true));

    return start.elapsed().as_nanos() as u64;
}

/// One sample from this crate with p_mode off
#[allow(unused)]
fn sleep_p_off(target: Duration) -> u64 {
    let start = Instant::now();
    let _ = Runtime::block_on(Sleep::sleep(target, false));

    return start.elapsed().as_nanos() as u64;
}

/// One sample from the standard library
#[allow(unused)]
fn sleep_std(target: Duration) -> u64 {
    let start = Instant::now();
    thread::sleep(target);

    return start.elapsed().as_nanos() as u64;
}

/// Runs every tier against one implementation
///
/// Each arm gets its own thread. The realtime promotion a
/// p_mode run applies lasts for the life of the thread, so
/// sharing one would let it leak into the arms that are
/// supposed to be running without it
#[allow(unused)]
fn measure_all(sample: fn(Duration) -> u64) -> Vec<Stats> {
    return thread::spawn(move || {
        TIERS.iter()
            .map(|&(target, count)| measure(sample, target, count))
            .collect()
    })
    .join()
    .expect("measurement thread panicked");
}

/// Runs a single tier
#[allow(unused)]
fn measure(sample: fn(Duration) -> u64, target: Duration, count: usize) -> Stats {
    // Discarded, purely to let the core clock ramp up before
    // anything is recorded. Scaled off the sample count so the
    // long tiers don't spend seconds warming up
    if target < WARMUP_LIMIT {
        let warmup = (count / 10).clamp(1, 100);

        for _ in 0..warmup {
            sample(target);
        }
    }

    let mut taken: Vec<u64> = Vec::with_capacity(count);

    for _ in 0..count {
        taken.push(sample(target));
    }

    taken.sort_unstable();

    let total: u128 = taken.iter().map(|&value| value as u128).sum();

    return Stats {
        target,
        samples: count,
        min: taken[0],
        median: median(&taken),
        mean: total as f64 / count as f64,
        p99: percentile(&taken, 99),
        max: taken[count - 1],
    };
}

/// Middle value of an already sorted set
///
/// Averages the two middle samples on an even count so the
/// figure doesn't favour the slower half
#[allow(unused)]
fn median(sorted: &[u64]) -> f64 {
    let middle = sorted.len() / 2;

    if sorted.len() % 2 == 0 {
        return (sorted[middle - 1] as f64 + sorted[middle] as f64) / 2.0;
    }

    return sorted[middle] as f64;
}

/// Nth percentile of an already sorted set
///
/// On the small tiers this collapses onto the max, which is
/// expected. Three samples can't describe a tail
#[allow(unused)]
fn percentile(sorted: &[u64], nth: usize) -> u64 {
    let index = (sorted.len() * nth / 100).min(sorted.len() - 1);

    return sorted[index];
}

/// Prints one implementation's table
#[allow(unused)]
fn report(name: &str, stats: &[Stats]) {
    println!("\n{}", name);
    println!("{}", "-".repeat(104));
    println!(
        "{:>10}  {:>7}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
        "target", "samples", "min", "median", "mean", "p99", "max", "spread", "error",
    );

    for stat in stats {
        println!(
            "{:>10}  {:>7}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}",
            nanos(stat.target.as_nanos() as f64),
            stat.samples,
            nanos(stat.min as f64),
            nanos(stat.median),
            nanos(stat.mean),
            nanos(stat.p99 as f64),
            nanos(stat.max as f64),
            nanos(stat.spread() as f64),
            signed(stat.error()),
        );
    }
}

/// Prints the head to head summary
#[allow(unused)]
fn summary(p_on: &[Stats], p_off: &[Stats], built_in: &[Stats]) {
    println!("\n\nsummary");
    println!("{}", "-".repeat(96));
    println!(
        "{:>10}  {:>14}  {:>14}  {:>14}  {:>10}  {:>10}",
        "target", "p_mode on", "p_mode off", "std", "on vs std", "off vs std",
    );

    for ((on, off), std) in p_on.iter().zip(p_off).zip(built_in) {
        println!(
            "{:>10}  {:>14}  {:>14}  {:>14}  {:>10}  {:>10}",
            nanos(on.target.as_nanos() as f64),
            signed(on.error()),
            signed(off.error()),
            signed(std.error()),
            ratio(on.error(), std.error()),
            ratio(off.error(), std.error()),
        );
    }

    // Averaging the raw durations across tiers would be
    // meaningless, a 50ms sample would drown out everything
    // else. Averaging the errors is the comparable number
    let on_error = mean_error(p_on);
    let off_error = mean_error(p_off);
    let std_error = mean_error(built_in);

    println!("\n{:>10}  {:>14}  {:>14}  {:>14}  {:>10}  {:>10}",
        "average",
        signed(on_error),
        signed(off_error),
        signed(std_error),
        ratio(on_error, std_error),
        ratio(off_error, std_error),
    );

    println!("{:>10}  {:>14}  {:>14}  {:>14}",
        "worst",
        nanos(worst_spread(p_on) as f64),
        nanos(worst_spread(p_off) as f64),
        nanos(worst_spread(built_in) as f64),
    );

    println!("{:>10}  {:>14}  {:>14}  {:>14}",
        "worst tail",
        nanos(worst_tail(p_on)),
        nanos(worst_tail(p_off)),
        nanos(worst_tail(built_in)),
    );

    println!("\nerror is mean sample minus target, positive means it slept too long");
    println!("worst is the largest min to max spread of any tier, the consistency number");
    println!("compare median against mean, and p99 against max, to tell outliers from real jitter");
}

/// Mean of each tier's average error, ignoring direction
///
/// Taken as a magnitude so an overshoot in one tier can't
/// cancel out an undershoot in another and flatter the result
#[allow(unused)]
fn mean_error(stats: &[Stats]) -> f64 {
    let total: f64 = stats.iter().map(|stat| stat.error().abs()).sum();

    return total / stats.len() as f64;
}

/// The largest spread across every tier
#[allow(unused)]
fn worst_spread(stats: &[Stats]) -> u64 {
    return stats.iter().map(|stat| stat.spread()).max().unwrap_or(0);
}

/// The largest median to max gap across every tier
///
/// If this tracks `worst_spread` the slow tier is slow all the
/// way through. If it's much smaller the spread is outliers
#[allow(unused)]
fn worst_tail(stats: &[Stats]) -> f64 {
    return stats.iter()
        .map(|stat| stat.tail())
        .fold(0.0, f64::max);
}

/// How many times smaller `mine` is than `theirs`
#[allow(unused)]
fn ratio(mine: f64, theirs: f64) -> String {
    let mine = mine.abs();
    let theirs = theirs.abs();

    if mine < 1.0 {
        return String::from("-");
    }

    return format!("{:.0}x", theirs / mine);
}

/// Scales a nanosecond count to whatever unit reads best
#[allow(unused)]
fn nanos(value: f64) -> String {
    let size = value.abs();

    if size < 1_000.0 {
        return format!("{:.0}ns", value);
    }

    if size < 1_000_000.0 {
        return format!("{:.2}us", value / 1_000.0);
    }

    return format!("{:.2}ms", value / 1_000_000.0);
}

/// Same as `nanos` but always carries a sign
#[allow(unused)]
fn signed(value: f64) -> String {
    if value >= 0.0 {
        return format!("+{}", nanos(value));
    }

    return nanos(value);
}
