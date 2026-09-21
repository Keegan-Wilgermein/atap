//! # Benchmark
//! What each kind of task costs as more of them run at once: the
//! cpu it takes, the memory the process peaks at while they run,
//! and how long from the first spawn to the last join
//!
//! Every number is process wide, so it all runs in one test, in
//! order, and the binary holds nothing else:
//!
//! `cargo test --release --features tls --test benchmark -- --ignored --nocapture`

mod common;

use atap::{
    Runtime, Task, TaskHandle,
    builder::{HandleKind, Standalone},
    channel::Channel,
    compute::Compute,
    fs::File,
    process::Process,
    signal::{Signal, SignalKind},
    sleep::Sleep,
    tcp::Tcp,
    udp::Udp,
    unix::Unix,
};
use common::{
    Resources, TestPath, cpu_time, footprint, mebibytes, raise_descriptor_limit, send_signal,
};
use std::{
    hint::black_box,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Tasks each row runs at once
const COUNTS: [usize; 6] = [0, 1, 100, 1_000, 10_000, 100_000];

/// How often the memory is read while a row runs
const SAMPLE: Duration = Duration::from_millis(5);

/// How long the process is given to settle before a row is
/// measured
const SETTLE: Duration = Duration::from_millis(400);

/// How long a row waits for a task that ought to have settled
const PATIENCE: Duration = Duration::from_secs(60);

/// Every table, in order
#[test]
#[ignore]
fn benchmark() {
    Runtime::init().expect("the runtime starts");

    println!("\natap benchmark");
    println!("  {}", Resources::now());
    println!(
        "  {} cores, {} descriptors",
        common::cores(),
        raise_descriptor_limit(),
    );
    println!("  cpu is the share of one core the row used, so 800 is every core busy");
    println!("  memory is the peak the process reached while the row ran");
    println!("  a signal or a watch row also waits for every task to be watching");
    println!("  the pool is woken before each row, so none of them start from idle");

    computes();
    sleeps();
    channels();
    files();
    signals();
    watches();
    datagrams();
    pipes();
    connections();
    streams();
    tls();
    children();

    println!("\n  {}", Resources::now());
}

/// Closures run on a worker
fn computes() {
    table(
        "Compute",
        100_000,
        "",
        20_000,
        || {
            black_box(blocked(Compute::compute(|()| black_box(1u64))));
        },
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|value| Runtime::task(Compute::compute(move |()| value * 2)).spawn())
            .collect();

        for handle in handles {
            handle.join().expect("every compute finishes");
        }
    });
}

/// Waits of both kinds: one long enough to be handed to a sleep
/// thread, and one short enough to be spun on a worker
fn sleeps() {
    table(
        "Sleep 1ms",
        100_000,
        "",
        200,
        || {
            black_box(blocked(Sleep::sleep(Duration::from_millis(1))));
        },
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(Sleep::sleep(Duration::from_millis(1))).spawn())
            .collect();

        for handle in handles {
            handle.join().expect("every sleep finishes");
        }
    });

    table(
        "Sleep 400us",
        100_000,
        "",
        500,
        || {
            black_box(blocked(Sleep::sleep(Duration::from_micros(400))));
        },
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(Sleep::sleep(Duration::from_micros(400))).spawn())
            .collect();

        for handle in handles {
            handle.join().expect("every sleep finishes");
        }
    });
}

/// Receives parked on one channel, woken by a value each
fn channels() {
    let (ready, taking) = Channel::new::<u64>().open().expect("a channel opens");

    table(
        "Channel recv",
        100_000,
        "",
        20_000,
        || {
            ready.send(1).expect("the send lands");
            black_box(blocked(taking.recv())).expect("the value is there");
        },
        |count| {
        let (tx, rx) = Channel::new::<u64>().open().expect("a channel opens");

        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(rx.recv()).spawn())
            .collect();

        for value in 0..count as u64 {
            tx.send(value).expect("the send lands");
        }

        for handle in handles {
            handle
                .join()
                .expect("every receive settles")
                .expect("every receive gets a value");
        }
    });
}

/// Reads of one small file, each opening it again
fn files() {
    let path = TestPath::new("benchmark");

    Runtime::block(File::write(path.path(), vec![b'x'; 4 * 1024])).expect("the write works");

    table(
        "File read 4KiB",
        100_000,
        "",
        2_000,
        || {
            black_box(blocked(File::read(path.path()))).expect("the read works");
        },
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(File::read(path.path())).spawn())
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("every read settles")
                .expect("every read works");
        }
    });
}

/// Waits for one signal, all woken by a single delivery
fn signals() {
    const KIND: SignalKind = SignalKind::User1;

    table(
        "Signal wait",
        10_000,
        "every wait puts its own filter on the one delivery",
        0,
        || {},
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(Signal::wait(KIND)).spawn())
            .collect();

        // A wait only sees a delivery that lands once it is
        // watching, so they all have to be up first
        watching(&handles);

        for _ in 0..count.min(1) {
            send_signal(KIND);
        }

        for handle in handles {
            handle
                .join_with_timeout(PATIENCE)
                .expect("every wait settles")
                .expect("every wait sees the signal");
        }
    });
}

/// Watches of one file, all woken by a single write
fn watches() {
    let path = TestPath::new("benchmark-watch");

    Runtime::block(File::write(path.path(), b"start".as_slice())).expect("the write works");

    table(
        "File watch",
        10_000,
        "every watch puts its own filter on the one change",
        0,
        || {},
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(File::watch(path.path())).spawn())
            .collect();

        // The same: a watch only sees a change made once it is on
        watching(&handles);

        for _ in 0..count.min(1) {
            Runtime::block(File::write(path.path(), b"again".as_slice())).expect("the write works");
        }

        for handle in handles {
            handle
                .join_with_timeout(PATIENCE)
                .expect("every watch settles")
                .expect("every watch sees the change");
        }
    });
}

/// Datagrams sent from one socket
fn datagrams() {
    let socket = Runtime::block(Udp::bind("127.0.0.1:0")).expect("a socket binds");
    let to = Runtime::block(Udp::bind("127.0.0.1:0")).expect("a socket binds");
    let address = to.local_addr();

    table(
        "Udp send",
        100_000,
        "",
        20_000,
        || {
            black_box(blocked(socket.send_to(address, b"hello".as_slice()))).expect("the send works");
        },
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(socket.send_to(address, b"hello".as_slice())).spawn())
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("every send settles")
                .expect("every send works");
        }
    });
}

/// Sends down one Unix socket pair, drained at the far end
fn pipes() {
    let (near, far) = Runtime::block(Unix::pair()).expect("a socket pair opens");

    table(
        "Unix send",
        10_000,
        "the pair's buffer bounds it, not the runtime",
        // Kept well under the pair's buffer, since nothing drains
        // the far end while this runs
        200,
        || {
            black_box(blocked(near.send(b"hello".as_slice()))).expect("the send works");
        },
        |count| {
        let reading = far.clone();

        let drain = thread::spawn(move || {
            let mut left = count * 5;

            while left > 0 {
                match Runtime::block(reading.recv(64 * 1024)) {
                    Ok(bytes) if !bytes.is_empty() => {
                        left = left.saturating_sub(bytes.len())
                    }
                    _ => break,
                }
            }
        });

        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(near.send(b"hello".as_slice())).spawn())
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("every send settles")
                .expect("every send works");
        }

        drain.join().expect("the far end keeps up");
    });
}

/// Loopback TCP connections, each opened and closed
fn connections() {
    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).expect("a listener opens");
    let address = listener.local_addr();

    table(
        "Tcp connect",
        100,
        "the kernel's listen backlog is 128, and the rest are reset",
        100,
        || {
            // The backlog holds these, so no accept has to be posted
            black_box(blocked(Tcp::connect(address))).expect("the connect works");
        },
        |count| {
        let accepting: Vec<_> = (0..count)
            .map(|_| Runtime::task(listener.accept()).spawn())
            .collect();

        let connecting: Vec<_> = (0..count)
            .map(|_| Runtime::task(Tcp::connect(address)).spawn())
            .collect();

        for handle in connecting {
            handle
                .join()
                .expect("every connect settles")
                .expect("every connect works");
        }

        for handle in accepting {
            handle
                .join()
                .expect("every accept settles")
                .expect("every accept works");
        }
    });
}

/// Sends down one loopback connection, drained at the far end
fn streams() {
    let listener = Runtime::block(Tcp::listen("127.0.0.1:0")).expect("a listener opens");
    let accepting = Runtime::task(listener.accept()).spawn();
    let near = Runtime::block(Tcp::connect(listener.local_addr())).expect("the connect works");
    let (far, _) = accepting
        .join()
        .expect("the accept settles")
        .expect("the accept works");

    table(
        "Tcp send",
        10_000,
        "the socket's buffer bounds it, not the runtime",
        2_000,
        || {
            black_box(blocked(near.send(b"hello".as_slice()))).expect("the send works");
        },
        |count| {
            let reading = far.clone();

            let drain = thread::spawn(move || {
                let mut left = count * 5;

                while left > 0 {
                    match Runtime::block(reading.recv(64 * 1024)) {
                        Ok(bytes) if !bytes.is_empty() => {
                            left = left.saturating_sub(bytes.len())
                        }
                        _ => break,
                    }
                }
            });

            let handles: Vec<_> = (0..count)
                .map(|_| Runtime::task(near.send(b"hello".as_slice())).spawn())
                .collect();

            for handle in handles {
                handle
                    .join()
                    .expect("every send settles")
                    .expect("every send works");
            }

            drain.join().expect("the far end keeps up");
        },
    );
}

/// The same, with a handshake on top
#[cfg(feature = "tls")]
fn tls() {
    use atap::tls::Tls;

    let certs = common::certs("benchmark");
    let listener = Runtime::block(Tls::listen("127.0.0.1:0", &certs.cert, &certs.key))
        .expect("a TLS listener opens");
    let address = listener.local_addr();

    table(
        "Tls connect",
        100,
        "the kernel's listen backlog is 128, and the rest are reset",
        0,
        || {},
        |count| {
        let accepting: Vec<_> = (0..count)
            .map(|_| Runtime::task(listener.accept()).spawn())
            .collect();

        let connecting: Vec<_> = (0..count)
            .map(|_| {
                Runtime::task(
                    Tls::connect(address)
                        .server_name("localhost")
                        .trust(certs.ca.as_bytes()),
                )
                .spawn()
            })
            .collect();

        for handle in connecting {
            handle
                .join()
                .expect("every connect settles")
                .expect("every connect works");
        }

        for handle in accepting {
            handle
                .join()
                .expect("every accept settles")
                .expect("every accept works");
        }
    });
}

/// Nothing to measure without the feature
#[cfg(not(feature = "tls"))]
fn tls() {
    println!("\nTask: Tls connect");
    println!("  not built, run with --features tls");
}

/// Other programs run and waited for
fn children() {
    table(
        "Process run",
        100,
        "each one is a program of its own",
        100,
        || {
            black_box(blocked(Process::run("/usr/bin/true", Process::NO_ARGS))).expect("true runs");
        },
        |count| {
        let handles: Vec<_> = (0..count)
            .map(|_| Runtime::task(Process::run("/usr/bin/true", Process::NO_ARGS)).spawn())
            .collect();

        for handle in handles {
            handle
                .join()
                .expect("every run settles")
                .expect("every run works");
        }
    });
}

/// Prints one kind of task's table
///
/// `run` spawns that many tasks and waits for every one of them.
/// Counts past `most` are left out, and `why` says what stops
/// them
fn table(
    kind: &str,
    most: usize,
    why: &str,
    blocked_runs: usize,
    blocking: impl Fn(),
    run: impl Fn(usize),
) {
    println!("\nTask: {kind}");
    println!(
        "  {:>7} | {:>9} | {:>11} | {:>12} | {:>11}",
        "count", "cpu", "memory", "time", "each",
    );

    // The same work on the calling thread, with none of the
    // scheduling around it
    if blocked_runs > 0 {
        thread::sleep(SETTLE);

        blocking();

        let before = cpu_time();
        let started = Instant::now();

        for _ in 0..blocked_runs {
            blocking();
        }

        let time = started.elapsed();
        let cpu = cpu_time().saturating_sub(before);

        println!(
            "  {:>7} | {:>9} | {:>11} | {:>12} | {:>11}",
            "blocked",
            share(blocked_runs, cpu, time),
            "-",
            format!("{time:.2?}"),
            each(blocked_runs, time),
        );
    }

    for count in COUNTS {
        if count > most {
            println!(
                "  {count:>7} | {:>9} | {:>11} | {:>12} | {:>11}",
                "-", "-", "-", "-",
            );
            continue;
        }

        // Whatever the last row left behind is given time to go, so
        // the reading belongs to this one
        thread::sleep(SETTLE);

        // One task first, so the row isn't charged for waking a
        // pool the settle put to sleep
        Runtime::task(Compute::compute(|()| 0u64))
            .spawn()
            .join()
            .expect("the pool is awake");

        let watching = Watcher::start();
        let started = Instant::now();

        run(count);

        let time = started.elapsed();
        let (cpu, peak) = watching.stop();

        println!(
            "  {count:>7} | {:>9} | {:>7.1} MiB | {:>12} | {:>11}",
            share(count, cpu, time),
            mebibytes(peak),
            format!("{time:.2?}"),
            each(count, time),
        );
    }

    if most < COUNTS[COUNTS.len() - 1] {
        println!("  past {most}: {why}");
    }
}

/// Waits until every one of these tasks has started
///
/// What they all settle on is one event, so anything still queued
/// would miss it
fn watching<T, W>(handles: &[TaskHandle<T, W>])
where
    W: HandleKind,
{
    if handles.is_empty() {
        return;
    }

    while handles.iter().any(TaskHandle::is_pending) {
        thread::yield_now();
    }

    // Long enough for the last of them to reach its park
    thread::sleep(Duration::from_millis(100));
}

/// The cpu the row used as a share of one core, as text
///
/// 100 is one core busy for the whole row, so a row on eight cores
/// can read up to 800
fn share(count: usize, cpu: Duration, time: Duration) -> String {
    // A row with nothing in it, or one over in a moment, is done
    // before a share means anything
    if count == 0 || time < Duration::from_micros(100) {
        return String::from("-");
    }

    format!("{:.0} %", cpu.as_secs_f64() / time.as_secs_f64() * 100.0)
}

/// Runs one task on the calling thread
///
/// Kept out of line, since a task the compiler can fold into its
/// caller leaves nothing of the call to measure
#[inline(never)]
fn blocked<F>(task: F) -> F::Output
where
    F: Task,
    F::Input: Standalone,
{
    Runtime::block(task)
}

/// How long one task took, as text
fn each(count: usize, time: Duration) -> String {
    if count == 0 {
        return String::from("-");
    }

    let nanos = time.as_secs_f64() * 1e9 / count as f64;

    match nanos < 1000.0 {
        true => format!("{nanos:.0} ns"),
        false => format!("{:.2} us", nanos / 1000.0),
    }
}

/// Reads what the process is using while a row runs
struct Watcher {
    /// Set once the row is over
    done: Arc<AtomicBool>,

    /// The most memory seen, in bytes
    peak: Arc<AtomicU64>,

    /// The thread doing the reading
    reader: JoinHandle<()>,

    /// What the process had used when the row started
    before: Resources,
}

impl Watcher {
    /// Starts reading
    ///
    /// The thread is up and has taken its first reading before the
    /// row's own cpu is counted, so starting it isn't charged to
    /// the row
    fn start() -> Self {
        let done = Arc::new(AtomicBool::new(false));
        let peak = Arc::new(AtomicU64::new(0));
        let up = Arc::new(AtomicBool::new(false));

        let stopping = Arc::clone(&done);
        let highest = Arc::clone(&peak);
        let running = Arc::clone(&up);

        let reader = thread::spawn(move || {
            highest.fetch_max(footprint(), Ordering::Relaxed);
            running.store(true, Ordering::Release);

            while !stopping.load(Ordering::Relaxed) {
                thread::sleep(SAMPLE);

                highest.fetch_max(footprint(), Ordering::Relaxed);
            }
        });

        while !up.load(Ordering::Acquire) {
            thread::yield_now();
        }

        Self {
            done,
            peak,
            reader,
            before: Resources::now(),
        }
    }

    /// Stops reading
    ///
    /// ## Returns
    /// The cpu the row was charged, and the most memory the process
    /// held while it ran
    fn stop(self) -> (Duration, u64) {
        self.done.store(true, Ordering::Relaxed);
        self.reader.join().expect("the reader stops");

        let cpu = Resources::now().cpu.saturating_sub(self.before.cpu);

        (cpu, self.peak.load(Ordering::Relaxed))
    }
}
