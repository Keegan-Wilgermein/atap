# atap

**A**ny **T**ime **A**ny **P**lace: an async runtime for macOS.

There is no `async`, no `await`, no `Future` and no `Pin`. A task is a value
that does nothing until you run it, so any function can make
one. Waiting tasks hold no thread.

The crate is designed to offload compute to a dynamically growing pool of worker threads, and save the result to a lock-free table to be accessed by as many threads that hold a clone of that tasks handle.

Task data is dropped automatically after all handles have been joined or dropped.

```toml
[dependencies]
atap = "0.1"
```

## Quick start

```rust
use atap::{Runtime, RuntimeError, compute::Compute};

fn main() -> Result<(), RuntimeError> {
    Runtime::init()?;

    // Inert until it is spawned
    let doubling = Runtime::task(Compute::compute(|()| 21 * 2)).spawn();

    assert_eq!(doubling.join()?, 42);

    Ok(())
}
```

## Building a `Task`

A task is built, then run one of two ways:

```rust
use atap::{Runtime, sleep::Sleep};
use std::time::Duration;

// Runs on the calling thread
let slept = Runtime::block(Sleep::sleep(Duration::from_millis(10)));

// On the pool, which hands back a handle
let waiting = Runtime::task(Sleep::sleep(Duration::from_millis(10))).spawn();
let slept = waiting.join();
```

A handle reads its output with `join`, moves it out once with `take`, checks
without waiting with `try_join`, and ends the task with `cancel`. Cloning a
handle makes another listener, not another task.

Tasks are built with a builder:

```rust
use atap::{Runtime, compute::Compute};
use std::time::Duration;

let handle = Runtime::task(Compute::compute(|()| 1))
    .priority(200)
    .after(Duration::from_millis(50))
    .timeout(Duration::from_secs(1))
    .spawn();
```

Entering a state opens the calls that belong to it, so a repeat's settings only
exist once you have asked for a repeat:

```rust
use atap::{Runtime, compute::Compute};
use std::time::Duration;

let ticking = Runtime::task(Compute::compute(|()| 1))
    .repeat()
    .every(Duration::from_millis(100))
    .count(10)
    .spawn();
```

## The handle

`spawn` gives back a `TaskHandle`. It is a listener on the task, not the task
itself, so cloning one makes another listener rather than another task.

```rust
use atap::{Runtime, RuntimeError, compute::Compute};
use std::time::Duration;

let handle = Runtime::task(Compute::compute(|()| 42u64)).spawn();
let watching = handle.clone();

// Waits, and leaves the output where it is for the next reader
let copy = handle.join()?;

// Moves it out, so only one caller ever gets it
let owned = watching.take()?;
```

Reads come in three lengths. `try_join` and `try_take` look and return
`NotReady` if the task has not settled. `join_with_timeout` and
`take_with_timeout` give up after a while and leave the output where it was.
`join` and `take` wait for as long as it takes.

A handle also answers without reading anything: `state`, `settled`,
`is_pending`, `is_running`, `is_ready`, `is_taken`, `is_cancelled`,
`is_timed_out`, `is_failed` and `is_finished`.

`cancel` ends the task for every listener, and a read after it gives
`Cancelled`. A task already running stops at its next checkpoint, and its
output is thrown away.

Dropping a handle gives up that listener's claim, and dropping every handle
does not stop the task. The runtime keeps its own claim until the task
finishes, so fire and forget works:

```rust
use atap::{Runtime, compute::Compute};

// Runs, even with nothing left to read it
drop(Runtime::task(Compute::compute(|()| println!("done"))).spawn());
```

Handles group. `Runtime::join_all` waits for a set, `Runtime::join_first`
races one, and `receive` takes a tuple of them.

## Passing data between tasks

**Wait for a value.** The task sits in its slot until something gives it one:

```rust
use atap::{Runtime, compute::Compute};

let doubler = Runtime::task(Compute::compute(|value: u64| value * 2))
    .wait_for::<u64>()
    .spawn();

doubler.give(21)?;

assert_eq!(doubler.join()?, 42);
```

**Take another task's output.** A receive starts when the task it reads from
settles:

```rust
use atap::{Runtime, compute::Compute};

let left = Runtime::task(Compute::compute(|()| 2u64)).spawn();
let right = Runtime::task(Compute::compute(|()| 3u64)).spawn();

let summed = Runtime::task(Compute::compute(|(a, b): (u64, u64)| a + b))
    .receive((left, right))
    .count(1)
    .spawn();

assert_eq!(summed.join()?, 5);
```

**Hand an output onward.** A task can be pointed at whoever needs its answer:

```rust
use atap::{Runtime, compute::Compute};

let printing = Runtime::task(Compute::compute(|line: String| line.len()))
    .wait_for::<String>()
    .spawn();

let reading = Runtime::task(Compute::compute(|()| String::from("hello")))
    .give_to(&printing)
    .spawn();
```

**Race several and keep the first.** The losers are cancelled, dropped or
handed back:

```rust
use atap::{JoinPolicy, Runtime, sleep::Sleep};
use std::time::Duration;

let racers: Vec<_> = [5, 50, 500]
    .iter()
    .map(|ms| Runtime::task(Sleep::sleep(Duration::from_millis(*ms))).spawn())
    .collect();

let first = Runtime::join_first(racers, JoinPolicy::Cancel);
```

## Every kind of task

### Compute

Runs a closure on a worker. Its input is whatever the builder wired to it, and
`()` when nothing did.

```rust
use atap::{Runtime, compute::Compute};

let sum = Runtime::task(Compute::compute(|()| (0..1_000u64).sum::<u64>())).spawn();

assert_eq!(sum.join()?, 499_500);
```

### Sleep

Waits for a duration or until a moment. Both hand back how long they slept.

```rust
use atap::{Runtime, sleep::{Sleep, SleepMode}};
use std::time::{Duration, Instant};

// Precise by default, which spins the last stretch for accuracy
let slept = Runtime::block(Sleep::sleep(Duration::from_micros(500)));

// Relaxed leaves the whole wait to the kernel
let rested = Runtime::block(Sleep::sleep(Duration::from_millis(50)).mode(SleepMode::Relaxed));

// Or a deadline rather than a length
let until = Runtime::block(Sleep::until(Instant::now() + Duration::from_millis(10)));
```

### Files

Read, write and stat by path, or hold one open for many reads and writes.

```rust
use atap::{Runtime, fs::File};

Runtime::block(File::write("notes.txt", b"first".as_slice()))?;
Runtime::block(File::append("notes.txt", b" and second".as_slice()))?;

let back = Runtime::block(File::read("notes.txt"))?;

assert_eq!(back, b"first and second");

// One descriptor, positional reads and writes
let held = Runtime::block(File::open("notes.txt").write(true))?;

let middle = Runtime::block(held.read_at(6, 3))?;

assert_eq!(middle, b"and");
```

Listings carry what each entry is:

```rust
use atap::{Runtime, fs::{File, FileKind}};

for entry in Runtime::block(File::read_dir("."))? {
    if entry.kind() == FileKind::File {
        println!("{}", entry.path().display());
    }
}
```

### Watches

Parks on a path and settles on the first change, holding no thread.

```rust
use atap::{Runtime, fs::File};

let watching = Runtime::task(File::watch("notes.txt")).spawn();

Runtime::block(File::write("notes.txt", b"changed".as_slice()))?;

let change = watching.join()??;

assert!(change.written());
```

### Processes

Run one and wait, collect what it said, or hold it open and talk to it.

```rust
use atap::{Runtime, process::Process};

// Just the exit status
let status = Runtime::block(Process::run("/usr/bin/true", Process::NO_ARGS))?;

assert!(status.success());

// Everything it wrote
let said = Runtime::block(Process::output("/bin/echo", ["hello"]))?;

assert_eq!(said.stdout(), b"hello\n".as_slice());

// Held open, written to, and waited for
let child = Runtime::block(Process::spawn("/bin/cat", Process::NO_ARGS))?;

if let Some(input) = child.stdin() {
    Runtime::block(input.send(b"ping\n".as_slice()))?;
}

let echoed = Runtime::block(child.stdout().recv_until(b"\n", 64))?;

child.close_stdin();
Runtime::block(child.wait())?;
```

### Signals

Waits for a signal and says how many arrived.

```rust
use atap::{Runtime, signal::{Signal, SignalKind}};

let waiting = Runtime::task(Signal::wait(SignalKind::User1)).spawn();

Runtime::block(Signal::send(std::process::id() as i32, SignalKind::User1))?;

let arrived = waiting.join()??;
```

### Channels

Hands values between threads and tasks, first in first out. A receive is a task
that parks; an unbounded send is a plain call.

```rust
use atap::{Runtime, channel::Channel};

let (tx, rx) = Channel::new::<u64>().open()?;

let next = Runtime::task(rx.recv()).spawn();

tx.send(7)?;

assert_eq!(next.join()??, 7);

// Bounded makes a full send wait, so it is a task too
let (tx, rx) = Channel::new::<u64>().bounded(1).open()?;

Runtime::block(tx.send(1))?;
```

### TCP

```rust
use atap::{Runtime, tcp::Tcp};

let listener = Runtime::block(Tcp::listen("127.0.0.1:0"))?;
let address = listener.local_addr();

let accepting = Runtime::task(listener.accept()).spawn();
let client = Runtime::block(Tcp::connect(address).nodelay(true))?;

let (server, _from) = accepting.join()??;

Runtime::block(client.send(b"hello".as_slice()))?;

let heard = Runtime::block(server.recv(64))?;

assert_eq!(heard, b"hello");
```

### UDP

```rust
use atap::{Runtime, udp::Udp};

let here = Runtime::block(Udp::bind("127.0.0.1:0"))?;
let there = Runtime::block(Udp::bind("127.0.0.1:0"))?;

Runtime::block(here.send_to(there.local_addr(), b"ping".as_slice()))?;

let (bytes, from) = Runtime::block(there.recv_from())?;

assert_eq!(bytes, b"ping");
```

### Unix sockets

```rust
use atap::{Runtime, unix::Unix};

// A pair, with no path in the filesystem
let (near, far) = Runtime::block(Unix::pair())?;

Runtime::block(near.send(b"over here".as_slice()))?;

let heard = Runtime::block(far.recv(64))?;

assert_eq!(heard, b"over here");
```

### TLS

Behind the `tls` feature, on top of TCP.

```toml
atap = { version = "0.1", features = ["tls"] }
```

```rust
use atap::{Runtime, tls::Tls};

let connection = Runtime::block(
    Tls::connect("example.com:443").alpn(["http/1.1"]),
)?;

Runtime::block(connection.send(b"GET / HTTP/1.0\r\n\r\n".as_slice()))?;

let answer = Runtime::block(connection.recv(4096))?;
```

## Tuning the runtime

`Runtime::init` uses the defaults. The builder changes what the pool starts
with, and a re-init after a shutdown can change them again.

```rust
use atap::Runtime;

Runtime::builder()
    .workers_per_core(2)
    .sleep_threads_per_core(4)
    .worker_stack(2 * 1024 * 1024)
    .init()?;
```

## How work is spread

Where a task lands depends on who spawned it.

A spawn from outside the pool goes on the shared queue, and one parked worker
is woken for it. A spawn from inside a running task goes to that worker's own
LIFO slot, its ring if the slot is full, and the shared queue if both are, so
work a task makes stays near the thread that made it.

A worker looking for work checks, in order: its LIFO slot, its own ring, the
shared queue, its LIFO slot once more, and then its peers' rings. It only takes
the LIFO slot three times in a row before giving the queue a turn, so a task
that keeps spawning cannot starve everything else.

Taking from the shared queue also tops up the ring with a share of what is
waiting, which is the queue's depth divided by the live workers. Tasks carry a
priority band, and a task that has been overtaken for too long is moved up a
band so a busy queue cannot bury it.

Tasks that block are not given to workers at all. Each task says whether it
blocks, and those go to a second queue that sleep threads serve, so a file read
or a `Relaxed` sleep never ties up a worker.

## How the pool grows and shrinks

The pool settles around a target rather than holding a fixed size. The manager
wakes every 10 milliseconds and does the arithmetic.

- **Floor**: one worker per core. The pool never goes below it.
- **Target**: four workers per core, and eight sleep threads per core, both
  settable with `Runtime::builder`.
- **Ceiling**: what the kernel will give the process, less a reserve for the
  threads that are not the pool's.

Below the target, a worker is started as soon as there is work waiting for one.
Past the target it takes more: every worker has to be stuck inside a task, and
there has to be work with nowhere to go, meaning a queue at least sixty four
deep per worker, a queue that has been waiting too long, or tasks stranded in a
stuck worker's ring. Each thread already past the target makes the next one
wait a tick longer, so a burst grows the pool quickly and a steady overload
grows it slowly.

Shrinking is the same rule backwards. A parked worker with an empty ring that
stays idle for half a second is stopped, and one past the target goes after
fifty milliseconds. The floor is never broken.

Before any of that, the manager wakes parked workers for queued work, since a
thread that already exists is cheaper than a new one.

## Failure recovery

The runtime is able to recover itself if something happens.

**A task.** A task held by a thread that dies is not lost quietly. Whatever was
queued on that thread goes back to the shared queue to be run by somebody else,
and the one task it was actually running settles as failed, so a listener reads
`TaskFailed` rather than waiting for an answer that is never coming. Tasks that
can never run again settle with a reason instead of waiting: a spawn before
`init` reads `NotInitialised`, and a run past its deadline reads `TimedOut`.

**A worker or a sleep thread.** A thread going down marks its slot dead. The
next thread to come up empty handed claims the slot, puts its work back, and
frees it for reuse. Workers do this themselves rather than waiting to be told,
so the pool recovers even while the manager is gone. If deaths come faster than
the pool can absorb, it stops restarting them.

**The manager.** It runs under a supervisor that restarts it when it falls
over, waiting longer each time, and gives up after five failures in five
seconds by shutting the runtime down. A restart builds a new kqueue, which
means every deadline and every parked task registered on the old one is
re-armed against the new one. The reactor is supervised the same way.

Blocking calls keep working throughout, since `Runtime::block` runs on the
calling thread, not the runtime.

What the runtime is doing is readable:

```rust
use atap::Runtime;

let stats = Runtime::pool();

println!("{} workers, {} queued", stats.len(), stats.queued());

assert!(Runtime::healthy());
```

## What this is not

macOS only. It uses kqueue, `EV_UDATA_SPECIFIC`, `SO_NOSIGPIPE` and Mach thread
calls, and a build anywhere else stops at a `compile_error!`. Platform support may or may not change in the future.

It does not speak `Future`, so the async ecosystem does not compose with it.
If you need hyper, tonic or sqlx, you need a runtime those crates were written
for.

This crate is for programs that need to pass a lot of data between many threads asynchronously.

## Performance

Measured on one 8 core machine in release, by `tests/benchmark.rs`:

```
cargo test --release --features tls --test benchmark -- --ignored --nocapture
```

`blocked` is one task run on the calling thread with `Runtime::block`. The
rest are spawned and joined, so the difference between the two is the task scheduling. Time is from the first call to the last join, cpu is the share of one
core the row used, and memory is the peak the process reached while it ran.

| Task | blocked | 1 spawned | 1,000 | 100,000 |
|---|---|---|---|---|
| compute | 1 ns | 19.5 us | 2.09 us each | 1.08 us each, 605% cpu, 27.9 MiB |
| channel receive | 978 ns | 74.8 us | 10.2 us each | 1.56 us each, 684% cpu, 31.4 MiB |
| unix send | 697 ns | 324 us | 1.60 us each | buffer bound |
| tcp send | 1.99 us | 147 us | 3.05 us each | buffer bound |
| udp send | 4.36 us | 78.1 us | 8.89 us each | 9.04 us each, 429% cpu, 45.5 MiB |
| file read of 4KiB | 25.5 us | 205 us | 13.7 us each | 12.1 us each, 653% cpu, 124 MiB |
| sleep of 1ms | 1.00 ms | 1.10 ms | 16.2 us each | 13.7 us each, 471% cpu, 36.7 MiB |
| sleep of 400us | 400 us | 427 us | 53.8 us each | 50.9 us each, 678% cpu, 32.3 MiB |
| tcp connect | 88.2 us | 418 us | 90.8 us each at 100 | backlog bound |
| tls connect | - | 12.8 ms | 687 us each at 100 | backlog bound |
| process run | 1.15 ms | 4.26 ms | 400 us each at 100 | each one is a program |

Blocking a compute costs a nanosecond, since nothing but the closure is left
to run. Everything else blocked is the syscall and no more. The spawned column
is one task against a pool that has just gone quiet, so its time includes a worker
being woken from sleep and being assigned a task.<br>

The two sleep times compare `SleepMode` costs: 400 microseconds blocked burns a whole
core, since anything under the tolerance is spun for its entirety if in `p_mode`.

A channel costs 6.5 nanoseconds to send and 5.4 to receive while values are
waiting, since the socket behind it is only rung when the queue goes empty or
stops being empty. A receive that has to park and be woken costs 5.3
microseconds.

The runtime can handle tens of millions of concurrent tasks (tested up to 25 million), without losing or confusing a single result.

## License

MIT.
