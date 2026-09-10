//! Process task tests
//!
//! Only programs a stock macOS install has are used, so nothing
//! here depends on a package manager having been anywhere near
//! the machine
//!
//! Several of these would hang rather than fail if the thing
//! they check for came back — a deadlock is the failure mode
//! this whole feature is designed around. Those run as spawned
//! tasks against a deadline, so a regression reports itself
//! instead of stopping the suite forever

use atap::{Process, Runtime, RuntimeError, TaskHandle};
use std::{
    fs, thread,
    time::{Duration, Instant},
};

/// How long a test waits for a child that ought to be quick
///
/// Generous on purpose. These run alongside every other test in
/// the binary, on a sleep pool they are all sharing, so the
/// number has to leave room for queueing — it is here to turn a
/// hang into a failure, not to measure anything
const PATIENCE: Duration = Duration::from_secs(20);

/// Waits for a spawned task, but not forever
///
/// ## Returns
/// `None` if the deadline passed with nothing settled, which is
/// what a deadlock looks like from out here
fn settled<T>(handle: &TaskHandle<T>, patience: Duration) -> Option<T> {
    let deadline = Instant::now() + patience;

    while Instant::now() < deadline {
        match handle.maybe_take() {
            Ok(value) => return Some(value),
            Err(RuntimeError::NotReady) => thread::sleep(Duration::from_millis(1)),
            Err(_) => break,
        }
    }

    None
}

/// Takes the next output a repeat produces
///
/// The same helper the file tests use, and for the same reason:
/// `Taken` is terminal, so asking between runs answers at once
/// and a loop with nothing slowing it down spends its whole
/// budget before the next run is even scheduled
fn next_run<T>(handle: &TaskHandle<T>, patience: Duration) -> Option<T> {
    let deadline = Instant::now() + patience;

    while Instant::now() < deadline {
        match handle.maybe_take() {
            Ok(value) => return Some(value),
            Err(RuntimeError::AlreadyTaken) | Err(RuntimeError::NotReady) => {
                thread::sleep(Duration::from_millis(1))
            }
            Err(_) => break,
        }
    }

    None
}

/// A program that ran and failed is an answer, not an error
#[test]
fn a_run_reports_its_exit_code() {
    Runtime::init();

    println!("running /usr/bin/true");
    let ok = Runtime::block(Process::run("/usr/bin/true", Process::NO_ARGS))
        .expect("true must run");

    assert!(ok.success(), "true must succeed, got {:?}", ok);
    assert_eq!(ok.code(), Some(0), "true must exit zero");

    println!("running /usr/bin/false");
    let failed = Runtime::block(Process::run("/usr/bin/false", Process::NO_ARGS))
        .expect("false must run");

    assert!(!failed.success(), "false must not succeed");
    assert_eq!(
        failed.code(),
        Some(1),
        "false must exit one, got {:?}",
        failed.code()
    );
    assert_eq!(failed.signal(), None, "false was not killed");
}

/// Each stream comes back as itself
///
/// The failure this catches is a swapped `dup2`, which produces
/// output that looks entirely reasonable until somebody reads
/// the wrong half of it
#[test]
fn output_comes_back_on_the_right_stream() {
    Runtime::init();

    let handle = Runtime::task(Process::output(
        "/bin/sh",
        ["-c", "echo out; echo err 1>&2"],
    ))
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a two line child must settle")
        .expect("sh must run");

    assert_eq!(
        found.stdout(),
        b"out\n",
        "stdout was {:?}",
        String::from_utf8_lossy(found.stdout())
    );
    assert_eq!(
        found.stderr(),
        b"err\n",
        "stderr was {:?}",
        String::from_utf8_lossy(found.stderr())
    );
    assert!(found.status().success(), "the shell itself must succeed");
}

/// A child that fills both pipes finishes
///
/// ## Behaviour
/// The headline test. A pipe holds 64 KiB, so four megabytes
/// down each stream is sixty times more than either can hold
/// without somebody reading it
///
/// Catches every version of the deadlock at once: waiting for
/// the exit before draining, draining one stream to its end
/// before starting the other, or forgetting to close this
/// process's copies of the write ends
#[test]
fn a_child_that_floods_both_pipes_does_not_deadlock() {
    Runtime::init();

    const FLOOD: usize = 4 * 1024 * 1024;

    let script = format!(
        "head -c {FLOOD} /dev/zero & head -c {FLOOD} /dev/zero 1>&2; wait"
    );

    println!("flooding both streams with {FLOOD} bytes each");

    let handle = Runtime::task(Process::output("/bin/sh", ["-c", &script])).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a flooding child must settle rather than deadlock")
        .expect("sh must run");

    assert_eq!(found.stdout().len(), FLOOD, "stdout was truncated");
    assert_eq!(found.stderr().len(), FLOOD, "stderr was truncated");
}

/// Children spawned at once don't hold each other's pipes open
///
/// ## Behaviour
/// The second deadlock, and the quiet one. Without
/// `CLOEXEC_DEFAULT` a pipe made by one task is inherited by
/// another task's child, and the first task's drain then waits
/// for an end that somebody else's child is holding shut
///
/// Sized under the sleep pool so this measures concurrency
/// rather than queueing
#[test]
fn many_children_at_once() {
    Runtime::init();

    const CHILDREN: usize = 16;

    let handles = (0..CHILDREN)
        .map(|index| {
            Runtime::task(Process::output(
                "/bin/sh",
                ["-c", &format!("echo {index}")],
            ))
            .spawn()
        })
        .collect::<Vec<_>>();

    println!("waiting on {CHILDREN} children at once");

    for (index, handle) in handles.iter().enumerate() {
        let found = settled(handle, PATIENCE)
            .unwrap_or_else(|| panic!("child {index} never settled"))
            .unwrap_or_else(|error| panic!("child {index} could not run: {error}"));

        assert_eq!(
            found.stdout(),
            format!("{index}\n").as_bytes(),
            "child {index} came back with somebody else's output"
        );
    }
}

/// A cancelled child stops running
///
/// ## Behaviour
/// Watching the state settle would prove nothing. `cancel`
/// moves a task to `Cancelled` before the thread running it has
/// noticed, so a handle reports the cancel long before the
/// child has been signalled — a runtime that never killed
/// anything would pass that test every time
///
/// So the child says whether it is alive instead. It appends to
/// a file for as long as it is running, and the file's length
/// after the cancel is compared against its length a while
/// later. A child that outlived its task goes on writing
///
/// #### Note
/// This does *not* cover the process group. The shell drives
/// the loop itself, so killing only the direct child would stop
/// the writing just as well — the grandchild here is the
/// `sleep`, which dies on its own either way. Proving the group
/// kill needs a child that outlives its parent deliberately,
/// and there is no test for that yet
#[test]
fn a_cancelled_child_stops_running() {
    Runtime::init();

    let scratch = std::env::temp_dir().join(format!("atap-cancel-{}.txt", std::process::id()));
    let _ = fs::remove_file(&scratch);

    let script = format!(
        "while true; do echo x >> {}; sleep 0.05; done",
        scratch.display()
    );

    let handle = Runtime::task(Process::run("/bin/sh", ["-c", &script])).spawn();

    // Long enough that the child is running and writing, since a
    // cancel that lands before the spawn takes a different path
    // and isn't the one under test
    thread::sleep(Duration::from_millis(500));

    let before_cancel = fs::metadata(&scratch).map(|found| found.len()).unwrap_or(0);

    assert!(
        before_cancel > 0,
        "the child must be writing before the cancel, or this proves nothing"
    );

    println!("cancelling a child that writes while it lives");
    handle.clone().cancel();
    let _ = handle.wait();

    // Long enough for a child that survived to write many more
    // times over, so a pass isn't just a narrow window
    thread::sleep(Duration::from_millis(750));
    let settled_at = fs::metadata(&scratch).map(|found| found.len()).unwrap_or(0);

    thread::sleep(Duration::from_millis(750));
    let later = fs::metadata(&scratch).map(|found| found.len()).unwrap_or(0);

    let _ = fs::remove_file(&scratch);

    assert_eq!(
        settled_at, later,
        "the child went on writing after its task was cancelled, \
         {settled_at} bytes then {later}"
    );

    assert!(
        handle.is_cancelled(),
        "the task must settle cancelled, was {:?}",
        handle.state()
    );
}

/// A program that isn't there says so
///
/// ## Behaviour
/// Also the test that catches `posix_spawn` being checked the
/// ordinary way. It returns its errno directly rather than
/// setting one, so a `check` that looks for a negative would
/// call this a success and go on to wait for a child that was
/// never started
#[test]
fn a_program_that_is_not_there_reports_it() {
    Runtime::init();

    let found = Runtime::block(Process::run("/no/such/program", Process::NO_ARGS));

    assert_eq!(
        found,
        Err(RuntimeError::CheckError(Some(libc::ENOENT))),
        "a missing program must come back as ENOENT, got {found:?}"
    );
}

/// A child's standard input is not this process's
///
/// `cat` with no arguments reads until its input ends. Given a
/// terminal, or given nothing at all, it never finishes — so
/// this settling at all is the assertion
#[test]
fn stdin_is_not_the_terminal() {
    Runtime::init();

    let handle = Runtime::task(Process::output("/bin/cat", Process::NO_ARGS)).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("cat must reach the end of its input")
        .expect("cat must run");

    assert!(found.stdout().is_empty(), "there was nothing to read");
    assert!(found.status().success(), "cat must finish happily");
}

/// A child gets its signals back the way it expects them
///
/// ## Behaviour
/// The standard library sets `SIGPIPE` to ignored at startup,
/// and an ignored disposition survives an `exec`. A child that
/// inherited it doesn't die when its reader goes away — it gets
/// `EPIPE` and, in the case of `yes`, spins on it forever
#[test]
fn sigpipe_is_reset() {
    Runtime::init();

    let handle = Runtime::task(Process::output("/bin/sh", ["-c", "yes | head -1"])).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a closed pipe must kill the writer rather than spin")
        .expect("sh must run");

    assert_eq!(found.stdout(), b"y\n", "head takes exactly one line");
}

/// An argument the kernel can't be given is refused, and the
/// refusal says which half was wrong
#[test]
fn a_zero_byte_is_refused() {
    Runtime::init();

    let bad_argument = Runtime::block(Process::run("/bin/echo", ["a\0b"]));

    assert_eq!(
        bad_argument,
        Err(RuntimeError::BadArgument),
        "a zero byte in an argument must be refused, got {bad_argument:?}"
    );

    let bad_program = Runtime::block(Process::run("/bin/ec\0ho", ["fine"]));

    assert_eq!(
        bad_program,
        Err(RuntimeError::BadPath),
        "a zero byte in the program must be refused, got {bad_program:?}"
    );
}

/// A repeat runs the same program again
///
/// Exercises the half of the design that keeps its arguments in
/// an `Arc` and does no per run setup — a task that needed a
/// `prepare` it doesn't have would come apart on the second run
/// rather than the first
/// #### Note
/// The gap is not decoration. A slot holds the latest output
/// rather than a queue of them, and a bare `.repeat()` puts the
/// next run back on the pool the instant the last one
/// published — so three runs of `echo` finish in less time than
/// a reader takes to look, and the outputs overwrite each other
/// before anybody sees them. Spacing the runs is what makes
/// each one observable, which is why the file tests do the same
#[test]
fn a_repeat_runs_the_same_program_again() {
    Runtime::init();

    let handle = Runtime::task(Process::output("/bin/echo", ["again"]))
        .repeat()
        .every(Duration::from_millis(30))
        .count(3)
        .spawn();

    let mut runs = 0;

    while let Some(found) = next_run(&handle, PATIENCE) {
        assert_eq!(
            found.expect("echo must run").stdout(),
            b"again\n",
            "run {runs} came back with the wrong output"
        );

        runs += 1;
    }

    println!("saw {runs} runs against a count of 3");

    assert!(handle.is_finished(), "the series never reported finishing");
    assert_eq!(runs, 3, "saw {runs} runs, not 3");
}

/// A run child keeps this process's own output
///
/// ## Behaviour
/// The canary for the one part of the spawn that rests on
/// behaviour outside POSIX. `CLOEXEC_DEFAULT` closes every
/// descriptor the child wasn't explicitly given, and the way an
/// fd is exempted from it is to duplicate it onto itself —
/// which Apple's own libc relies on, but which no standard
/// promises
///
/// If that stopped working, the child would come up with no
/// standard error at all and writing to it would fail. So the
/// child writes to it and the exit code is the assertion
///
/// #### Note
/// Deliberately not checking *where* the bytes went. They go to
/// this test binary's own stderr, which the harness owns and
/// this test has no business reading. That the write succeeded
/// is the part that would break
#[test]
fn a_run_child_still_has_somewhere_to_write() {
    Runtime::init();

    let wrote = Runtime::block(Process::run(
        "/bin/sh",
        ["-c", "echo run-inherits-stderr 1>&2"],
    ))
    .expect("sh must run");

    assert!(
        wrote.success(),
        "a run child must inherit a usable stderr, exited {:?}",
        wrote.code()
    );

    let to_stdout =
        Runtime::block(Process::run("/bin/sh", ["-c", "echo run-inherits-stdout"]))
            .expect("sh must run");

    assert!(
        to_stdout.success(),
        "a run child must inherit a usable stdout, exited {:?}",
        to_stdout.code()
    );
}
