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

// -- Input, working directory and environment --------------

/// The bytes reach the child
#[test]
fn input_reaches_the_child() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/cat", Process::NO_ARGS).input(b"hello\n".as_slice()),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("cat must settle")
        .expect("cat must run");

    assert_eq!(
        found.stdout(),
        b"hello\n",
        "cat gave back {:?}",
        String::from_utf8_lossy(found.stdout())
    );
}

/// A child fed more than a pipe holds finishes
///
/// ## Behaviour
/// The headline test for the input side. `cat` echoes as it
/// reads, so both directions are live at once and four
/// megabytes is sixty times what either pipe can hold
///
/// Fails at about 64 KiB for every version of the mistake:
/// writing it all before draining, draining before writing, or
/// following an `EVFILT_WRITE` wake with an ordinary blocking
/// write — which is the subtle one, since a wake there means
/// there is *room*, possibly one byte, and a blocking write
/// does not come back until it has placed everything
#[test]
fn a_child_fed_more_than_a_pipe_holds_does_not_deadlock() {
    Runtime::init();

    const FLOOD: usize = 4 * 1024 * 1024;

    let fed = vec![b'z'; FLOOD];

    println!("feeding cat {FLOOD} bytes while reading it back");

    let handle =
        Runtime::task(Process::output("/bin/cat", Process::NO_ARGS).input(fed.as_slice())).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a flooded child must settle rather than deadlock")
        .expect("cat must run");

    assert_eq!(found.stdout().len(), FLOOD, "cat gave back the wrong amount");
    assert!(found.status().success(), "cat must finish happily");
}

/// The child is told when its input has ended
///
/// `wc` reads until end of file and then reports. If the write
/// end is never closed there is no end of file, and it waits
/// for one forever — so this settling at all is half the
/// assertion and the count is the other half
#[test]
fn input_ends_so_the_child_sees_its_end() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/usr/bin/wc", ["-c"]).input(b"12345".as_slice()),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("wc must reach the end of its input")
        .expect("wc must run");

    let counted = String::from_utf8_lossy(found.stdout()).trim().to_string();

    assert_eq!(counted, "5", "wc counted {counted:?}");
}

/// A child that never reads its input still finishes
///
/// The parent is left holding four megabytes nobody wants. A
/// write loop with no way out would sit on them forever
#[test]
fn a_child_that_ignores_its_input_finishes() {
    Runtime::init();

    let fed = vec![b'z'; 4 * 1024 * 1024];

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "echo done"]).input(fed.as_slice()),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a child that ignores its input must still finish")
        .expect("sh must run");

    assert_eq!(found.stdout(), b"done\n", "the child ran to its own end");
    assert!(found.status().success(), "and was not treated as a failure");
}

/// A child that takes part of its input still finishes
///
/// The `sigpipe_is_reset` test seen from the other end of the
/// pipe. `head` stops reading after ten bytes and the parent is
/// left with the rest — which is a choice the child is allowed
/// to make, not an error to report
#[test]
fn a_child_that_takes_part_of_its_input_finishes() {
    Runtime::init();

    let fed = vec![b'z'; 4 * 1024 * 1024];

    let handle = Runtime::task(
        Process::output("/usr/bin/head", ["-c", "10"]).input(fed.as_slice()),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a child that stops reading must not hang its parent")
        .expect("head must run");

    assert_eq!(found.stdout().len(), 10, "head takes exactly what it asked for");
    assert!(
        found.status().success(),
        "a child stopping early is not a failure, exited {:?}",
        found.status().code()
    );
}

/// Input reaches a run child too
///
/// No capture here, so the exit code is the whole evidence —
/// `grep -q` succeeds only if it actually read the line
#[test]
fn input_reaches_a_run_child() {
    Runtime::init();

    let found = Runtime::block(
        Process::run("/usr/bin/grep", ["-q", "ping"]).input(b"ping\n".as_slice()),
    )
    .expect("grep must run");

    assert!(found.success(), "grep must find what it was fed");

    let missing = Runtime::block(
        Process::run("/usr/bin/grep", ["-q", "ping"]).input(b"pong\n".as_slice()),
    )
    .expect("grep must run");

    assert_eq!(
        missing.code(),
        Some(1),
        "grep must not find what it was not fed"
    );
}

/// A run blocked on input can still be cancelled
///
/// ## Behaviour
/// The test that justifies waiting on a queue rather than
/// simply writing. `sleep` holds its standard input open and
/// never reads a byte, so there is no broken pipe to end the
/// write — a plain blocking `write` would wedge here for thirty
/// seconds with the cancel unable to reach the thread
///
/// The child says whether it is alive by holding the pool. What
/// this actually watches is the task settling at all
#[test]
fn a_run_blocked_on_input_can_still_be_cancelled() {
    Runtime::init();

    let fed = vec![b'z'; 4 * 1024 * 1024];

    let handle =
        Runtime::task(Process::run("/bin/sleep", ["30"]).input(fed.as_slice())).spawn();

    // Long enough that the write has filled the pipe and the
    // thread is inside its wait, since a cancel before that is a
    // different path and not the one under test
    thread::sleep(Duration::from_millis(500));

    println!("cancelling a run wedged on a child that never reads");
    let started = Instant::now();

    handle.clone().cancel();
    let _ = handle.wait();

    assert!(
        started.elapsed() < Duration::from_secs(15),
        "a cancel must reach a task waiting on room to write, took {:?}",
        started.elapsed()
    );

    assert!(
        handle.is_cancelled(),
        "the task must settle cancelled, was {:?}",
        handle.state()
    );
}

/// An empty input is the same as none at all
///
/// Both are an immediate end of file from the child's side, so
/// the cheaper of the two is used for each
#[test]
fn an_empty_input_is_the_same_as_none() {
    Runtime::init();

    let handle =
        Runtime::task(Process::output("/bin/cat", Process::NO_ARGS).input(b"".as_slice())).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("an empty input must still end")
        .expect("cat must run");

    assert!(found.stdout().is_empty(), "there was nothing to give it");
    assert!(found.status().success(), "cat must finish happily");
}

/// The child starts where it was told to
///
/// `/usr` rather than `/tmp`, which is a symlink to
/// `/private/tmp` — `pwd` reports the physical path and the
/// test would be comparing against the wrong one
#[test]
fn in_dir_changes_where_the_child_starts() {
    Runtime::init();

    let handle =
        Runtime::task(Process::output("/bin/pwd", Process::NO_ARGS).in_dir("/usr")).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("pwd must settle")
        .expect("pwd must run");

    let where_it_ran = String::from_utf8_lossy(found.stdout()).trim().to_string();

    assert_eq!(where_it_ran, "/usr", "the child started in {where_it_ran:?}");
}

/// A relative program runs once, in the new directory
///
/// ## Behaviour
/// The macOS bug this design exists to route around: a relative
/// program spawned alongside a directory change is *launched*
/// and then reported as `ENOENT` anyway, which would leave a
/// child running that nothing has a pid for
///
/// Two assertions, and the second is the important one. Success
/// says the spawn was not wrongly reported as a failure; the
/// byte count says the program ran **once**, which is what
/// would break if the `PATH` walk retried after a bogus error
#[test]
fn a_relative_program_runs_once_in_the_new_directory() {
    Runtime::init();

    let scratch = std::env::temp_dir().join(format!("atap-relative-{}.txt", std::process::id()));
    let _ = fs::remove_file(&scratch);

    let script = format!("echo x >> {}", scratch.display());

    let found = Runtime::block(Process::run("./sh", ["-c", &script]).in_dir("/bin"))
        .expect("a relative program must run rather than report a phantom ENOENT");

    assert!(found.success(), "the shell itself must succeed");

    let wrote = fs::metadata(&scratch).map(|found| found.len()).unwrap_or(0);
    let _ = fs::remove_file(&scratch);

    assert_eq!(wrote, 2, "the program must run exactly once, wrote {wrote} bytes");
}

/// A directory that isn't there is reported
///
/// The failure this guards against is the worst one available:
/// a directory quietly ignored, so the program runs somewhere
/// nobody asked for and succeeds
#[test]
fn a_directory_that_is_not_there_is_reported() {
    Runtime::init();

    let found = Runtime::block(Process::run("/bin/pwd", Process::NO_ARGS).in_dir("/no/such/dir"));

    assert!(
        found.is_err(),
        "a missing directory must not be silently ignored, got {found:?}"
    );
}

/// A directory that can't be used is refused before anything
/// runs
#[test]
fn a_relative_directory_is_refused() {
    Runtime::init();

    let relative = Runtime::block(Process::run("/bin/pwd", Process::NO_ARGS).in_dir("build"));

    assert_eq!(
        relative,
        Err(RuntimeError::BadDirectory),
        "a relative directory must be refused, got {relative:?}"
    );

    let holed = Runtime::block(Process::run("/bin/pwd", Process::NO_ARGS).in_dir("/a\0b"));

    assert_eq!(
        holed,
        Err(RuntimeError::BadDirectory),
        "a zero byte must be refused, got {holed:?}"
    );
}

/// A variable reaches the child
#[test]
fn env_puts_a_variable_in_the_child() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "printf %s \"$ATAP_TEST\""])
            .env([("ATAP_TEST", "yes")]),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("sh must settle")
        .expect("sh must run");

    assert_eq!(found.stdout(), b"yes", "the variable did not arrive");
}

/// An overlay leaves the rest of the environment alone
#[test]
fn env_leaves_the_rest_of_the_environment_alone() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "printf %s \"$PATH\""]).env([("ATAP_TEST", "yes")]),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("sh must settle")
        .expect("sh must run");

    assert!(
        !found.stdout().is_empty(),
        "an overlay must not replace the whole environment"
    );
}

/// An overlay replaces a variable rather than adding it twice
///
/// ## Behaviour
/// The test that separates a real merge from a concatenation.
/// Appending and trusting the child to read the first of two
/// entries is unsound as a contract — POSIX leaves duplicates
/// unspecified, and a program walking the array itself sees
/// both
#[test]
fn env_replaces_a_variable_rather_than_adding_it_twice() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "env | grep -c '^HOME='"]).env([("HOME", "/atap")]),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("sh must settle")
        .expect("sh must run");

    let seen = String::from_utf8_lossy(found.stdout()).trim().to_string();

    assert_eq!(seen, "1", "HOME appeared {seen} times, not once");

    let value = Runtime::task(
        Process::output("/bin/sh", ["-c", "printf %s \"$HOME\""]).env([("HOME", "/atap")]),
    )
    .spawn();

    let found = settled(&value, PATIENCE)
        .expect("sh must settle")
        .expect("sh must run");

    assert_eq!(found.stdout(), b"/atap", "and the overlay's value is the one kept");
}

/// A replaced environment gives the child nothing else
///
/// An absolute program, so nothing here depends on whether the
/// `PATH` lookup reads the parent's environment or the child's
#[test]
fn env_only_gives_the_child_nothing_else() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/usr/bin/env", Process::NO_ARGS).env_only([("ONLY", "1")]),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("env must settle")
        .expect("env must run");

    assert_eq!(
        found.stdout(),
        b"ONLY=1\n",
        "the child kept more than it was given: {:?}",
        String::from_utf8_lossy(found.stdout())
    );
}

/// A variable that can't be passed on is refused
#[test]
fn a_bad_variable_is_refused() {
    Runtime::init();

    for (name, value, why) in [
        ("A\0B", "x", "a zero byte in the name"),
        ("A", "x\0y", "a zero byte in the value"),
        ("A=B", "x", "an equals sign in the name"),
        ("", "x", "an empty name"),
    ] {
        let found = Runtime::block(
            Process::run("/usr/bin/true", Process::NO_ARGS).env([(name, value)]),
        );

        assert_eq!(
            found,
            Err(RuntimeError::BadVariable),
            "{why} must be refused, got {found:?}"
        );
    }
}

/// A repeat feeds every run
///
/// ## Behaviour
/// Answers from outside the question the design answers from
/// inside: the write cursor is a local of the run rather than a
/// field on the task. A design that kept it on the task, or
/// that consumed the input, would give `hi` once and then
/// nothing twice
#[test]
fn a_repeat_feeds_every_run() {
    Runtime::init();

    let handle = Runtime::task(Process::output("/bin/cat", Process::NO_ARGS).input(b"hi".as_slice()))
        .repeat()
        .every(Duration::from_millis(30))
        .count(3)
        .spawn();

    let mut runs = 0;

    while let Some(found) = next_run(&handle, PATIENCE) {
        assert_eq!(
            found.expect("cat must run").stdout(),
            b"hi",
            "run {runs} was not fed"
        );

        runs += 1;
    }

    println!("saw {runs} fed runs against a count of 3");

    assert!(handle.is_finished(), "the series never reported finishing");
    assert_eq!(runs, 3, "saw {runs} runs, not 3");
}

/// Every setting survives a repeat
///
/// The whole `Clone` story in one: a repeat clones the task per
/// run, and a setting that did not come across would show up on
/// the second run rather than the first
#[test]
fn every_setting_survives_a_repeat() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "pwd; printf %s \"$V\""])
            .in_dir("/usr")
            .env([("V", "set")]),
    )
    .repeat()
    .every(Duration::from_millis(30))
    .count(2)
    .spawn();

    let mut runs = 0;

    while let Some(found) = next_run(&handle, PATIENCE) {
        let said = String::from_utf8_lossy(found.expect("sh must run").stdout()).to_string();

        assert_eq!(said, "/usr\nset", "run {runs} lost a setting, said {said:?}");

        runs += 1;
    }

    assert_eq!(runs, 2, "saw {runs} runs, not 2");
}

/// All three settings at once
///
/// Chiefly a test of the file action ordering in the spawn —
/// the axes interfering with each other would show up here and
/// nowhere else
#[test]
fn all_three_at_once() {
    Runtime::init();

    let handle = Runtime::task(
        Process::output("/bin/sh", ["-c", "cat; pwd; printf %s \"$V\""])
            .input(b"fed\n".as_slice())
            .in_dir("/usr")
            .env([("V", "set")]),
    )
    .spawn();

    let found = settled(&handle, PATIENCE)
        .expect("sh must settle")
        .expect("sh must run");

    let said = String::from_utf8_lossy(found.stdout()).to_string();

    assert_eq!(said, "fed\n/usr\nset", "the three settings interfered: {said:?}");
}
