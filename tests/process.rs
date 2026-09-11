//! Process task tests
//!
//! Only programs a stock macOS install has are used

use atap::{Process, Runtime, RuntimeError, TaskHandle};
use std::{
    fs, thread,
    time::{Duration, Instant},
};

/// How long a test waits for a child that ought to be quick
const PATIENCE: Duration = Duration::from_secs(20);

/// Waits for a spawned task, but not forever
///
/// ## Returns
/// `None` if the deadline passed with nothing settled
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

/// Stdout and stderr each come back as themselves
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

/// A child that writes far more than a pipe holds to both
/// streams finishes
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

/// Many children spawned at once all finish
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

/// A cancelled task's child stops running
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

    // Long enough that the child is running and writing
    thread::sleep(Duration::from_millis(500));

    let before_cancel = fs::metadata(&scratch).map(|found| found.len()).unwrap_or(0);

    assert!(
        before_cancel > 0,
        "the child must be writing before the cancel, or this proves nothing"
    );

    println!("cancelling a child that writes while it lives");
    handle.clone().cancel();
    let _ = handle.wait();

    // Long enough for a surviving child to write many more times
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

/// A child's standard input is `/dev/null`, not this process's
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

/// A child gets the default `SIGPIPE` back
#[test]
fn sigpipe_is_reset() {
    Runtime::init();

    let handle = Runtime::task(Process::output("/bin/sh", ["-c", "yes | head -1"])).spawn();

    let found = settled(&handle, PATIENCE)
        .expect("a closed pipe must kill the writer rather than spin")
        .expect("sh must run");

    assert_eq!(found.stdout(), b"y\n", "head takes exactly one line");
}

/// A zero byte is refused, and the refusal says which half was
/// wrong
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

/// A run child can still write to this process's stderr
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

/// A child fed more than a pipe holds, while echoing it back,
/// finishes
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

/// A child that takes only part of its input still finishes
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

/// An empty input is the same as none at all
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

/// Input, directory and environment all at once
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
