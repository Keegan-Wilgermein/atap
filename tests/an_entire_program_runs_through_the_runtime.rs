//! # An entire program, run through the runtime
//!
//! A small interpreter whose every stage is a task. Several programs
//! run side by side, and every line that comes out the end is checked
//! against the same program run the plain way
//!
//! Reads the process wide live count at the end, so it has a binary to
//! itself

mod common;

use atap::{Compute, File, Runtime, TaskHandle, Waiting};
use common::{report, settles};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

/// How long anything here waits before counting as stranded
const PATIENCE: Duration = Duration::from_secs(60);

/// Programs run side by side
const PROGRAMS: usize = 8;

/// Lines in each program
const LINES: usize = 60;

/// Below this, `fib` is worked out in place rather than split
const SPLIT_FROM: u32 = 12;

// ---- the language

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Num(i64),
    Name(String),
    Let,
    Print,
    Op(char),
    Open,
    Close,
    Comma,
    Equals,
}

#[derive(Clone, Debug)]
enum Expr {
    Num(i64),
    Var(String),
    Neg(Box<Expr>),
    Bin(Box<Expr>, char, Box<Expr>),
    Call(String, Vec<Expr>),
}

#[derive(Clone, Debug)]
enum Stmt {
    Let(String, Expr),
    Print(Expr),
}

/// What the lexer publishes: the line number and its tokens
type Lexed = (usize, Result<Vec<Token>, String>);

/// What the parser publishes: the line number and its statement
type Parsed = (usize, Result<Stmt, String>);

/// What the evaluator publishes: the line number and what it printed
type Printed = (usize, String);

/// Names every program can read without setting them
type Constants = Arc<HashMap<String, i64>>;

/// Splits a line into tokens
fn lex(text: &str) -> Result<Vec<Token>, String> {
    let chars: Vec<char> = text.chars().collect();
    let mut tokens = Vec::new();
    let mut at = 0;

    while at < chars.len() {
        let c = chars[at];

        match c {
            ' ' => at += 1,

            '0'..='9' => {
                let start = at;

                while at < chars.len() && chars[at].is_ascii_digit() {
                    at += 1;
                }

                let digits: String = chars[start..at].iter().collect();

                tokens.push(Token::Num(
                    digits.parse().map_err(|_| format!("{digits} is too big"))?,
                ));
            }

            'a'..='z' | '_' => {
                let start = at;

                while at < chars.len() && (chars[at].is_ascii_alphanumeric() || chars[at] == '_') {
                    at += 1;
                }

                let word: String = chars[start..at].iter().collect();

                tokens.push(match word.as_str() {
                    "let" => Token::Let,
                    "print" => Token::Print,
                    _ => Token::Name(word),
                });
            }

            '+' | '-' | '*' | '/' | '%' => {
                tokens.push(Token::Op(c));
                at += 1;
            }

            '(' | ')' | ',' | '=' => {
                tokens.push(match c {
                    '(' => Token::Open,
                    ')' => Token::Close,
                    ',' => Token::Comma,
                    _ => Token::Equals,
                });
                at += 1;
            }

            other => return Err(format!("unexpected {other:?} at column {at}")),
        }
    }

    Ok(tokens)
}

/// A recursive descent parser over one line's tokens
struct Parser {
    tokens: Vec<Token>,
    at: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.at)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.at).cloned();
        self.at += 1;
        token
    }

    fn expect(&mut self, wanted: Token) -> Result<(), String> {
        match self.next() {
            Some(token) if token == wanted => Ok(()),
            other => Err(format!("wanted {wanted:?}, found {other:?}")),
        }
    }

    fn statement(&mut self) -> Result<Stmt, String> {
        let statement = match self.next() {
            Some(Token::Let) => {
                let name = match self.next() {
                    Some(Token::Name(name)) => name,
                    other => return Err(format!("let wants a name, found {other:?}")),
                };

                self.expect(Token::Equals)?;

                Stmt::Let(name, self.expr()?)
            }

            Some(Token::Print) => Stmt::Print(self.expr()?),

            other => return Err(format!("a line starts with let or print, not {other:?}")),
        };

        match self.peek() {
            None => Ok(statement),
            Some(extra) => Err(format!("{extra:?} left over at the end")),
        }
    }

    fn expr(&mut self) -> Result<Expr, String> {
        let mut left = self.term()?;

        while let Some(Token::Op(op @ ('+' | '-'))) = self.peek().cloned() {
            self.at += 1;
            left = Expr::Bin(Box::new(left), op, Box::new(self.term()?));
        }

        Ok(left)
    }

    fn term(&mut self) -> Result<Expr, String> {
        let mut left = self.factor()?;

        while let Some(Token::Op(op @ ('*' | '/' | '%'))) = self.peek().cloned() {
            self.at += 1;
            left = Expr::Bin(Box::new(left), op, Box::new(self.factor()?));
        }

        Ok(left)
    }

    fn factor(&mut self) -> Result<Expr, String> {
        match self.next() {
            Some(Token::Num(value)) => Ok(Expr::Num(value)),

            Some(Token::Op('-')) => Ok(Expr::Neg(Box::new(self.factor()?))),

            Some(Token::Open) => {
                let inner = self.expr()?;
                self.expect(Token::Close)?;
                Ok(inner)
            }

            Some(Token::Name(name)) => {
                if self.peek() != Some(&Token::Open) {
                    return Ok(Expr::Var(name));
                }

                self.at += 1;

                let mut args = vec![self.expr()?];

                while self.peek() == Some(&Token::Comma) {
                    self.at += 1;
                    args.push(self.expr()?);
                }

                self.expect(Token::Close)?;

                Ok(Expr::Call(name, args))
            }

            other => Err(format!("wanted a value, found {other:?}")),
        }
    }
}

/// Turns one line's tokens into a statement
fn parse(tokens: Vec<Token>) -> Result<Stmt, String> {
    Parser { tokens, at: 0 }.statement()
}

/// What an expression can see
#[derive(Clone)]
struct Scope {
    vars: Arc<HashMap<String, i64>>,
    constants: Constants,
}

/// Whether an expression calls anything, and so is worth a task
fn heavy(expr: &Expr) -> bool {
    match expr {
        Expr::Num(_) | Expr::Var(_) => false,
        Expr::Neg(inner) => heavy(inner),
        Expr::Bin(left, _, right) => heavy(left) || heavy(right),
        Expr::Call(..) => true,
    }
}

/// One arithmetic step, wrapping rather than overflowing
fn apply(left: i64, op: char, right: i64) -> Result<i64, String> {
    match op {
        '+' => Ok(left.wrapping_add(right)),
        '-' => Ok(left.wrapping_sub(right)),
        '*' => Ok(left.wrapping_mul(right)),
        '/' | '%' if right == 0 => Err(String::from("division by zero")),
        '/' => Ok(left.wrapping_div(right)),
        _ => Ok(left.wrapping_rem(right)),
    }
}

/// Fibonacci, split into tasks from `SPLIT_FROM` up when `split` says to
fn fib(n: u32, split: bool) -> i64 {
    if n < 2 {
        return n as i64;
    }

    if !split || n < SPLIT_FROM {
        return fib(n - 1, false) + fib(n - 2, false);
    }

    let left = Runtime::task(Compute::compute(move |()| fib(n - 1, true))).spawn();
    let right = fib(n - 2, true);

    left.join().expect("a fib branch failed") + right
}

/// Works out an expression, the plain way or split into tasks
///
/// Either way the answer and the first error are the same
fn eval(expr: &Expr, scope: &Scope, split: bool) -> Result<i64, String> {
    match expr {
        Expr::Num(value) => Ok(*value),

        Expr::Var(name) => scope
            .vars
            .get(name)
            .or_else(|| scope.constants.get(name))
            .copied()
            .ok_or_else(|| format!("unknown variable {name}")),

        Expr::Neg(inner) => Ok(eval(inner, scope, split)?.wrapping_neg()),

        Expr::Bin(left, op, right) => {
            let (left, right) = match split && heavy(left) && heavy(right) {
                // The left half on a task of its own, the right half here
                true => {
                    let left_expr = (**left).clone();
                    let left_scope = scope.clone();

                    let handle = Runtime::task(Compute::compute(move |()| {
                        eval(&left_expr, &left_scope, true)
                    }))
                    .spawn();

                    let right = eval(right, scope, true);

                    (
                        handle
                            .join()
                            .unwrap_or_else(|error| Err(format!("a split failed: {error:?}"))),
                        right,
                    )
                }

                false => (eval(left, scope, split), eval(right, scope, split)),
            };

            apply(left?, *op, right?)
        }

        Expr::Call(name, args) => call(name, args, scope, split),
    }
}

/// A built in function
fn call(name: &str, args: &[Expr], scope: &Scope, split: bool) -> Result<i64, String> {
    match name {
        "fib" => {
            let [arg] = args else {
                return Err(String::from("fib takes one argument"));
            };

            let n = eval(arg, scope, split)?.rem_euclid(23) as u32;

            Ok(fib(n, split))
        }

        // Every argument on a task of its own, joined in order
        "sum" if split => {
            let handles: Vec<_> = args
                .iter()
                .cloned()
                .map(|arg| {
                    let scope = scope.clone();

                    Runtime::task(Compute::compute(move |()| eval(&arg, &scope, true))).spawn()
                })
                .collect();

            let mut total = 0i64;

            for result in Runtime::join_all(handles) {
                let value =
                    result.unwrap_or_else(|error| Err(format!("a split failed: {error:?}")))?;
                total = total.wrapping_add(value);
            }

            Ok(total)
        }

        "sum" => args.iter().try_fold(0i64, |total, arg| {
            Ok(total.wrapping_add(eval(arg, scope, false)?))
        }),

        // Worked out by blocking, from inside whatever task is running it
        "max" if split => {
            let args = args.to_vec();
            let scope = scope.clone();

            Runtime::block(Compute::compute(move |()| max(&args, &scope, true)))
        }

        "max" => max(args, scope, false),

        other => Err(format!("no function called {other}")),
    }
}

/// The largest of the arguments
fn max(args: &[Expr], scope: &Scope, split: bool) -> Result<i64, String> {
    let mut best = i64::MIN;

    for arg in args {
        best = best.max(eval(arg, scope, split)?);
    }

    Ok(best)
}

/// Runs one statement, and says what the line printed
fn exec(
    statement: Result<Stmt, String>,
    vars: &mut HashMap<String, i64>,
    constants: Constants,
    split: bool,
) -> String {
    let statement = match statement {
        Ok(statement) => statement,
        Err(error) => return format!("error: {error}"),
    };

    let scope = Scope {
        vars: Arc::new(vars.clone()),
        constants,
    };

    match statement {
        Stmt::Let(name, expr) => match eval(&expr, &scope, split) {
            Ok(value) => {
                vars.insert(name.clone(), value);
                format!("{name} = {value}")
            }
            Err(error) => format!("error: {error}"),
        },

        Stmt::Print(expr) => match eval(&expr, &scope, split) {
            Ok(value) => value.to_string(),
            Err(error) => format!("error: {error}"),
        },
    }
}

/// The constants every program starts with
fn constants() -> Constants {
    Arc::new(HashMap::from([
        (String::from("answer"), 42),
        (String::from("year"), 2026),
    ]))
}

/// A program run the plain way, on this thread, with no tasks at all
fn run_plain(lines: &[String]) -> Vec<String> {
    let mut vars = HashMap::new();

    lines
        .iter()
        .map(|line| exec(lex(line).and_then(parse), &mut vars, constants(), false))
        .collect()
}

// ---- writing programs

/// A small, fast, reproducible source of choices
struct Dice(u64);

impl Dice {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn below(&mut self, bound: u64) -> u64 {
        let mut x = self.0;

        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;

        self.0 = x;

        x % bound.max(1)
    }
}

/// A value: a number, a variable already set, or a constant
fn atom(dice: &mut Dice, names: &[String]) -> String {
    match dice.below(10) {
        0 => String::from("answer"),
        roll if roll < 5 && !names.is_empty() => {
            names[dice.below(names.len() as u64) as usize].clone()
        }
        _ => dice.below(100).to_string(),
    }
}

/// An expression up to `depth` levels deep
fn expression(dice: &mut Dice, names: &[String], depth: u32) -> String {
    if depth == 0 {
        return atom(dice, names);
    }

    match dice.below(9) {
        0 | 1 => atom(dice, names),
        2 => format!("fib({})", expression(dice, names, depth - 1)),
        3 => format!(
            "sum({}, {}, {})",
            expression(dice, names, depth - 1),
            expression(dice, names, depth - 1),
            expression(dice, names, depth - 1),
        ),
        4 => format!(
            "max({}, {})",
            expression(dice, names, depth - 1),
            expression(dice, names, depth - 1),
        ),
        5 => format!("({})", expression(dice, names, depth - 1)),
        6 => format!("-{}", atom(dice, names)),
        _ => {
            let op = ['+', '-', '*', '/', '%'][dice.below(5) as usize];

            let right = match op {
                '/' | '%' => (dice.below(9) + 1).to_string(),
                _ => expression(dice, names, depth - 1),
            };

            format!("{} {} {}", expression(dice, names, depth - 1), op, right)
        }
    }
}

/// A program of `lines` lines, with a few mistakes in it on purpose
fn program(seed: u64, lines: usize) -> Vec<String> {
    let mut dice = Dice::new(seed);
    let mut names: Vec<String> = Vec::new();

    (0..lines)
        .map(|_| match dice.below(24) {
            0 => String::from("let = 5"),
            1 => format!("print {} $ 2", dice.below(9)),
            2 => String::from("print ghost + 1"),
            3 => format!("print {} / (answer - 42)", dice.below(99) + 1),
            roll if roll < 13 || names.is_empty() => {
                let name = format!("v{}", names.len());
                let expr = expression(&mut dice, &names, 3);

                names.push(name.clone());

                format!("let {name} = {expr}")
            }
            _ => format!("print {}", expression(&mut dice, &names, 3)),
        })
        .collect()
}

// ---- the program as tasks

/// One program's stages
struct Pipeline {
    lexer: TaskHandle<Lexed, Waiting<(usize, String)>>,
    parser: TaskHandle<Parsed>,
    evaluator: TaskHandle<Printed>,
    printer: TaskHandle<usize, Waiting<Printed>>,
    printed: mpsc::Receiver<Printed>,
    log: PathBuf,
}

/// Spawns every stage of one program, wired together
fn pipeline(index: usize, constants: &TaskHandle<Constants>) -> Pipeline {
    let log = files().join(format!("program-{}-{}.log", std::process::id(), index));

    let _ = fs::remove_file(&log);

    let (sent, printed) = mpsc::channel::<Printed>();

    // Appends to a file, so it waits on a sleep thread rather than a worker
    let printer = {
        let log = log.clone();

        Runtime::task(
            Compute::compute(move |(n, line): Printed| {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log)
                    .expect("could not open the log");

                writeln!(file, "{n:>3}: {line}").expect("could not write to the log");

                let _ = sent.send((n, line.clone()));

                line.len()
            })
            .blocking(),
        )
        .wait_for::<Printed>()
        .spawn()
    };

    let lexer = Runtime::task(Compute::compute(|(n, text): (usize, String)| {
        (n, lex(&text))
    }))
    .wait_for::<(usize, String)>()
    .spawn();

    let parser = Runtime::task(Compute::compute(|(n, tokens): Lexed| {
        (n, tokens.and_then(parse))
    }))
    .receive(lexer.clone())
    .spawn();

    // Holds a handle to the constants, so they outlive every handle the
    // program itself drops
    let evaluator = {
        let constants = constants.clone();
        let vars = Arc::new(Mutex::new(HashMap::new()));

        Runtime::task(Compute::compute(move |(n, statement): Parsed| {
            let constants = constants
                .join_with_timeout(PATIENCE)
                .expect("the constants never arrived");

            let mut vars = vars.lock().expect("the variables were poisoned");

            (n, exec(statement, &mut vars, constants, true))
        }))
        .receive(parser.clone())
        .give_to(&printer)
        .spawn()
    };

    Pipeline {
        lexer,
        parser,
        evaluator,
        printer,
        printed,
        log,
    }
}

/// Where the source files and logs go
fn files() -> PathBuf {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

    fs::create_dir_all(&root).expect("could not make tests/files");

    root
}

/// Every program, through every stage, checked line by line
#[test]
fn an_entire_program_runs_through_the_runtime() {
    Runtime::init();

    let before = Runtime::workers();

    report("before");

    let programs: Vec<Vec<String>> = (0..PROGRAMS)
        .map(|index| program(index as u64 + 1, LINES))
        .collect();

    let expected: Vec<Vec<String>> = programs.iter().map(|lines| run_plain(lines)).collect();

    // ---- the source files, written blocking and read back spawned
    let paths: Vec<PathBuf> = programs
        .iter()
        .enumerate()
        .map(|(index, lines)| {
            let path = files().join(format!("program-{}-{}.calc", std::process::id(), index));

            Runtime::block(File::write(&path, lines.join("\n").into_bytes()))
                .expect("could not write a program");

            path
        })
        .collect();

    let sources: Vec<Vec<String>> = Runtime::join_all(
        paths
            .iter()
            .map(|path| Runtime::task(File::read(path)).spawn()),
    )
    .into_iter()
    .map(|read| {
        let bytes = read
            .expect("a read failed")
            .expect("a program couldn't be read");

        String::from_utf8(bytes)
            .expect("a program came back as something other than text")
            .lines()
            .map(String::from)
            .collect()
    })
    .collect();

    assert_eq!(
        sources, programs,
        "the programs came back from disk changed"
    );

    println!(
        "{} programs of {} lines written and read back through the runtime",
        PROGRAMS, LINES
    );

    // ---- every stage of every program, spawned before any line goes in
    let shared = Runtime::task(Compute::compute(|()| constants())).spawn();

    let pipelines: Vec<Pipeline> = (0..PROGRAMS)
        .map(|index| pipeline(index, &shared))
        .collect();

    // Only the evaluators hold it from here on
    drop(shared);

    let heard = Arc::new(Mutex::new(Vec::<Printed>::new()));

    let logger = {
        let heard = Arc::clone(&heard);

        Runtime::task(Compute::compute(move |line: Printed| {
            heard.lock().expect("the logger was poisoned").push(line);
        }))
        .receive_any(
            pipelines
                .iter()
                .map(|pipeline| pipeline.evaluator.clone())
                .collect::<Vec<_>>(),
        )
        .spawn()
    };

    report("every stage spawned");

    // ---- each program driven from its own thread
    let started = Instant::now();

    let drivers: Vec<_> = pipelines
        .into_iter()
        .zip(sources)
        .enumerate()
        .map(|(index, (pipeline, lines))| {
            thread::spawn(move || {
                let began = Instant::now();
                let mut out = Vec::with_capacity(lines.len());

                for (n, line) in lines.into_iter().enumerate() {
                    pipeline
                        .lexer
                        .give((n, line))
                        .expect("the lexer refused a line");

                    let (at, printed) =
                        pipeline.printed.recv_timeout(PATIENCE).unwrap_or_else(|_| {
                            panic!("program {index}, line {n} never came out the end")
                        });

                    assert_eq!(
                        at, n,
                        "program {index} printed line {at} when line {n} went in"
                    );

                    out.push(printed);
                }

                (index, out, pipeline, began.elapsed())
            })
        })
        .collect();

    let mut finished: Vec<_> = drivers
        .into_iter()
        .map(|driver| driver.join().expect("a program's driver went down"))
        .collect();

    finished.sort_by_key(|(index, ..)| *index);

    let ran = started.elapsed();

    report("every program run");

    // ---- what came out the end
    println!();

    for (index, out, _, took) in &finished {
        let errors = out.iter().filter(|line| line.starts_with("error")).count();
        let lets = out.iter().filter(|line| line.contains(" = ")).count();

        println!(
            "program {index}: {} lines in {:?}, {lets} lets, {} prints, {errors} errors, last line \
             {:?}",
            out.len(),
            took,
            out.len() - lets - errors,
            out.last().map(String::as_str).unwrap_or(""),
        );

        assert_eq!(
            out, &expected[*index],
            "program {index} came out different from the same program run the plain way"
        );
    }

    println!("\nprogram 0, line by line:");

    for (source, printed) in programs[0].iter().zip(&finished[0].1).take(20) {
        println!("  {source:<48} => {printed}");
    }

    println!("  ...");

    // ---- where every stage finished, gathered in one receive
    for (index, _, pipeline, _) in &finished {
        let last = Runtime::task(Compute::compute(
            |(lexed, parsed, printed): (Lexed, Parsed, Printed)| (lexed.0, parsed.0, printed.0),
        ))
        .receive((
            pipeline.lexer.clone(),
            pipeline.parser.clone(),
            pipeline.evaluator.clone(),
        ))
        .count(1)
        .spawn();

        assert_eq!(
            last.join_with_timeout(PATIENCE),
            Ok((LINES - 1, LINES - 1, LINES - 1)),
            "program {index}'s stages didn't all finish on its last line",
        );
    }

    // ---- the logger heard only real lines
    let heard = heard.lock().expect("the logger was poisoned").clone();

    println!(
        "\n{} programs, {} lines, ran in {:?}; the logger heard {} of them through receive_any",
        PROGRAMS,
        PROGRAMS * LINES,
        ran,
        heard.len(),
    );

    assert!(!heard.is_empty(), "the logger never heard a line");

    for (n, line) in &heard {
        assert!(
            expected.iter().any(|program| program.get(*n) == Some(line)),
            "the logger heard line {n} as {line:?}, which no program printed",
        );
    }

    // ---- the logs the printers wrote on sleep threads
    for (index, _, pipeline, _) in &finished {
        let written = Runtime::block(File::read(&pipeline.log)).expect("a log couldn't be read");
        let lines = String::from_utf8_lossy(&written).lines().count();

        assert_eq!(lines, LINES, "program {index}'s log has {lines} lines");
    }

    // ---- winding down: ending the lexers ends everything after them
    let logs: Vec<PathBuf> = finished
        .iter()
        .map(|(_, _, pipeline, _)| pipeline.log.clone())
        .collect();

    // Each printer's channel, which only closes once the printer has
    // finished and dropped its end. A handle to a printer could give to
    // it, so holding one would keep it waiting
    let mut outputs = Vec::new();

    for (_, _, pipeline, _) in finished {
        let Pipeline {
            lexer,
            parser,
            evaluator,
            printer,
            printed,
            ..
        } = pipeline;

        drop((parser, evaluator, printer));
        lexer.cancel();

        outputs.push(printed);
    }

    assert!(
        settles(|| logger.is_finished()),
        "the logger outlived every evaluator it heard"
    );

    for (index, printed) in outputs.into_iter().enumerate() {
        assert!(
            matches!(
                printed.recv_timeout(PATIENCE),
                Err(mpsc::RecvTimeoutError::Disconnected)
            ),
            "program {index}'s printer kept waiting after nothing could give to it"
        );
    }

    drop(logger);

    assert!(
        settles(|| Runtime::workers().live() <= before.live()),
        "{} tasks still live after the programs ended, against {} before",
        Runtime::workers().live(),
        before.live(),
    );

    report("wound down");

    for path in paths.iter().chain(&logs) {
        let _ = fs::remove_file(path);
    }
}
