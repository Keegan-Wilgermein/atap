//! # Send signal

use atap::{Runtime, Signal, SignalKind};
use std::process;

/// Sends `kind` to this program
pub fn send_signal(kind: SignalKind) {
    Runtime::block(Signal::send(process::id() as libc::pid_t, kind)).expect("the signal must go");
}
