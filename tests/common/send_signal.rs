//! # Send signal

use atap::{
    Runtime,
    signal::{Signal, SignalKind},
};
use std::process;

/// Sends `kind` to this program
pub fn send_signal(kind: SignalKind) {
    Runtime::block(Signal::send(process::id() as i32, kind)).expect("the signal must go");
}
