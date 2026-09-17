//! # Sleeping

use atap::{
    Runtime, TaskHandle,
    sleep::{Sleep, SleepMode},
};
use std::time::Duration;

/// A sleep of a given length, spawned
pub fn sleeping(millis: u64) -> TaskHandle<Duration> {
    Runtime::task(Sleep::sleep(Duration::from_millis(millis)).mode(SleepMode::Relaxed)).spawn()
}
