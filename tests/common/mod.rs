//! # Shared test helpers
//!
//! One helper to a file. Every test binary that declares `mod common`
//! compiles all of them, so the ones it doesn't use may go unused

#![allow(dead_code, unused_imports)]

#[cfg(feature = "tls")]
mod certs;
mod cores;
mod cpu_time;
mod descriptor_limit;
mod drain;
mod max_rss;
mod next_run;
mod report;
mod report_full;
mod resources;
mod send_signal;
mod settles;
mod sleeping;
mod take_a_run;
mod taken_over;
mod test_path;
mod until_started;
mod within;

#[cfg(feature = "tls")]
pub use certs::{Certs, certs};
pub use cores::cores;
pub use cpu_time::cpu_time;
pub use descriptor_limit::raise_descriptor_limit;
pub use drain::drain;
pub use max_rss::max_rss;
pub use next_run::next_run;
pub use report::report;
pub use report_full::report_full;
pub use resources::{Resources, footprint, mebibytes};
pub use send_signal::send_signal;
pub use settles::settles;
pub use sleeping::sleeping;
pub use take_a_run::take_a_run;
pub use taken_over::taken_over;
pub use test_path::TestPath;
pub use until_started::until_started;
pub use within::within;
