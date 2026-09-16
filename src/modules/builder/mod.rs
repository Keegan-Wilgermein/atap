//! # Builder
//! The chain every task is started through, and the type level
//! states it moves between

pub mod builder_markers;
pub mod task_builder;

pub use builder_markers::{
    NoWait, Once, Open, Rate, ReceiveAll, ReceiveAny, Repeat, Repeatable, Set, Unset, WaitFor, Waits,
    Wiring,
};
pub use task_builder::TaskBuilder;
