//! # Builder
//! The chain every task is started through, and the type level
//! states it moves between

pub mod builder_markers;
pub mod task_builder;

pub use builder_markers::{Once, Open, Rate, Repeat, Repeatable, Set};
pub use task_builder::TaskBuilder;
