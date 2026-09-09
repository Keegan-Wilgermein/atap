//! # Builder
//! The chain that every task is started through, and the type
//! level states it moves between
//!
//! Split in two because the two halves are read for different
//! reasons. [`task_builder`] is the API — what may be called
//! and what each call does. [`builder_markers`] is the reason
//! the API has the shape it does, and is where to look when a
//! chain doesn't compile and the message names a state

pub mod builder_markers;
pub mod task_builder;

pub use builder_markers::{Once, Open, Rate, Repeat, Repeatable, Set};
pub use task_builder::TaskBuilder;
