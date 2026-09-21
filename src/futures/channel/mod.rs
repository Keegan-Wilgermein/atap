//! # Channel
//! Handing values between threads and tasks, in order

pub mod channel;
pub mod channel_task;
pub(crate) mod core;
pub mod ends;

pub use channel::{Bounded, Channel, Unbounded};
pub use channel_task::{ChannelRecvTask, ChannelSendTask};
pub use ends::{BoundedSender, Receiver, Sender};
