//! # Net
//! What the socket families share: addresses, making sockets,
//! the step state every socket task keeps, and the send and
//! receive tasks a byte stream hands out

pub mod address;
pub(crate) mod datagram;
pub(crate) mod socket;
pub(crate) mod step;
pub mod stream;

pub use address::NetAddress;
pub use stream::{RecvTask, SendTask};
