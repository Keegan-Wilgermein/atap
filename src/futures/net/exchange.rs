//! # Exchange
//! Connecting, sending one request and reading the answer to the
//! end, for any family whose connect hands back a byte stream
//!
//! TCP and TLS both make requests this way. The only difference
//! is how they connect, which is the connect task they step

use crate::{
    RuntimeError,
    futures::{
        net::{
            step::Clock,
            stream::{RecvTask, SendTask},
        },
        task::{Task, sealed::Step},
    },
};
use std::sync::Arc;

/// Where a request is
#[derive(Default)]
pub(crate) enum Stage {
    /// Opening the connection, and whatever the family does to it
    /// before it can carry the request
    #[default]
    Connecting,

    /// Sending the request down it
    Sending(SendTask),

    /// Reading the answer
    Reading(RecvTask),
}

/// A connection a request can be sent down
pub(crate) trait Sends {
    /// Sends every byte of `data`
    fn send_all(&self, data: Arc<[u8]>) -> SendTask;
}

/// Takes a request as far as it can go without waiting
///
/// `clock` covers the whole exchange, so the send and the read
/// run to the one the connect started
pub(crate) fn advance<C, S>(
    connect: &mut C,
    stage: &mut Stage,
    data: &Arc<[u8]>,
    clock: Clock,
    reactor_id: i32,
    task_id: usize,
) -> Step<Result<Vec<u8>, RuntimeError>>
where
    C: Task<Output = Result<S, RuntimeError>>,
    S: Sends,
{
    loop {
        match &mut *stage {
            Stage::Connecting => match connect.step(reactor_id, task_id) {
                Step::Done(Ok(conn)) => {
                    *stage = Stage::Sending(conn.send_all(Arc::clone(data)).timed(clock));
                }

                Step::Done(Err(error)) => return Step::Done(Err(error)),
                Step::Park(park) => return Step::Park(park),
            },

            Stage::Sending(send) => match send.step(reactor_id, task_id) {
                Step::Done(Ok(_)) => {
                    let read = RecvTask::to_end(send.source().clone()).timed(clock);

                    *stage = Stage::Reading(read);
                }

                Step::Done(Err(error)) => return Step::Done(Err(error)),
                Step::Park(park) => return Step::Park(park),
            },

            Stage::Reading(read) => return read.step(reactor_id, task_id),
        }
    }
}
