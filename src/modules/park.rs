//! # Park
//! Runs a task that parks to its end on the calling thread,
//! for the callers that can't give their thread back

use crate::{
    EventDesc,
    constants::READY_POLL,
    futures::task::{
        Task,
        sealed::{Park, Step},
    },
    modules::{
        int_check::IntCheck,
        kevent::{KEvent, eventlist},
        kqueue,
    },
};
use std::{ptr, thread, time::Instant};

/// Steps a task until it is done, waiting on this thread's own
/// queue whenever it parks
///
/// ## Behaviour
/// What `Runtime::block` gets for a task that would park if it
/// were spawned. The wait holds the thread, and a timeout is the
/// task's own deadline, which it checks on every step
pub(crate) fn drive<F>(mut task: F, reactor_id: i32, task_id: usize) -> F::Output
where
    F: Task,
{
    loop {
        match task.step(reactor_id, task_id) {
            Step::Done(out) => return out,
            Step::Park(park) => wait_ready(park),
        }
    }
}

/// Blocks until a parked task's socket might be ready, or its
/// deadline comes
///
/// Any wake will do, since the step that follows looks at the
/// socket and the clock again
fn wait_ready(park: Park) {
    let left = match park.deadline {
        Some(deadline) => {
            let left = deadline.saturating_duration_since(Instant::now());

            // The step that follows gives up by itself
            if left.is_zero() {
                return;
            }

            Some(left)
        }

        None => None,
    };

    let poll = left.map_or(READY_POLL, |left| left.min(READY_POLL));

    // No queue to wait on, so this just polls
    let Ok(queue) = kqueue::id() else {
        thread::sleep(poll);
        return;
    };

    let watched = unsafe {
        KEvent::register(
            queue,
            park.fd as usize,
            0,
            ptr::null_mut(),
            EventDesc::new_ready(park.filter),
        )
    }
    .check();

    if watched.is_err() {
        thread::sleep(poll);
        return;
    }

    let mut events = eventlist();

    let _ = unsafe {
        match left {
            Some(left) => KEvent::listen_for(queue, &mut events, left),
            None => KEvent::listen(queue, &mut events),
        }
    };

    // One shot, so already gone if it was what fired. Taken off
    // either way, since the queue outlives this task
    let _ = unsafe {
        KEvent::register(
            queue,
            park.fd as usize,
            0,
            ptr::null_mut(),
            EventDesc::new_ready_delete(park.filter),
        )
    };
}
