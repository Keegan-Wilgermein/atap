//! # Sleep
//! The `Sleep` future waits for a set time
//! then continues

use std::{pin::Pin, task::{Context, Poll}, time::{Instant}};

pub struct Sleep {
    wake_at: Instant,
}

impl Future for Sleep {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if Instant::now() >= self.wake_at {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}
