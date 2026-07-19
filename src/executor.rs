//! # Executor
//! 
//! The executor does shit after an event is detected by the `Reactor`
//! 
//! It also registers events with said `Reactor`

use std::{collections::HashMap, ptr, sync::{Arc, Mutex, mpsc::{Receiver, Sender}}, task::Waker, thread};
use libc::kevent;
use crate::{reactor::status_error_check, modules::{interest::Interest, kevent::new_kevent}};

pub(crate) struct Executor {
    kqueue: i32,
    mutex: Arc<Mutex<HashMap<i32, ()>>>,
}

impl Executor {
    pub(crate) fn new(
        kqueue: i32,
        recv: Receiver<i32>,
    ) -> Self {
        let mutex = Arc::new(
            Mutex::new(HashMap::new())
        );

        executor_thread(recv, mutex.clone());

        Self {
            kqueue,
            mutex,
        }
    }

    pub(crate) fn register(&mut self, interest: Interest, waker: Waker) {
        let new_kevent = new_kevent(interest, waker.clone());

        let status =  unsafe {
            kevent(
                self.kqueue,
                &new_kevent,
                1,
                ptr::null_mut(),
                0,
                ptr::null(),
            )
        };

        if status != 0 {
            status_error_check();
        }

       let data = waker.data() as i32;
        let mut guard = self.mutex.try_lock().unwrap();
        guard.insert(data, ());
    }
}

fn executor_thread(
    recv: Receiver<i32>,
    queue: Arc<Mutex<HashMap<i32, ()>>>,
) {
    thread::spawn(move || {
        loop {
            recv.iter()
            .for_each(|fd| {
                let guard = queue.try_lock().unwrap();
                if let Some(task) = guard.get(&fd) {

                }
            });
        }
    });
}
