//! # Executor
//! Executes async tasks as they
//! are ready, delegates IDs
//! across threads, and communicates
//! with `TaskHandle`s

use std::{cell::RefCell, pin::Pin, ptr, thread, time::Duration};

use crate::{Runtime, Sleep, futures::task::Task, modules::{task_data::TaskData, task_handle::TaskHandle}};

thread_local! {
    static DATA: RefCell<Vec<Pin<Box<TaskData>>>> = RefCell::new(Vec::new());
}

/// Async task executor and handler
pub(crate) struct Executor;

impl Executor {
    /// Initialises a new `Executor`
    pub(crate) fn init() {
        executor_loop();
    }

    /// Adds a new `Task` to be processed
    pub(crate) fn new_task<T>(task: impl Task) -> TaskHandle<T> {
        let task_data = TaskData::new::<T>();

        DATA.with_borrow_mut(|data| {
            data.push(Pin::new(Box::new(task_data)));
        });

        // Don't use 0 for IDs
        // so it is impossible to
        // overlap with blocking calls
        let id = 1;

        TaskHandle::new(id)
    }

    /// Adds 1 to the listener count on a piece of data
    /// 
    /// This is so multiple listeners can be on the same object
    /// while preventing the data from getting cleaned up early
    pub(crate) fn add_listener() {
        todo!()
    }

    /// Gets the task result for a corresponding `TaskHandle` id
    pub(crate) fn get_task_result<T>(id: usize) -> T {
        let result = DATA.with_borrow(|data| {
            unsafe { ptr::read_unaligned(data[id - 1].get_data().as_mut_ptr().cast::<T>()) }
        });

        DATA.with_borrow_mut(|data| {
            data.remove(id);
        });

        result
    }
}

/// The loop the executor runs on
fn executor_loop() {
    thread::spawn(|| {
        loop {
            thread::sleep(Duration::from_secs(1));

            if DATA.with_borrow(|data| {
                data.len()
            }) > 0 {
                let time = Runtime::block(Sleep::sleep(Duration::from_secs(1), true));

                DATA.with_borrow_mut(|data| {
                    data[0].set_data(
                        time.as_nanos().to_ne_bytes().to_vec()
                    );
    
                    // unsafe {
                    //     // Switch for kqueue EVFILT_USER
                    //     libc::os_sync_wake_by_address_all(
                    //         data[0].get_ready_ptr(),
                    //         mem::size_of::<Duration>(),
                    //         libc::OS_SYNC_WAIT_ON_ADDRESS_NONE,
                    //     )
                    // };
                });
            }
        }
    });
}
