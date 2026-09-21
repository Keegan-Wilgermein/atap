//! Testing the api to make sure
//! it all works as intended and
//! is intuitive to use
//! 
//! For testing the api, not functionality

use atap::{Runtime, prelude::Compute};

#[test]
#[ignore = "Not for testing functionality"]
fn api() {
    let _ = Runtime::builder()
        .workers_per_core(12)
        .init();

    let outer = Runtime::task(
        Compute::compute(|i: i32| i)
    )
    .wait_for::<i32>()
    .spawn();

    let outer2 = outer.clone();
    let inner = Runtime::task(
        Compute::compute(move |i: i32| {
            let _ = outer.clone().give(i);
        })
    )
    .receive(outer2)
    .spawn();

    let _ = inner.join();
}
