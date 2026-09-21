//! Testing the api to make sure
//! it all works as intended and
//! is intuitive to use
//! 
//! For testing the api, not functionality

use std::time::Instant;

use atap::{Runtime, prelude::Compute};

#[test]
#[ignore = "Not for testing functionality"]
fn api() {
    let _ = Runtime::builder()
        .workers_per_core(12)
        .init();

    let now = Instant::now();
    let task1 = Runtime::task(
        Compute::compute(|(data1, data2)| {
            data1 + data2
        })
    )
    .wait_for::<(u8, u8)>()
    .spawn();

    let _ = task1.give((3, 2));

    let result = task1.join().unwrap();

    println!("Took: {:?}", now.elapsed());
    println!("Result: {}", result);
    println!("{}", Runtime::pool());
}
