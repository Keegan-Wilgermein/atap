use std::{thread::sleep, time::{Duration, Instant}};
use whenever::{Runtime, Sleep};

#[test]
fn main() {
    Runtime::init();

    let start = Instant::now();

    let _ = Runtime::block_on(
        Sleep::sleep(Duration::from_secs(5)),
    );

    println!("Custom sleep total time: {:?}", start.elapsed());

    let start = Instant::now();
    sleep(Duration::from_secs(5));
    println!("Built in sleep total time: {:?}", start.elapsed());
}
