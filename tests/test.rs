use std::{thread::sleep, time::{Duration, Instant}};
use whenever::{Runtime, Sleep};

#[test]
fn main() {
    Runtime::init();

    let _ = Runtime::block_on(
        Sleep::sleep(Duration::from_secs(5)),
    );
    let now = Instant::now();
    println!("Registered at: {:?}", now);

    sleep(Duration::from_secs(6));
}
