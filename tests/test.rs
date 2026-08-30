use std::time::Duration;
use whenever::{Runtime, sleep::Sleep};

#[test]
fn main() {
    Runtime::init();

    let _ = Runtime::block_on(
        Sleep::sleep(Duration::from_nanos(249))
    );
}
