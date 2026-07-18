use std::{thread, time::Duration};
use whenever::{Runtime};

#[test]
fn main() {
    let runtime = Runtime::new();

    let future = runtime.whenever(
        || {
            for _ in 0..5 {
                thread::sleep(Duration::from_secs(1));
            }

            return 0;
    });

    println!("{:?}", future.maybe_get());
}
