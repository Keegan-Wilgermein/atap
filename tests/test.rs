use whenever::{Runtime};

#[test]
fn main() {
    let runtime = Runtime::new();

    let future = runtime.block_on(
        async {
            return 0;
        }
    );
}
