# atap
atap (Any Time Any Place) is an async runtime built in rust

## Quick start
```rust
use whenever::{Runtime, Sleep};

fn main() {
    Runtime::init();

    let task: SleepTask = Sleep::sleep(Duration::from_secs(5)); // Inert on creation
    
    let _ = Runtime::block_on(
        task // Starts execution
    );
}
```
