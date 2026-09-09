use std::{
    thread,
    time::{Duration, Instant},
};
use atap::{Runtime, Sleep};

#[test]
fn sleep_accuracy_vs_std_blocking() {
    Runtime::init();

    let duration = Duration::from_secs(1);

    let handle = thread::spawn(move || {
        println!("Running std ...\n");
        let start = Instant::now();
        thread::sleep(duration);
        let elapsed = start.elapsed();
        println!("std slept for: {:?}", elapsed);

        elapsed
    });

    println!("Running atap ...");
    let result = Runtime::block(
        Sleep::sleep(duration, true),
    );

    println!("Result: {:?}", result);

    let std = handle.join().unwrap();

    println!("\nDiff: {:?}", std - result);
    println!("std error:{:?}\natap error:{:?}\n", std - duration, result - duration)
}

#[test]
fn sleep_multi_threaded_blocking() {
    Runtime::init();

    let threads = 10;

    (1..=threads).into_iter()
    .for_each(|i| {
        thread::spawn(move || {
            let duration = Duration::from_secs(i);
    
            let time = Runtime::block(
                Sleep::sleep(
                    duration ,i % 2 == 0,
                )
            );
    
            let error = time - duration;
            println!("Thread {} slept for {:?}\n{:?} error\n", i, time, error);
        });
    });

    Runtime::block(
        Sleep::sleep(
            Duration::from_secs(threads + 2), false,
        )
    );
}

#[test]
fn single_spawned_task() {
    Runtime::init();

    let handle = Runtime::spawn(
        Sleep::sleep(Duration::from_secs(2), true),
    );

    if let Ok(time) = handle.join() {
        println!("atap slept for: {:?}", time);
    } else {
        println!("FUUUUCK");
    };
}
