//! # Test path

use std::{
    fs,
    path::PathBuf,
    process,
    sync::atomic::{AtomicUsize, Ordering},
};

/// Keeps test file names apart
static NEXT: AtomicUsize = AtomicUsize::new(0);

/// A path that cleans itself up
pub struct TestPath(PathBuf);

impl TestPath {
    /// Reserves a name nothing else in this run will use
    pub fn new(tag: &str) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/files");

        fs::create_dir_all(&root).expect("could not make tests/files");

        let name = format!(
            "{}-{}-{}.txt",
            tag,
            process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );

        Self(root.join(name))
    }

    /// The path itself
    pub fn path(&self) -> &PathBuf {
        &self.0
    }
}

impl Drop for TestPath {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_dir_all(&self.0);
    }
}
