// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use kedr_core::storage::{FileStorage, StorageIo, read_exact_at, write_all_at};
use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::Duration,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);
struct ScratchDir(PathBuf);
impl ScratchDir {
    fn new() -> Self {
        for _ in 0..100 {
            let id = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!("kedr-tests-{}-{id}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("cannot create test directory: {error}"),
            }
        }
        panic!("cannot reserve test directory");
    }
}
impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn file_reopen_sparse_write_resize_and_exclusive_creation() {
    let dir = ScratchDir::new();
    let path = dir.0.join("file.bin");
    assert!(FileStorage::open_existing(&path).is_err());
    let mut file = FileStorage::create_new(&path).unwrap();
    write_all_at(&mut file, 4, b"hello").unwrap();
    file.sync().unwrap();
    assert!(FileStorage::create_new(&path).is_err());
    drop(file);
    let mut file = FileStorage::open_existing(&path).unwrap();
    let mut bytes = [0xff; 9];
    read_exact_at(&mut file, 0, &mut bytes).unwrap();
    assert_eq!(&bytes, b"\0\0\0\0hello");
    assert_eq!(file.len().unwrap(), 9);
    file.set_len(6).unwrap();
    file.set_len(9).unwrap();
    file.sync().unwrap();
    read_exact_at(&mut file, 0, &mut bytes).unwrap();
    assert_eq!(&bytes, b"\0\0\0\0he\0\0\0");
    assert_eq!(
        write_all_at(&mut file, u64::MAX, b"bad")
            .unwrap_err()
            .code(),
        "KD1002"
    );
    assert_eq!(file.len().unwrap(), 9);
}

#[test]
fn synchronized_prefix_survives_real_process_kill() {
    let dir = ScratchDir::new();
    let path = dir.0.join("crash.bin");
    let child = Command::new(env!("CARGO_BIN_EXE_kedr-crash-fixture"))
        .arg(&path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut child = ChildGuard(child);
    let stdout = child.0.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut line = String::new();
        let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
        let _ = sender.send(result);
    });
    let ready = receiver
        .recv_timeout(Duration::from_secs(10))
        .expect("child did not reach sync point")
        .unwrap();
    assert_eq!(ready.trim(), "READY");
    child.0.kill().unwrap(); // SIGKILL on Unix: no destructor or graceful shutdown.
    let status = child.0.wait().unwrap();
    assert!(!status.success());
    reader.join().unwrap();
    let mut file = FileStorage::open_existing(path).unwrap();
    let mut prefix = [0; 19];
    read_exact_at(&mut file, 0, &mut prefix).unwrap();
    assert_eq!(&prefix, b"synchronized-prefix");
    // Unsynced bytes may survive a process kill through the OS cache. No claim
    // about their disappearance or about power failure is made by this test.
}
