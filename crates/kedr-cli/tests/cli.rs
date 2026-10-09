// SPDX-License-Identifier: Apache-2.0
use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct ScratchDir(PathBuf);
impl ScratchDir {
    fn new() -> Self {
        loop {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("kedr-cli-test-{}-{id}", std::process::id()));
            match std::fs::create_dir(&path) {
                Ok(()) => return Self(path),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => panic!("cannot create scratch directory: {error}"),
            }
        }
    }
}
impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_kedr"))
}

#[test]
fn doctor_reports_actual_capabilities() {
    let output = cli().arg("doctor").output().unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("Apache-2.0"));
    assert!(text.contains("SQL server, pages, WAL and transactions: not implemented."));
}

#[test]
fn invalid_arguments_are_reported_as_errors() {
    let output = cli().arg("start-server").output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .starts_with("KD1009:")
    );
}

#[test]
fn storage_check_preserves_existing_files_and_cleans_up() {
    let dir = ScratchDir::new();
    let existing = dir.0.join("important.bin");
    std::fs::write(&existing, b"keep this file").unwrap();
    let output = cli().arg("storage-check").arg(&dir.0).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(existing).unwrap(), b"keep this file");
    assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 1);
}

#[test]
fn storage_check_rejects_file_as_directory_without_mutation() {
    let dir = ScratchDir::new();
    let path = dir.0.join("file.bin");
    std::fs::write(&path, b"do not change").unwrap();
    let output = cli().arg("storage-check").arg(&path).output().unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .starts_with("KD1009:")
    );
    assert_eq!(std::fs::read(path).unwrap(), b"do not change");
}
