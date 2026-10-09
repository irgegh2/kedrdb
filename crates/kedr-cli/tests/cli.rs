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
    assert!(text.contains("Slotted-page codec and CRC32C: available."));
    assert!(
        text.contains(
            "B+ tree, bounded page cache, typed row codec and integrity check: available."
        )
    );
    assert!(
        text.contains("Single-owner storage transactions, WAL redo and checkpoints: available.")
    );
    assert!(
        text.contains("Local concurrent sessions, fair locks and transaction timeouts: available.")
    );
    assert!(text.contains("SQL server and network protocol: not implemented."));
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

#[test]
fn store_commands_persist_and_integrity_read_preserves_file() {
    let dir = ScratchDir::new();
    let path = dir.0.join("data.kdr");
    let output = cli().arg("create-store").arg(&path).output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = cli()
        .arg("put")
        .arg(&path)
        .args(["-42", "Кедр 🌲"])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let output = cli().arg("get").arg(&path).arg("-42").output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "Кедр 🌲\n");
    let before = std::fs::read(&path).unwrap();
    let output = cli().arg("integrity-check").arg(&path).output().unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("1 records")
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let output = cli().arg("create-store").arg(&path).output().unwrap();
    assert!(!output.status.success());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let output = cli()
        .arg("put")
        .arg(&path)
        .args(["9223372036854775808", "overflow"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    let output = cli().arg("delete").arg(&path).arg("-42").output().unwrap();
    assert!(output.status.success());
    let output = cli().arg("get").arg(&path).arg("-42").output().unwrap();
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("Key not found")
    );
}

#[test]
fn locked_store_rejects_another_process_and_releases_on_close() {
    use kedr_core::storage::FileStorage;
    let dir = ScratchDir::new();
    let path = dir.0.join("locked.kdr");
    assert!(
        cli()
            .arg("create-store")
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    let before = std::fs::read(&path).unwrap();
    let owner = FileStorage::open_existing(&path).unwrap();
    owner.try_lock_exclusive().unwrap();
    let output = cli()
        .arg("put")
        .arg(&path)
        .args(["1", "must fail"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("already locked")
    );
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(owner);
    assert!(
        cli()
            .arg("put")
            .arg(&path)
            .args(["1", "now works"])
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn database_directory_commands_recover_and_checkpoint_between_processes() {
    let dir = ScratchDir::new();
    let path = dir.0.join("db");
    for args in [
        vec!["create-db"],
        vec!["put", "-42", "Кедр 🌲"],
        vec!["checkpoint"],
        vec!["integrity-check"],
    ] {
        let mut command = cli();
        command.arg(args[0]).arg(&path).args(&args[1..]);
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = cli().arg("get").arg(&path).arg("-42").output().unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "Кедр 🌲\n");
    assert!(
        !cli()
            .arg("create-db")
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    let output = cli().arg("get").arg(&path).arg("-42").output().unwrap();
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "Кедр 🌲\n");
    assert!(
        cli()
            .arg("delete")
            .arg(&path)
            .arg("-42")
            .output()
            .unwrap()
            .status
            .success()
    );
}

#[test]
fn local_sql_commands_persist_and_raw_commands_cannot_damage_sql_namespace() {
    let root = ScratchDir::new();
    let path = root.0.join("sql-db");
    assert!(
        cli()
            .arg("create-db")
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    for query in [
        "CREATE TABLE t (id BIGINT PRIMARY KEY,s TEXT)",
        "INSERT INTO t (id,s) VALUES (1,'Кедр')",
    ] {
        let output = cli().arg("sql").arg(&path).arg(query).output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let output = cli()
        .arg("sql")
        .arg(&path)
        .arg("SELECT id,s FROM t ORDER BY id")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("\"rows\":[[1,\"Кедр\"]]")
    );
    for command in ["put", "get", "delete"] {
        let mut cmd = cli();
        cmd.arg(command).arg(&path).arg("1");
        if command == "put" {
            cmd.arg("forbidden");
        }
        let output = cmd.output().unwrap();
        assert!(!output.status.success());
        assert!(
            String::from_utf8(output.stderr)
                .unwrap()
                .contains("raw key/value commands are disabled")
        );
    }
    assert!(
        cli()
            .arg("integrity-check")
            .arg(&path)
            .output()
            .unwrap()
            .status
            .success()
    );
    use std::io::Write;
    let mut child = cli()
        .arg("sql-stream")
        .arg(&path)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(
            b"BEGIN\nUPDATE t SET s='private' WHERE id=1\nROLLBACK\nSELECT s FROM t WHERE id=1\n",
        )
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout)
            .unwrap()
            .lines()
            .last()
            .unwrap(),
        "{\"columns\":[{\"name\":\"s\",\"type\":\"text\"}],\"rows\":[[\"Кедр\"]],\"affected\":1,\"tag\":\"SELECT\"}"
    );
}
