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
    assert!(text.contains("SQL server and network protocol: available via kedr-server."));
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

fn success(args: &[&str]) -> String {
    let out = cli().args(args).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap()
}
#[test]
fn physical_backup_manifest_restores_exact_state_and_rejects_damage_without_overwrite() {
    let root = ScratchDir::new();
    let db = root.0.join("source");
    let backup = root.0.join("backup");
    let restored = root.0.join("restored");
    let d = db.to_str().unwrap();
    let b = backup.to_str().unwrap();
    let r = restored.to_str().unwrap();
    success(&["create-db", d]);
    success(&[
        "sql",
        d,
        "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT)",
    ]);
    success(&[
        "sql",
        d,
        "INSERT INTO items (id,name) VALUES (1,'Кедр; '' 🌲')",
    ]);
    let rows = success(&["sql", d, "SELECT * FROM items"]);
    success(&["backup", d, b]);
    success(&["verify-backup", b]);
    success(&["restore", b, r]);
    assert_eq!(rows, success(&["sql", r, "SELECT * FROM items"]));
    assert_eq!(
        std::fs::read(db.join("data.kdr")).unwrap(),
        std::fs::read(restored.join("data.kdr")).unwrap()
    );
    let before = std::fs::read(restored.join("data.kdr")).unwrap();
    assert!(!cli().args(["restore", b, r]).status().unwrap().success());
    assert_eq!(before, std::fs::read(restored.join("data.kdr")).unwrap());
    let m = std::fs::read(backup.join("manifest.json")).unwrap();
    let j: serde_json::Value = serde_json::from_slice(&m).unwrap();
    assert_eq!(j["files"].as_array().unwrap().len(), 3);
    let mut corrupt = std::fs::read(backup.join("data.kdr")).unwrap();
    corrupt[100] ^= 1;
    std::fs::write(backup.join("data.kdr"), corrupt).unwrap();
    let rejected = root.0.join("rejected");
    assert!(
        !cli()
            .args(["restore", b, rejected.to_str().unwrap()])
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(!rejected.exists());
    let mut j: serde_json::Value = serde_json::from_slice(&m).unwrap();
    j["files"][0]["path"] = serde_json::json!("../source/.identity");
    std::fs::write(
        backup.join("manifest.json"),
        serde_json::to_vec(&j).unwrap(),
    )
    .unwrap();
    assert!(
        !cli()
            .args(["verify-backup", b])
            .output()
            .unwrap()
            .status
            .success()
    );
}
#[test]
fn canonical_sql_roundtrip_preserves_quotes_null_types_dependencies_and_self_cycles() {
    let root = ScratchDir::new();
    let db = root.0.join("source");
    let target = root.0.join("target");
    let export = root.0.join("export.sql");
    let again = root.0.join("again.sql");
    let d = db.to_str().unwrap();
    let t = target.to_str().unwrap();
    let e = export.to_str().unwrap();
    success(&["create-db", d]);
    for statement in [
        "CREATE TABLE zparent (id BIGINT PRIMARY KEY, name TEXT UNIQUE)",
        "CREATE TABLE achild (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES zparent (id), n INT, active BOOLEAN, message TEXT)",
        "CREATE INDEX my_index ON achild (parent)",
        "CREATE TABLE selfref (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES selfref (id))",
        "INSERT INTO zparent (id,name) VALUES (-9223372036854775808,'Кедр')",
        "INSERT INTO achild (id,parent,n,active,message) VALUES (9,-9223372036854775808,-2147483648,NULL,'one; ''two'' \\ three\n-- KedrDB SQL export end\n🌲')",
        "INSERT INTO selfref (id,parent) VALUES (1,2),(2,1)",
        "ALTER TABLE achild ADD COLUMN later TEXT",
    ] {
        success(&["sql", d, statement]);
    }
    success(&["export-sql", d, e]);
    success(&["import-sql", e, t]);
    success(&["export-sql", t, again.to_str().unwrap()]);
    assert_eq!(
        std::fs::read(export).unwrap(),
        std::fs::read(again).unwrap()
    );
    for query in [
        "SELECT * FROM zparent ORDER BY id",
        "SELECT * FROM achild ORDER BY id",
        "SELECT * FROM selfref ORDER BY id",
    ] {
        assert_eq!(success(&["sql", d, query]), success(&["sql", t, query]));
    }
}
#[test]
fn incomplete_incompatible_import_and_backup_never_publish_or_overwrite() {
    let root = ScratchDir::new();
    let input = root.0.join("broken.sql");
    let target = root.0.join("target");
    std::fs::write(
        &input,
        "-- KedrDB SQL export 1\nCREATE TABLE x (id BIGINT PRIMARY KEY);\n",
    )
    .unwrap();
    assert!(
        !cli()
            .arg("import-sql")
            .arg(&input)
            .arg(&target)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(!target.exists());
    std::fs::write(
        &input,
        "-- KedrDB SQL export 1\nDROP TABLE x;\n-- KedrDB SQL export end\n",
    )
    .unwrap();
    assert!(
        !cli()
            .arg("import-sql")
            .arg(&input)
            .arg(&target)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(!target.exists());
    success(&["create-db", target.to_str().unwrap()]);
    std::fs::write(
        target.join(".import-incomplete"),
        "physical restore incomplete\n",
    )
    .unwrap();
    assert!(
        !cli()
            .arg("sql")
            .arg(&target)
            .arg("SHOW TABLES")
            .output()
            .unwrap()
            .status
            .success()
    );
    let copy = root.0.join("copy");
    assert!(
        !cli()
            .arg("backup")
            .arg(&target)
            .arg(&copy)
            .output()
            .unwrap()
            .status
            .success()
    );
    assert!(!copy.exists());
}

#[test]
fn killed_import_resumes_same_job_without_duplicates_and_rejects_changed_source() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;
    use std::time::Duration;
    let dir = ScratchDir::new();
    let input = dir.0.join("input.sql");
    let target = dir.0.join("target");
    let mut dump = String::from(
        "-- KedrDB SQL export 1\nCREATE TABLE \"items\" (\"id\" BIGINT PRIMARY KEY, \"name\" TEXT);\n",
    );
    for i in 0..1024 {
        dump.push_str(&format!(
            "INSERT INTO \"items\" (\"id\", \"name\") VALUES ({i}, 'row {i}');\n"
        ));
    }
    dump.push_str("-- KedrDB SQL export end\n");
    std::fs::write(&input, &dump).unwrap();
    let mut child = cli()
        .arg("import-sql")
        .arg(&input)
        .arg(&target)
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else {
                break;
            };
            if sender.send(line).is_err() {
                break;
            }
        }
    });
    let progress = receiver.recv_timeout(Duration::from_secs(30)).unwrap();
    assert!(progress.contains("import_progress"));
    child.kill().unwrap();
    child.wait().unwrap();
    drop(receiver);
    reader.join().unwrap();
    assert!(target.join(".import-incomplete").exists());
    assert!(
        !cli()
            .args(["sql", target.to_str().unwrap(), "SELECT * FROM items"])
            .status()
            .unwrap()
            .success()
    );
    let changed = dir.0.join("changed.sql");
    std::fs::write(&changed, dump.replace("row 7", "changed 7")).unwrap();
    assert!(
        !cli()
            .arg("resume-import")
            .arg(changed)
            .arg(&target)
            .status()
            .unwrap()
            .success()
    );
    success(&[
        "resume-import",
        input.to_str().unwrap(),
        target.to_str().unwrap(),
    ]);
    assert!(!target.join(".import-incomplete").exists());
    let output = dir.0.join("output.sql");
    success(&[
        "export-sql",
        target.to_str().unwrap(),
        output.to_str().unwrap(),
    ]);
    assert_eq!(
        std::fs::read(input).unwrap(),
        std::fs::read(output).unwrap()
    );
}

#[test]
fn backup_refuses_live_owner_unknown_format_missing_wal_and_symlink_members() {
    use kedr_core::{database::FileDatabase, page::PAGE_SIZE};
    use std::os::unix::fs::symlink;
    let dir = ScratchDir::new();
    let source = dir.0.join("db");
    let backup = dir.0.join("backup");
    let db = FileDatabase::create(&source, [71; 16], 4 * PAGE_SIZE).unwrap();
    assert!(
        !cli()
            .arg("backup")
            .arg(&source)
            .arg(&backup)
            .status()
            .unwrap()
            .success()
    );
    assert!(!backup.exists());
    drop(db);
    success(&["backup", source.to_str().unwrap(), backup.to_str().unwrap()]);
    let manifest = backup.join("manifest.json");
    let bytes = std::fs::read(&manifest).unwrap();
    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    value["data_format"] = 99.into();
    std::fs::write(&manifest, serde_json::to_vec(&value).unwrap()).unwrap();
    assert!(
        !cli()
            .arg("verify-backup")
            .arg(&backup)
            .status()
            .unwrap()
            .success()
    );
    std::fs::write(&manifest, &bytes).unwrap();
    let data = backup.join("data.kdr");
    std::fs::remove_file(&data).unwrap();
    symlink(source.join("data.kdr"), &data).unwrap();
    assert!(
        !cli()
            .arg("verify-backup")
            .arg(&backup)
            .status()
            .unwrap()
            .success()
    );
    std::fs::remove_file(&data).unwrap();
    std::fs::copy(source.join("data.kdr"), &data).unwrap();
    let wal = std::fs::read_dir(backup.join("wal"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    std::fs::remove_file(wal).unwrap();
    assert!(
        !cli()
            .arg("restore")
            .arg(&backup)
            .arg(dir.0.join("restore"))
            .status()
            .unwrap()
            .success()
    );
    assert!(!dir.0.join("restore").exists());
    symlink(dir.0.join("missing"), source.join(".import-incomplete")).unwrap();
    assert!(
        !cli()
            .arg("integrity-check")
            .arg(source)
            .status()
            .unwrap()
            .success()
    );
}
