// SPDX-License-Identifier: Apache-2.0
//! Offline, bounded copies. Completion manifests are written last, after file/directory sync.
use kedr_core::{
    Error, Operation, Result,
    checksum::crc32c,
    database::FileDatabase,
    engine::inspect_checkpoint,
    storage::{FileStorage, StorageIo},
    wal::{SEGMENT_HEADER, WalLog},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt},
    path::Path,
    time::Instant,
};
const CACHE: usize = 4 * kedr_core::page::PAGE_SIZE;
const MAX_FILE: u64 = 64 * 1024 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    path: String,
    bytes: u64,
    sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    backup_version: u32,
    data_format: u32,
    page_format: u32,
    wal_format: u32,
    database_id: String,
    checkpoint_lsn: u64,
    segment_id: u64,
    records: u64,
    files: Vec<Member>,
}
fn invalid(message: &'static str) -> Error {
    Error::InvalidArgument { message }
}
fn io(error: std::io::Error) -> Error {
    Error::io(Operation::Open, error)
}
pub(crate) fn sync_dir(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::io(Operation::Sync, e))
}
pub(crate) fn new_dir(path: &Path) -> Result<()> {
    fs::DirBuilder::new().mode(0o700).create(path).map_err(io)?;
    sync_dir(
        path.parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )
}
pub(crate) fn new_file(path: &Path) -> Result<File> {
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(io)
}
pub(crate) fn regular(path: &Path) -> Result<File> {
    let before = fs::symlink_metadata(path).map_err(io)?;
    if !before.is_file() {
        return Err(invalid(
            "maintenance input must be a regular file, not a symlink",
        ));
    }
    let file = File::open(path).map_err(io)?;
    let after = file.metadata().map_err(io)?;
    if before.dev() != after.dev() || before.ino() != after.ino() || !after.is_file() {
        return Err(invalid("input changed while opening"));
    }
    Ok(file)
}
pub(crate) fn digest(mut file: File) -> Result<(u64, String)> {
    let mut hash = Sha256::new();
    let mut count = 0u64;
    let mut buffer = [0; 65536];
    loop {
        let n = file.read(&mut buffer).map_err(io)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .ok_or(invalid("file size overflow"))?;
        if count > MAX_FILE {
            return Err(invalid("maintenance file exceeds 64 GiB"));
        }
        hash.update(&buffer[..n]);
    }
    Ok((count, format!("{:x}", hash.finalize())))
}
fn copy(from: &Path, to: &Path) -> Result<Member> {
    let mut source = regular(from)?;
    let mut target = new_file(to)?;
    let mut buffer = [0; 65536];
    let mut hash = Sha256::new();
    let mut count = 0u64;
    loop {
        let n = source.read(&mut buffer).map_err(io)?;
        if n == 0 {
            break;
        }
        count = count
            .checked_add(n as u64)
            .ok_or(invalid("file size overflow"))?;
        if count > MAX_FILE {
            return Err(invalid("maintenance file exceeds 64 GiB"));
        }
        target.write_all(&buffer[..n]).map_err(io)?;
        hash.update(&buffer[..n]);
    }
    target.sync_all().map_err(io)?;
    Ok(Member {
        path: String::new(),
        bytes: count,
        sha256: format!("{:x}", hash.finalize()),
    })
}
fn directory(path: &Path) -> Result<()> {
    if !fs::symlink_metadata(path).map_err(io)?.is_dir() {
        return Err(invalid("maintenance path must be a real directory"));
    }
    Ok(())
}
fn destination(source: &Path, target: &Path) -> Result<()> {
    match fs::symlink_metadata(target) {
        Ok(_) => return Err(invalid("destination must not exist")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(io(e)),
    }
    let parent = target
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = fs::canonicalize(parent).map_err(io)?;
    let source = fs::canonicalize(source).map_err(io)?;
    if parent.starts_with(source) {
        return Err(invalid("destination must be outside the source directory"));
    }
    Ok(())
}
fn members(root: &Path, expected: &[&str]) -> Result<()> {
    let mut actual = fs::read_dir(root)
        .map_err(io)?
        .take(expected.len() + 1)
        .map(|e| e.map(|e| e.file_name().into_string().unwrap_or_default()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(io)?;
    actual.sort();
    let mut expected = expected.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    expected.sort();
    if actual != expected {
        return Err(invalid("backup contains unexpected or missing members"));
    }
    Ok(())
}
fn load(root: &Path) -> Result<Manifest> {
    directory(root)?;
    directory(&root.join("wal"))?;
    members(root, &[".identity", "data.kdr", "wal", "manifest.json"])?;
    let mut bytes = Vec::new();
    regular(&root.join("manifest.json"))?
        .take(16385)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    if bytes.len() > 16384 {
        return Err(invalid("backup manifest exceeds 16 KiB"));
    }
    let m: Manifest =
        serde_json::from_slice(&bytes).map_err(|_| invalid("invalid backup manifest"))?;
    if m.backup_version != 1
        || m.data_format != 2
        || m.page_format != 1
        || m.wal_format != 1
        || m.segment_id == 0
        || m.files.len() != 3
        || m.database_id.len() != 32
        || !m
            .database_id
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        || m.database_id.bytes().all(|c| c == b'0')
    {
        return Err(invalid("unsupported backup format or identity"));
    }
    let wal = format!("wal/{:016x}.wal", m.segment_id);
    let expected = [".identity", "data.kdr", wal.as_str()];
    for (e, name) in m.files.iter().zip(expected) {
        if e.path != name
            || e.bytes > MAX_FILE
            || e.sha256.len() != 64
            || !e
                .sha256
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return Err(invalid("invalid backup member/path/hash"));
        }
    }
    members(&root.join("wal"), &[&format!("{:016x}.wal", m.segment_id)])?;
    Ok(m)
}
fn verify(root: &Path, m: &Manifest) -> Result<()> {
    for entry in &m.files {
        let (bytes, hash) = digest(regular(&root.join(&entry.path))?)?;
        if bytes != entry.bytes || hash != entry.sha256 {
            return Err(invalid("backup checksum/length mismatch"));
        }
    }
    let mut identity = Vec::new();
    regular(&root.join(".identity"))?
        .take(33)
        .read_to_end(&mut identity)
        .map_err(io)?;
    if identity.len() != 32 || &identity[..8] != b"KEDRID01" {
        return Err(invalid("backup identity format"));
    }
    let crc = u32::from_le_bytes(identity[24..28].try_into().unwrap());
    identity[24..28].fill(0);
    let id = identity[8..24]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    if id != m.database_id || identity[28..].iter().any(|b| *b != 0) || crc32c(&identity) != crc {
        return Err(invalid("backup identity mismatch"));
    }
    let (database_id, checkpoint_lsn, report) =
        inspect_checkpoint(FileStorage::open_readonly(root.join("data.kdr"))?, CACHE)?;
    if database_id != identity[8..24]
        || checkpoint_lsn != m.checkpoint_lsn
        || report.records != m.records
    {
        return Err(invalid("backup tree/checkpoint mismatch"));
    }
    let mut wal = FileStorage::open_readonly(root.join(&m.files[2].path))?;
    let config = WalLog::read_config(&mut wal)?;
    if wal.len()? != SEGMENT_HEADER as u64
        || config.database_id != database_id
        || config.segment_id != m.segment_id
        || config.previous_commit_lsn != m.checkpoint_lsn
    {
        return Err(invalid("backup WAL boundary mismatch"));
    }
    Ok(())
}
pub fn backup(source: &Path, target: &Path) -> Result<()> {
    destination(source, target)?;
    let began = Instant::now();
    let mut db = FileDatabase::open(source, CACHE)?;
    let report = db.integrity_check()?;
    db.checkpoint()?;
    let info = db.snapshot_info()?;
    // The owner is held until every copied byte and completion marker is durable.
    new_dir(target)?;
    new_dir(&target.join("wal"))?;
    let names = [
        ".identity".to_string(),
        "data.kdr".to_string(),
        format!("wal/{:016x}.wal", info.segment_id),
    ];
    let mut files = Vec::new();
    for (from, path) in info.files.iter().zip(names) {
        let mut entry = copy(from, &target.join(&path))?;
        entry.path = path;
        files.push(entry);
    }
    let m = Manifest {
        backup_version: 1,
        data_format: 2,
        page_format: 1,
        wal_format: 1,
        database_id: info
            .database_id
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
        checkpoint_lsn: info.checkpoint_lsn,
        segment_id: info.segment_id,
        records: report.records,
        files,
    };
    verify(target, &m)?;
    sync_dir(&target.join("wal"))?;
    sync_dir(target)?;
    let mut file = new_file(&target.join(".manifest.pending"))?;
    serde_json::to_writer_pretty(&mut file, &m).map_err(|_| invalid("cannot write manifest"))?;
    file.write_all(b"\n").map_err(io)?;
    file.sync_all().map_err(io)?;
    fs::rename(
        target.join(".manifest.pending"),
        target.join("manifest.json"),
    )
    .map_err(io)?;
    sync_dir(target)?;
    println!(
        "{}",
        serde_json::json!({"event":"backup_complete","database_id":m.database_id,"checkpoint_lsn":m.checkpoint_lsn,"bytes":m.files.iter().map(|f|f.bytes).sum::<u64>(),"elapsed_ms":began.elapsed().as_millis()})
    );
    Ok(())
}
pub fn check_backup(root: &Path) -> Result<()> {
    let m = load(root)?;
    verify(root, &m)?;
    println!(
        "Backup verified: {} records, checkpoint LSN {}.",
        m.records, m.checkpoint_lsn
    );
    Ok(())
}
pub fn restore(source: &Path, target: &Path) -> Result<()> {
    destination(source, target)?;
    let m = load(source)?;
    verify(source, &m)?; // No destination mutation until validation passes.
    new_dir(target)?;
    new_dir(&target.join("wal"))?;
    // Preserve the manifest as a quarantine marker until all copied bytes validate.
    let mut marker = new_file(&target.join(".import-incomplete"))?;
    marker
        .write_all(b"physical restore incomplete\n")
        .map_err(io)?;
    marker.sync_all().map_err(io)?;
    sync_dir(target)?;
    for entry in &m.files {
        let copied = copy(&source.join(&entry.path), &target.join(&entry.path))?;
        if copied.bytes != entry.bytes || copied.sha256 != entry.sha256 {
            return Err(invalid("backup changed during restore"));
        }
    }
    new_file(&target.join(".lock"))?.sync_all().map_err(io)?;
    sync_dir(&target.join("wal"))?;
    sync_dir(target)?;
    {
        let mut db = FileDatabase::open_importing(target, CACHE)?;
        let info = db.snapshot_info()?;
        if info
            .database_id
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>()
            != m.database_id
            || info.checkpoint_lsn != m.checkpoint_lsn
            || db.integrity_check()?.records != m.records
        {
            return Err(invalid("restored database does not match manifest"));
        }
    }
    fs::remove_file(target.join(".import-incomplete")).map_err(io)?;
    sync_dir(target)?;
    println!("Restored and verified into a new database directory.");
    Ok(())
}
pub fn diagnostics(source: &Path, target: &Path) -> Result<()> {
    let mut db = FileDatabase::open(source, CACHE)?;
    let info = db.snapshot_info()?;
    let report = db.integrity_check()?;
    let value = serde_json::json!({"diagnostic_version":1,"server_version":env!("CARGO_PKG_VERSION"),"platform":std::env::consts::OS,"arch":std::env::consts::ARCH,"database_id":info.database_id.iter().map(|b|format!("{b:02x}")).collect::<String>(),"data_format":2,"page_format":1,"wal_format":1,"checkpoint_lsn":info.checkpoint_lsn,"segment_id":info.segment_id,"records":report.records,"tree_pages":report.tree_pages,"free_pages":report.free_pages,"height":report.height,"data_bytes":regular(&info.files[1])?.metadata().map_err(io)?.len(),"wal_bytes":regular(&info.files[2])?.metadata().map_err(io)?.len()});
    let mut file = new_file(target)?;
    serde_json::to_writer_pretty(&mut file, &value)
        .map_err(|_| invalid("cannot write diagnostic JSON"))?;
    file.write_all(b"\n").map_err(io)?;
    file.sync_all().map_err(io)?;
    sync_dir(
        target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    println!("Diagnostic metadata saved; no SQL, table names, rows, credentials or keys included.");
    Ok(())
}
