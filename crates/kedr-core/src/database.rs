// SPDX-License-Identifier: Apache-2.0
//! Local Unix database directory, ownership and checkpoint/WAL lifecycle.
use crate::{
    Error, Operation, Result,
    checksum::crc32c,
    engine::{StorageEngine, StorageTransaction},
    storage::{FileStorage, StorageIo, read_exact_at, write_all_at},
    tree::IntegrityReport,
    wal::{SEGMENT_HEADER, WalConfig, WalLog},
};
use std::{
    fs,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
};

type Engine = StorageEngine<FileStorage, FileStorage>;
pub const DEFAULT_SEGMENT_BYTES: u64 = 128 * 1024 * 1024;
const MAX_SEGMENTS: usize = 1024;

pub struct FileDatabase {
    root: PathBuf,
    _ownership: FileStorage,
    engine: Option<Engine>,
    cache_budget: usize,
    segment_bytes: u64,
}
impl FileDatabase {
    /// The parent directory must already exist. Existing paths are preserved.
    /// A failed creation leaves a diagnostic incomplete directory, never deletes it.
    pub fn create(path: impl AsRef<Path>, id: [u8; 16], cache_budget: usize) -> Result<Self> {
        if id == [0; 16] {
            return Err(Error::InvalidArgument {
                message: "zero database identity",
            });
        }
        let path = path.as_ref();
        fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .map_err(open_error)?;
        let root = fs::canonicalize(path).map_err(open_error)?;
        sync_directory(root.parent().ok_or(Error::InvalidArgument {
            message: "database requires parent directory",
        })?)?;
        let mut ownership = FileStorage::create_new(root.join(".lock"))?;
        ownership.try_lock_exclusive()?;
        ownership.sync()?;
        sync_directory(&root)?;
        let mut identity = FileStorage::create_new(root.join(".identity"))?;
        let mut bytes = [0; 32];
        bytes[..8].copy_from_slice(b"KEDRID01");
        bytes[8..24].copy_from_slice(&id);
        let crc = crc32c(&bytes);
        bytes[24..28].copy_from_slice(&crc.to_le_bytes());
        write_all_at(&mut identity, 0, &bytes)?;
        identity.sync()?;
        let data = FileStorage::create_new(root.join("data.kdr"))?;
        data.try_lock_exclusive()?;
        fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join("wal"))
            .map_err(open_error)?;
        sync_directory(&root)?;
        let wal = FileStorage::create_new(segment_path(&root, 1))?;
        wal.try_lock_exclusive()?;
        // Persist all names before the first commit can be acknowledged.
        sync_directory(&root.join("wal"))?;
        let engine = Engine::create(data, wal, id, cache_budget)?;
        Ok(Self {
            root,
            _ownership: ownership,
            engine: Some(engine),
            cache_budget,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
        })
    }
    pub fn open(path: impl AsRef<Path>, cache_budget: usize) -> Result<Self> {
        require_directory(path.as_ref())?;
        let root = fs::canonicalize(path).map_err(open_error)?;
        let ownership = regular_file(&root.join(".lock"))?;
        ownership.try_lock_exclusive()?;
        let id = read_identity(&root)?;
        require_directory(&root.join("wal"))?;
        let data = regular_file(&root.join("data.kdr"))?;
        data.try_lock_exclusive()?;
        let mut segments = list_segments(&root)?;
        let mut last = *segments.last().ok_or(Error::CorruptWal {
            reason: "missing WAL segment",
        })?;
        let mut wal = regular_file(&segment_path(&root, last))?;
        wal.try_lock_exclusive()?;
        if wal.len()? < SEGMENT_HEADER as u64 && segments.len() > 1 {
            // Checkpoint created a new name but did not finish its header. Its
            // predecessor has not been removed before a durable complete header.
            drop(wal);
            fs::remove_file(segment_path(&root, last)).map_err(open_error)?;
            sync_directory(&root.join("wal"))?;
            segments.pop();
            last = *segments.last().expect("predecessor");
            wal = regular_file(&segment_path(&root, last))?;
            wal.try_lock_exclusive()?;
        }
        let config = WalLog::read_config(&mut wal)?;
        if config.database_id != id || config.segment_id != last {
            return Err(Error::CorruptWal {
                reason: "segment filename/database identity mismatch",
            });
        }
        let (engine, _) = Engine::recover(data, wal, config, cache_budget)?;
        let mut db = Self {
            root,
            _ownership: ownership,
            engine: Some(engine),
            cache_budget,
            segment_bytes: DEFAULT_SEGMENT_BYTES,
        };
        db.prune(last)?;
        Ok(db)
    }
    pub fn set_segment_bytes(&mut self, bytes: u64) -> Result<()> {
        if !(32 * 1024..=DEFAULT_SEGMENT_BYTES).contains(&bytes) {
            return Err(Error::InvalidArgument {
                message: "segment threshold must be 32 KiB..128 MiB",
            });
        }
        self.segment_bytes = bytes;
        Ok(())
    }
    fn engine(&mut self) -> Result<&mut Engine> {
        let engine = self.engine.as_mut().ok_or(Error::RecoveryRequired)?;
        if engine.is_poisoned() {
            return Err(Error::RecoveryRequired);
        }
        Ok(engine)
    }
    pub fn transaction<T>(
        &mut self,
        operation: impl FnOnce(&mut StorageTransaction<'_, FileStorage, FileStorage>) -> Result<T>,
    ) -> Result<T> {
        let result = self.engine()?.transaction(operation)?;
        if self.engine()?.wal_length() >= self.segment_bytes {
            self.checkpoint().map_err(|source| Error::CommitUncertain {
                source: Box::new(source),
            })?;
        }
        Ok(result)
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.engine()?.get(key)
    }
    pub fn integrity_check(&mut self) -> Result<IntegrityReport> {
        self.engine()?.integrity_check()
    }
    pub fn last_commit_lsn(&mut self) -> Result<u64> {
        Ok(self.engine()?.last_commit_lsn())
    }
    pub fn active_segment(&mut self) -> Result<u64> {
        Ok(self.engine()?.wal_config().segment_id)
    }
    /// The engine remains unavailable after any checkpoint error until reopen.
    pub fn checkpoint(&mut self) -> Result<()> {
        self.checkpoint_with(&mut SystemCheckpoint)
    }
    fn checkpoint_with(&mut self, filesystem: &mut impl CheckpointIo) -> Result<()> {
        self.engine()?;
        let engine = self.engine.take().expect("healthy engine");
        let (mut data, old) = engine.into_parts();
        let config =
            WalConfig {
                database_id: old.config().database_id,
                segment_id: old.config().segment_id.checked_add(1).ok_or(
                    Error::InvalidArgument {
                        message: "segment id exhausted",
                    },
                )?,
                first_lsn: old.next_record_lsn(),
                previous_transaction_id: old.next_transaction_id()? - 1,
                previous_commit_lsn: old.last_commit_lsn(),
            };
        // Every applied page and both metadata copies must precede the new base.
        filesystem.sync_data(&mut data)?;
        let io = filesystem.create_segment(&segment_path(&self.root, config.segment_id))?;
        io.try_lock_exclusive()?;
        let wal = filesystem.initialize_segment(io, config)?;
        filesystem.sync_directory(&self.root.join("wal"))?;
        drop(old);
        // Retention is pruned only after the complete successor and data are durable.
        self.prune_with(config.segment_id, filesystem)?;
        self.engine = Some(Engine::resume(data, wal, self.cache_budget));
        Ok(())
    }
    fn prune(&mut self, active: u64) -> Result<()> {
        self.prune_with(active, &mut SystemCheckpoint)
    }
    fn prune_with(&mut self, active: u64, filesystem: &mut impl CheckpointIo) -> Result<()> {
        for id in list_segments(&self.root)? {
            if id < active {
                filesystem.remove_segment(&segment_path(&self.root, id))?;
            }
        }
        filesystem.sync_directory(&self.root.join("wal"))
    }
}
// Private seam for fault tests; the public database always uses real OS calls.
trait CheckpointIo {
    fn sync_data(&mut self, data: &mut FileStorage) -> Result<()>;
    fn create_segment(&mut self, path: &Path) -> Result<FileStorage>;
    fn initialize_segment(
        &mut self,
        io: FileStorage,
        config: WalConfig,
    ) -> Result<WalLog<FileStorage>>;
    fn sync_directory(&mut self, path: &Path) -> Result<()>;
    fn remove_segment(&mut self, path: &Path) -> Result<()>;
}
struct SystemCheckpoint;
impl CheckpointIo for SystemCheckpoint {
    fn sync_data(&mut self, data: &mut FileStorage) -> Result<()> {
        data.sync()
    }
    fn create_segment(&mut self, path: &Path) -> Result<FileStorage> {
        FileStorage::create_new(path)
    }
    fn initialize_segment(
        &mut self,
        io: FileStorage,
        config: WalConfig,
    ) -> Result<WalLog<FileStorage>> {
        WalLog::create(io, config)
    }
    fn sync_directory(&mut self, path: &Path) -> Result<()> {
        sync_directory(path)
    }
    fn remove_segment(&mut self, path: &Path) -> Result<()> {
        fs::remove_file(path).map_err(open_error)
    }
}

fn segment_path(root: &Path, id: u64) -> PathBuf {
    root.join("wal").join(format!("{id:016x}.wal"))
}
fn open_error(source: std::io::Error) -> Error {
    Error::io(Operation::Open, source)
}
fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| Error::io(Operation::Sync, source))
}
fn require_directory(path: &Path) -> Result<()> {
    if !fs::symlink_metadata(path)
        .map_err(open_error)?
        .file_type()
        .is_dir()
    {
        return Err(Error::CorruptStore {
            reason: "database path must be a real directory",
        });
    }
    Ok(())
}
fn regular_file(path: &Path) -> Result<FileStorage> {
    if !fs::symlink_metadata(path)
        .map_err(open_error)?
        .file_type()
        .is_file()
    {
        return Err(Error::CorruptStore {
            reason: "database member must be a regular file",
        });
    }
    FileStorage::open_existing(path)
}
fn read_identity(root: &Path) -> Result<[u8; 16]> {
    let mut file = regular_file(&root.join(".identity"))?;
    if file.len()? != 32 {
        return Err(Error::CorruptStore {
            reason: "identity length",
        });
    }
    let mut bytes = [0; 32];
    read_exact_at(&mut file, 0, &mut bytes)?;
    let stored = u32::from_le_bytes(bytes[24..28].try_into().expect("width"));
    bytes[24..28].fill(0);
    let id = bytes[8..24].try_into().expect("width");
    if &bytes[..8] != b"KEDRID01"
        || bytes[28..].iter().any(|v| *v != 0)
        || crc32c(&bytes) != stored
        || id == [0; 16]
    {
        return Err(Error::CorruptStore {
            reason: "identity checksum/format",
        });
    }
    Ok(id)
}
fn list_segments(root: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(root.join("wal")).map_err(open_error)? {
        let entry = entry.map_err(open_error)?;
        let name = entry.file_name();
        let name = name.to_str().ok_or(Error::CorruptWal {
            reason: "segment filename encoding",
        })?;
        let id = name
            .strip_suffix(".wal")
            .filter(|s| {
                s.len() == 16
                    && s.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            .and_then(|s| u64::from_str_radix(s, 16).ok())
            .filter(|v| *v != 0)
            .ok_or(Error::CorruptWal {
                reason: "unexpected WAL directory entry",
            })?;
        if !entry.file_type().map_err(open_error)?.is_file() {
            return Err(Error::CorruptWal {
                reason: "segment must be regular file",
            });
        }
        if ids.len() == MAX_SEGMENTS {
            return Err(Error::LimitExceeded {
                limit: MAX_SEGMENTS,
                requested: MAX_SEGMENTS as u64 + 1,
            });
        }
        ids.push(id);
    }
    ids.sort_unstable();
    if ids
        .windows(2)
        .any(|pair| pair[0].checked_add(1) != Some(pair[1]))
    {
        return Err(Error::CorruptWal {
            reason: "gap in retained segment suffix",
        });
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{page::PAGE_SIZE, record::primary_key};
    use std::time::{SystemTime, UNIX_EPOCH};
    struct FaultCheckpoint {
        cut: usize,
        after: bool,
        calls: usize,
    }
    impl FaultCheckpoint {
        fn run<T>(&mut self, operation: impl FnOnce() -> Result<T>) -> Result<T> {
            self.calls += 1;
            let fault = self.calls == self.cut;
            let fail = || {
                Error::io(
                    Operation::Sync,
                    std::io::Error::other("checkpoint injected fault"),
                )
            };
            if fault && !self.after {
                return Err(fail());
            }
            let result = operation();
            if fault { Err(fail()) } else { result }
        }
    }
    impl CheckpointIo for FaultCheckpoint {
        fn sync_data(&mut self, data: &mut FileStorage) -> Result<()> {
            self.run(|| data.sync())
        }
        fn create_segment(&mut self, path: &Path) -> Result<FileStorage> {
            self.run(|| FileStorage::create_new(path))
        }
        fn initialize_segment(
            &mut self,
            io: FileStorage,
            config: WalConfig,
        ) -> Result<WalLog<FileStorage>> {
            self.run(|| WalLog::create(io, config))
        }
        fn sync_directory(&mut self, path: &Path) -> Result<()> {
            self.run(|| sync_directory(path))
        }
        fn remove_segment(&mut self, path: &Path) -> Result<()> {
            self.run(|| fs::remove_file(path).map_err(open_error))
        }
    }
    struct Scratch(PathBuf);
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    #[test]
    fn every_checkpoint_lifecycle_step_before_and_after_failure_preserves_acknowledgements() {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        // Six lifecycle calls: data sync, create, header sync, name sync,
        // predecessor removal, deletion sync. Test actual filesystem effects.
        for cut in 1..=6 {
            for after in [false, true] {
                let root = Scratch(std::env::temp_dir().join(format!(
                    "kedr-checkpoint-{}-{stamp}-{cut}-{after}",
                    std::process::id()
                )));
                let mut db = FileDatabase::create(&root.0, [31; 16], 4 * PAGE_SIZE).unwrap();
                db.transaction(|tx| tx.insert(&primary_key(1), b"acknowledged"))
                    .unwrap();
                let lsn = db.last_commit_lsn().unwrap();
                let mut faults = FaultCheckpoint {
                    cut,
                    after,
                    calls: 0,
                };
                assert!(db.checkpoint_with(&mut faults).is_err());
                assert_eq!(faults.calls, cut);
                assert!(db.get(&primary_key(1)).is_err());
                drop(db);
                for _ in 0..2 {
                    let mut db = FileDatabase::open(&root.0, 4 * PAGE_SIZE).unwrap();
                    assert_eq!(
                        db.get(&primary_key(1)).unwrap(),
                        Some(b"acknowledged".to_vec())
                    );
                    assert_eq!(db.last_commit_lsn().unwrap(), lsn);
                    assert_eq!(db.integrity_check().unwrap().records, 1);
                }
            }
        }
    }
}
