// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use kedr_core::{
    database::FileDatabase, page::PAGE_SIZE, record::primary_key, storage::FileStorage,
    tree::BPlusTree,
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
const CACHE: usize = 4 * PAGE_SIZE;
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let unique = NEXT.fetch_add(1, Ordering::Relaxed);
        Self(std::env::temp_dir().join(format!("kedr-db-{}-{stamp}-{unique}", std::process::id())))
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn rotations_reopen_and_prevent_raw_page_writes_or_a_second_owner() {
    let root = Scratch::new();
    let mut db = FileDatabase::create(&root.0, [12; 16], CACHE).unwrap();
    assert!(FileDatabase::create(&root.0, [13; 16], CACHE).is_err());
    assert!(FileDatabase::open(&root.0, CACHE).is_err());
    db.set_segment_bytes(32 * 1024).unwrap();
    for key in 0..20i64 {
        db.transaction(|tx| tx.insert(&primary_key(key), &vec![key as u8; 4096]))
            .unwrap();
        assert_eq!(fs::read_dir(root.0.join("wal")).unwrap().count(), 1);
    }
    let lsn = db.last_commit_lsn().unwrap();
    assert!(db.active_segment().unwrap() > 1);
    assert_eq!(db.integrity_check().unwrap().records, 20);
    drop(db);
    assert!(
        BPlusTree::open(
            FileStorage::open_existing(root.0.join("data.kdr")).unwrap(),
            CACHE
        )
        .is_err()
    );
    let mut db = FileDatabase::open(&root.0, CACHE).unwrap();
    assert_eq!(db.last_commit_lsn().unwrap(), lsn);
    for key in 0..20i64 {
        assert_eq!(
            db.get(&primary_key(key)).unwrap(),
            Some(vec![key as u8; 4096])
        );
    }
    db.transaction(|tx| tx.delete(&primary_key(7)).map(|_| ()))
        .unwrap();
    db.checkpoint().unwrap();
    drop(db);
    let mut db = FileDatabase::open(&root.0, CACHE).unwrap();
    assert_eq!(db.integrity_check().unwrap().records, 19);
    assert!(db.get(&primary_key(7)).unwrap().is_none());
}

#[test]
fn incomplete_checkpoint_header_uses_predecessor_but_complete_corruption_fails() {
    for length in [0, 1, 63, 127, 128] {
        let root = Scratch::new();
        let mut db = FileDatabase::create(&root.0, [12; 16], CACHE).unwrap();
        db.transaction(|tx| tx.insert(&primary_key(1), b"acknowledged"))
            .unwrap();
        drop(db);
        fs::write(root.0.join("wal/0000000000000002.wal"), vec![0; length]).unwrap();
        let result = FileDatabase::open(&root.0, CACHE);
        if length < 128 {
            let mut db = result.unwrap();
            assert_eq!(
                db.get(&primary_key(1)).unwrap(),
                Some(b"acknowledged".to_vec())
            );
            assert_eq!(db.active_segment().unwrap(), 1);
        } else {
            assert!(result.is_err());
        }
    }
}

#[test]
fn checkpoint_failure_poisoning_can_be_resolved_only_by_reopen() {
    let root = Scratch::new();
    let mut db = FileDatabase::create(&root.0, [12; 16], CACHE).unwrap();
    db.transaction(|tx| tx.insert(&primary_key(1), b"safe"))
        .unwrap();
    // The next name already exists: exclusive create must fail without overwrite.
    let collision = root.0.join("wal/0000000000000002.wal");
    fs::write(&collision, b"reserved").unwrap();
    assert!(db.checkpoint().is_err());
    assert!(db.get(&primary_key(1)).is_err());
    assert_eq!(fs::read(&collision).unwrap(), b"reserved");
    drop(db);
    let mut db = FileDatabase::open(&root.0, CACHE).unwrap();
    assert_eq!(db.get(&primary_key(1)).unwrap(), Some(b"safe".to_vec()));
}

#[test]
fn identity_corruption_and_symlink_members_are_rejected_without_changes() {
    let root = Scratch::new();
    drop(FileDatabase::create(&root.0, [12; 16], CACHE).unwrap());
    let data = fs::read(root.0.join("data.kdr")).unwrap();
    let mut id = fs::read(root.0.join(".identity")).unwrap();
    id[8] ^= 1;
    fs::write(root.0.join(".identity"), id).unwrap();
    assert!(FileDatabase::open(&root.0, CACHE).is_err());
    assert_eq!(fs::read(root.0.join("data.kdr")).unwrap(), data);
    let root = Scratch::new();
    drop(FileDatabase::create(&root.0, [12; 16], CACHE).unwrap());
    fs::rename(root.0.join("data.kdr"), root.0.join("elsewhere")).unwrap();
    std::os::unix::fs::symlink("elsewhere", root.0.join("data.kdr")).unwrap();
    assert!(FileDatabase::open(&root.0, CACHE).is_err());
}
