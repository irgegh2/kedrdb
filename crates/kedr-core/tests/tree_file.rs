// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use kedr_core::{page::PAGE_SIZE, record::primary_key, storage::FileStorage, tree::BPlusTree};
use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
#[test]
fn persistent_data_exceeds_cache_and_reopens_with_a_single_frame() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::temp_dir().join(format!("kedr-tree-{}-{stamp}", std::process::id()));
    let file = FileStorage::create_new(&path).unwrap();
    let _cleanup = Scratch(path.clone());
    let budget = 4 * PAGE_SIZE;
    let mut tree = BPlusTree::create(file, [9; 16], budget).unwrap();
    for key in 0..1024i64 {
        tree.insert(&primary_key(key), &vec![key as u8; 4096])
            .unwrap();
    }
    assert!(tree.page_count() * PAGE_SIZE as u64 >= 16 * budget as u64);
    tree.checkpoint().unwrap();
    let stats = tree.cache_stats();
    assert!(stats.allocated_bytes <= budget);
    assert!(stats.evictions > 0);
    drop(tree);
    // One frame plus bookkeeping; moving between pages must evict safely.
    let budget = PAGE_SIZE + 128;
    let file = FileStorage::open_existing(&path).unwrap();
    let mut tree = BPlusTree::open(file, budget).unwrap();
    assert_eq!(tree.cache_stats().frames, 1);
    for key in (0..1024i64).rev() {
        assert_eq!(
            tree.get(&primary_key(key)).unwrap(),
            Some(vec![key as u8; 4096])
        );
    }
    assert_eq!(tree.integrity_check().unwrap().records, 1024);
    assert!(tree.cache_stats().allocated_bytes <= budget);
    for key in (0..1024i64).step_by(2) {
        assert!(tree.delete(&primary_key(key)).unwrap());
    }
    tree.checkpoint().unwrap();
    drop(tree);
    let file = FileStorage::open_existing(&path).unwrap();
    let mut tree = BPlusTree::open(file, budget).unwrap();
    assert_eq!(tree.integrity_check().unwrap().records, 512);
    for key in 0..1024i64 {
        assert_eq!(tree.get(&primary_key(key)).unwrap().is_some(), key % 2 == 1);
    }
}
