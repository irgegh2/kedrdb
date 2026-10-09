// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error,
    page::{MAX_RECORD_SIZE, PAGE_SIZE},
    record::{Value, primary_key, secondary_key},
    storage::StorageIo,
    tree::BPlusTree,
};
use kedr_testkit::{Fault, FaultIo, MemoryStorage};
use std::collections::BTreeMap;
const CACHE: usize = 4 * PAGE_SIZE;
fn tree() -> BPlusTree<MemoryStorage> {
    BPlusTree::create(MemoryStorage::new(64 * 1024 * 1024), [7; 16], CACHE).unwrap()
}

#[test]
fn split_range_update_delete_reopen_and_reuse() {
    let mut tree = tree();
    for key in (0..300i64).rev() {
        tree.insert(&primary_key(key), &vec![key as u8; 1024])
            .unwrap();
    }
    assert!(tree.integrity_check().unwrap().height >= 2);
    assert!(matches!(
        tree.insert(&primary_key(1), b"duplicate"),
        Err(Error::DuplicateKey)
    ));
    tree.replace(&primary_key(1), b"replacement").unwrap();
    tree.replace(&primary_key(-1), b"upsert").unwrap();
    assert_eq!(
        tree.get(&primary_key(1)).unwrap(),
        Some(b"replacement".to_vec())
    );
    let rows = tree
        .range(Some(&primary_key(90)), Some(&primary_key(100)), 100)
        .unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows.first().unwrap().0, primary_key(90));
    assert_eq!(rows.last().unwrap().0, primary_key(99));
    assert_eq!(tree.range(None, None, 5).unwrap().len(), 5);
    assert!(
        tree.range(Some(&primary_key(10)), Some(&primary_key(2)), 5)
            .unwrap()
            .is_empty()
    );
    let grown = tree.page_count();
    for key in -1..300 {
        assert!(tree.delete(&primary_key(key)).unwrap());
    }
    assert!(!tree.delete(&primary_key(5)).unwrap());
    let empty = tree.integrity_check().unwrap();
    assert_eq!(empty.records, 0);
    assert_eq!(empty.tree_pages, 1);
    assert!(empty.free_pages > 0);
    tree.checkpoint().unwrap();
    let mut tree = BPlusTree::open(tree.into_io(), CACHE).unwrap();
    for key in 0..300i64 {
        tree.insert(&primary_key(key), &vec![key as u8; 1024])
            .unwrap();
    }
    assert!(tree.page_count() <= grown);
    tree.checkpoint().unwrap();
    let mut tree = BPlusTree::open(tree.into_io(), CACHE).unwrap();
    assert_eq!(tree.database_id(), [7; 16]);
    assert_eq!(tree.integrity_check().unwrap().records, 300);
    for key in 0..300 {
        assert_eq!(
            tree.get(&primary_key(key)).unwrap(),
            Some(vec![key as u8; 1024])
        );
    }
    assert!(tree.cache_stats().allocated_bytes <= CACHE);
    assert!(tree.cache_stats().evictions > 0);
}

#[test]
fn multi_level_tree_with_variable_length_keys_and_secondary_duplicates() {
    let mut tree = tree();
    let mut expected = BTreeMap::new();
    for pk in 0..1500i64 {
        let text = format!(
            "{:04}-{}",
            pk / 3,
            "x".repeat(if pk % 4 == 0 { 440 } else { 20 })
        );
        let key = secondary_key(&Value::Text(text), pk).unwrap();
        let value = vec![pk as u8; 2048];
        tree.insert(&key, &value).unwrap();
        expected.insert(key, value);
    }
    assert!(tree.integrity_check().unwrap().height >= 3);
    assert_eq!(
        tree.range(None, None, 2000).unwrap(),
        expected.clone().into_iter().collect::<Vec<_>>()
    );
    for key in expected.keys().step_by(3) {
        assert!(tree.delete(key).unwrap());
    }
    let remaining: Vec<_> = expected
        .into_iter()
        .enumerate()
        .filter(|(at, _)| at % 3 != 0)
        .map(|(_, v)| v)
        .collect();
    tree.checkpoint().unwrap();
    let mut tree = BPlusTree::open(tree.into_io(), CACHE).unwrap();
    assert_eq!(tree.range(None, None, 2000).unwrap(), remaining);
    tree.integrity_check().unwrap();
}

#[test]
fn maximum_records_and_logical_failures_preserve_contents() {
    let mut tree = tree();
    for key in [i64::MIN, -1, 0, i64::MAX] {
        tree.insert(&primary_key(key), &[0xab; MAX_RECORD_SIZE])
            .unwrap();
    }
    let before = tree.range(None, None, 10).unwrap();
    assert!(tree.insert(b"", b"x").is_err());
    assert!(tree.insert(&[1; 513], b"x").is_err());
    assert!(
        tree.replace(&primary_key(0), &[0; MAX_RECORD_SIZE + 1])
            .is_err()
    );
    assert_eq!(tree.range(None, None, 10).unwrap(), before);
    tree.integrity_check().unwrap();
    for key in [i64::MIN, -1, 0, i64::MAX] {
        assert!(tree.delete(&primary_key(key)).unwrap());
    }
    tree.integrity_check().unwrap();
}

#[test]
fn header_and_node_corruption_are_detected() {
    let mut tree = tree();
    tree.insert(&primary_key(1), b"record").unwrap();
    tree.checkpoint().unwrap();
    let mut io = tree.into_io();
    let length = io.len().unwrap();
    io.write_at(2 * PAGE_SIZE as u64 + 100, &[0xff]).unwrap();
    assert!(BPlusTree::open(io, CACHE).is_err());
    let mut io = tree_fn_io();
    io.write_at(0, b"BADMAGIC").unwrap();
    io.write_at(PAGE_SIZE as u64, b"BADMAGIC").unwrap();
    assert!(BPlusTree::open(io, CACHE).is_err());
    let mut io = tree_fn_io();
    io.set_len(length - 1).unwrap();
    assert!(BPlusTree::open(io, CACHE).is_err());
}
fn tree_fn_io() -> MemoryStorage {
    tree().into_io()
}

#[test]
fn ambiguous_checkpoint_sync_poison_stops_further_operations() {
    let io = tree_fn_io();
    // open: len + two reads; root read is call 4; no dirty pages, sync is 5.
    let fault = FaultIo::new(io, [(5, Fault::SyncThenError(std::io::ErrorKind::Other))]).unwrap();
    let mut tree = BPlusTree::open(fault, CACHE).unwrap();
    assert!(tree.checkpoint().is_err());
    assert!(matches!(
        tree.get(&primary_key(1)),
        Err(Error::RecoveryRequired)
    ));
    assert!(matches!(
        tree.insert(&primary_key(1), b"x"),
        Err(Error::RecoveryRequired)
    ));
}

#[test]
fn seeded_tree_operations_match_independent_map() {
    let seeds: u64 = std::env::var("KEDR_TREE_SEEDS")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(4);
    let steps: usize = std::env::var("KEDR_TREE_STEPS")
        .ok()
        .map(|s| s.parse().unwrap())
        .unwrap_or(1000);
    for seed in 1..=seeds {
        let mut tree = tree();
        let mut model = BTreeMap::new();
        let mut state = seed;
        for step in 0..steps {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let key = primary_key(((state >> 32) % 512) as i64 - 256).to_vec();
            match (state >> 16) % 5 {
                0 => {
                    let value = vec![state as u8; ((state >> 40) % 512) as usize];
                    let result = tree.insert(&key, &value);
                    if let std::collections::btree_map::Entry::Vacant(entry) = model.entry(key) {
                        result.unwrap();
                        entry.insert(value);
                    } else {
                        assert!(
                            matches!(result, Err(Error::DuplicateKey)),
                            "seed={seed} step={step}"
                        );
                    }
                }
                1 => {
                    let value = vec![state as u8; ((state >> 40) % 512) as usize];
                    tree.replace(&key, &value).unwrap();
                    model.insert(key, value);
                }
                2 => assert_eq!(
                    tree.delete(&key).unwrap(),
                    model.remove(&key).is_some(),
                    "seed={seed} step={step}"
                ),
                3 => assert_eq!(
                    tree.get(&key).unwrap(),
                    model.get(&key).cloned(),
                    "seed={seed} step={step}"
                ),
                _ => {
                    let expected: Vec<_> = model
                        .range(key.clone()..)
                        .take(20)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    assert_eq!(
                        tree.range(Some(&key), None, 20).unwrap(),
                        expected,
                        "seed={seed} step={step}"
                    );
                }
            }
            if step % 257 == 0 {
                assert_eq!(
                    tree.integrity_check().unwrap().records,
                    model.len() as u64,
                    "seed={seed} step={step}"
                );
                tree.checkpoint().unwrap();
                tree = BPlusTree::open(tree.into_io(), CACHE).unwrap();
                assert_eq!(
                    tree.range(None, None, 1000).unwrap(),
                    model
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect::<Vec<_>>()
                );
            }
        }
        tree.checkpoint().unwrap();
        let mut tree = BPlusTree::open(tree.into_io(), CACHE).unwrap();
        tree.integrity_check().unwrap();
        assert_eq!(
            tree.range(None, None, 1000).unwrap(),
            model.into_iter().collect::<Vec<_>>(),
            "seed={seed}"
        );
        if seeds >= 20 && seed % 20 == 0 {
            eprintln!("tree campaign: {seed}/{seeds} seeds, {steps} operations per seed");
        }
    }
}
