// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error,
    checksum::crc32c,
    page::PAGE_SIZE,
    storage::{StorageIo, write_all_at},
    wal::{PageImage, RECORD_HEADER, SEGMENT_HEADER, WalConfig, WalLog},
};
use kedr_testkit::{Fault, FaultIo, MemoryStorage};
const LIMIT: usize = 16 * 1024 * 1024;
const IMAGE: usize = RECORD_HEADER + 8 + PAGE_SIZE;
const COMMIT: usize = RECORD_HEADER + 32;
fn config() -> WalConfig {
    WalConfig::first([7; 16])
}
fn image(byte: u8) -> PageImage {
    PageImage {
        page_id: 2,
        bytes: [byte; PAGE_SIZE],
    }
}
fn copy(bytes: &[u8]) -> MemoryStorage {
    let mut io = MemoryStorage::new(LIMIT);
    write_all_at(&mut io, 0, bytes).unwrap();
    io.sync().unwrap();
    io
}
fn seal(bytes: &mut [u8]) {
    bytes[40..44].fill(0);
    let crc = crc32c(bytes);
    bytes[40..44].copy_from_slice(&crc.to_le_bytes());
}

#[test]
fn committed_groups_replay_identically_and_append_continues() {
    let mut wal = WalLog::create(MemoryStorage::new(LIMIT), config()).unwrap();
    assert_eq!(wal.commit(1, &[image(1)], 3 * PAGE_SIZE as u64).unwrap(), 2);
    assert_eq!(wal.commit(2, &[image(2)], 3 * PAGE_SIZE as u64).unwrap(), 4);
    let io = wal.into_io();
    let bytes = io.visible_bytes().to_vec();
    for _ in 0..2 {
        let mut applied = Vec::new();
        let (wal, summary) = WalLog::recover(copy(&bytes), config(), |group| {
            applied.push((
                group.transaction_id,
                group.commit_lsn,
                group.images[0].bytes[0],
            ));
            Ok(())
        })
        .unwrap();
        assert_eq!(applied, vec![(1, 2, 1), (2, 4, 2)]);
        assert_eq!(summary.transactions, 2);
        assert_eq!(summary.ignored_tail_bytes, 0);
        assert_eq!(wal.next_transaction_id().unwrap(), 3);
    }
    let (mut wal, _) = WalLog::recover(io, config(), |_| Ok(())).unwrap();
    wal.clear_uncommitted_tail().unwrap();
    assert_eq!(wal.commit(3, &[image(3)], 3 * PAGE_SIZE as u64).unwrap(), 6);
}

#[test]
fn partial_eof_and_uncommitted_images_do_not_apply_and_must_be_cleared() {
    let mut wal = WalLog::create(MemoryStorage::new(LIMIT), config()).unwrap();
    wal.commit(1, &[image(1)], 3 * PAGE_SIZE as u64).unwrap();
    wal.commit(2, &[image(2)], 3 * PAGE_SIZE as u64).unwrap();
    let bytes = wal.into_io().visible_bytes().to_vec();
    let prefix = SEGMENT_HEADER + IMAGE + COMMIT;
    for tail in [
        1,
        7,
        RECORD_HEADER - 1,
        RECORD_HEADER,
        100,
        IMAGE - 1,
        IMAGE,
        IMAGE + 1,
        IMAGE + RECORD_HEADER - 1,
        IMAGE + RECORD_HEADER + 1,
        IMAGE + COMMIT - 1,
    ] {
        let mut applied = Vec::new();
        let (mut wal, summary) = WalLog::recover(copy(&bytes[..prefix + tail]), config(), |g| {
            applied.push(g.transaction_id);
            Ok(())
        })
        .unwrap();
        assert_eq!(applied, vec![1], "tail={tail}");
        assert_eq!(summary.valid_end, prefix as u64);
        assert_eq!(summary.ignored_tail_bytes, tail as u64);
        assert!(matches!(
            wal.commit(2, &[image(9)], 3 * PAGE_SIZE as u64),
            Err(Error::RecoveryRequired)
        ));
        wal.clear_uncommitted_tail().unwrap();
        wal.commit(2, &[image(9)], 3 * PAGE_SIZE as u64).unwrap();
        let mut values = Vec::new();
        WalLog::recover(wal.into_io(), config(), |g| {
            values.push(g.images[0].bytes[0]);
            Ok(())
        })
        .unwrap();
        assert_eq!(values, vec![1, 9]);
    }
}

#[test]
fn damaged_full_records_and_valid_crc_invalid_commits_fail_closed() {
    let mut wal = WalLog::create(MemoryStorage::new(LIMIT), config()).unwrap();
    wal.commit(1, &[image(1)], 3 * PAGE_SIZE as u64).unwrap();
    let bytes = wal.into_io().visible_bytes().to_vec();
    for offset in [
        0,
        8,
        32,
        64,
        SEGMENT_HEADER,
        SEGMENT_HEADER + 40,
        SEGMENT_HEADER + 100,
        SEGMENT_HEADER + IMAGE + 40,
    ] {
        let mut damaged = bytes.clone();
        damaged[offset] ^= 1;
        assert!(
            WalLog::recover(copy(&damaged), config(), |_| Ok(())).is_err(),
            "offset={offset}"
        );
    }
    for offset in [24, 32, 48, 56, 64, 68, 72] {
        let mut damaged = bytes.clone();
        let commit = SEGMENT_HEADER + IMAGE;
        damaged[commit + offset] ^= 1;
        seal(&mut damaged[commit..]);
        assert!(
            WalLog::recover(copy(&damaged), config(), |_| Ok(())).is_err(),
            "valid CRC offset={offset}"
        );
    }
    let mut wrong = config();
    wrong.database_id = [8; 16];
    assert!(WalLog::recover(copy(&bytes), wrong, |_| Ok(())).is_err());
}

#[test]
fn duplicate_images_and_invalid_lengths_fail_before_any_write() {
    let mut wal = WalLog::create(MemoryStorage::new(LIMIT), config()).unwrap();
    assert!(
        wal.commit(1, &[image(1), image(2)], 3 * PAGE_SIZE as u64)
            .is_err()
    );
    assert!(wal.commit(1, &[image(1)], PAGE_SIZE as u64).is_err());
    assert!(wal.commit(1, &[], 3 * PAGE_SIZE as u64).is_err());
    assert!(wal.commit(2, &[image(1)], 3 * PAGE_SIZE as u64).is_err());
    assert!(!wal.is_poisoned());
    wal.commit(1, &[image(1)], 3 * PAGE_SIZE as u64).unwrap();
}

#[test]
fn failed_append_is_poisoned_and_sync_before_error_is_ambiguous() {
    let base = WalLog::create(MemoryStorage::new(LIMIT), config())
        .unwrap()
        .into_io()
        .visible_bytes()
        .to_vec();
    for (call, fault, committed) in [
        (3, Fault::Error(std::io::ErrorKind::StorageFull), false),
        (4, Fault::Error(std::io::ErrorKind::Other), false),
        (5, Fault::Error(std::io::ErrorKind::Other), false),
        (5, Fault::SyncThenError(std::io::ErrorKind::Other), true),
    ] {
        let fault = FaultIo::new(copy(&base), [(call, fault)]).unwrap();
        let (mut wal, _) = WalLog::recover(fault, config(), |_| Ok(())).unwrap();
        assert!(wal.commit(1, &[image(1)], 3 * PAGE_SIZE as u64).is_err());
        assert!(wal.is_poisoned());
        assert!(matches!(
            wal.commit(1, &[image(1)], 3 * PAGE_SIZE as u64),
            Err(Error::RecoveryRequired)
        ));
        let mut io = wal.into_io().into_inner();
        io.crash().unwrap();
        let mut applied = 0;
        WalLog::recover(io, config(), |_| {
            applied += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(applied, usize::from(committed));
    }
}
