// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error,
    checksum::crc32c,
    page::{HEADER_SIZE, MAX_RECORD_SIZE, PAGE_SIZE, SLOT_SIZE, SlottedPage},
};

fn reseal(bytes: &mut [u8; PAGE_SIZE]) {
    bytes[32..36].fill(0);
    let checksum = crc32c(bytes);
    bytes[32..36].copy_from_slice(&checksum.to_le_bytes());
}

#[test]
fn explicit_little_endian_format_and_roundtrip() {
    let mut page = SlottedPage::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718).unwrap();
    let empty = page.insert(b"").unwrap();
    let record = page.insert(b"hello").unwrap();
    let bytes = page.encode();
    assert_eq!(&bytes[..16], b"KEDRPAGE\x01\x00\x01\x00\x40\x00\x00\x00");
    assert_eq!(&bytes[16..24], &[8, 7, 6, 5, 4, 3, 2, 1]);
    assert_eq!(
        &bytes[24..32],
        &[0x18, 0x17, 0x16, 0x15, 0x14, 0x13, 0x12, 0x11]
    );
    assert_eq!(&bytes[36..42], &[2, 0, 96, 0, 0xfb, 0x3f]);
    assert_eq!(&bytes[64..70], &[0, 0x40, 0, 0, 0, 0]);
    assert_eq!(&bytes[80..86], &[0xfb, 0x3f, 5, 0, 0, 0]);
    let decoded = SlottedPage::decode(&bytes, page.page_id()).unwrap();
    assert_eq!(decoded.page_lsn(), page.page_lsn());
    assert_eq!(decoded.get(empty).unwrap(), Some(b"".as_slice()));
    assert_eq!(decoded.get(record).unwrap(), Some(b"hello".as_slice()));
    assert_eq!(decoded.encode(), bytes);
    assert!(SlottedPage::new(1, 0).is_err());
    assert!(SlottedPage::decode(&bytes, 2).is_err());
}

#[test]
fn record_and_capacity_limits_are_atomic() {
    let mut page = SlottedPage::new(2, 0).unwrap();
    let first = page.insert(&[0xa5; MAX_RECORD_SIZE]).unwrap();
    let before = page.encode();
    assert!(matches!(
        page.insert(&[1; MAX_RECORD_SIZE + 1]),
        Err(Error::RecordTooLarge { .. })
    ));
    assert_eq!(page.encode(), before);
    assert!(matches!(
        page.insert(&[1; MAX_RECORD_SIZE]),
        Err(Error::PageFull)
    ));
    assert_eq!(page.encode(), before);
    let remaining = page.free_bytes() - SLOT_SIZE;
    page.insert(&vec![0x3c; remaining]).unwrap();
    assert_eq!(page.free_bytes(), 0);
    let full = page.encode();
    assert!(matches!(page.insert(b""), Err(Error::PageFull)));
    assert_eq!(page.encode(), full);
    assert_eq!(
        SlottedPage::decode(&full, 2)
            .unwrap()
            .get(first)
            .unwrap()
            .unwrap(),
        &[0xa5; MAX_RECORD_SIZE]
    );
}

#[test]
fn deletion_keeps_other_slots_and_clears_payload() {
    let mut page = SlottedPage::new(2, 0).unwrap();
    let a = page.insert(b"secret-one").unwrap();
    let b = page.insert(b"two").unwrap();
    let free = page.free_bytes();
    assert!(page.delete(a).unwrap());
    assert!(!page.delete(a).unwrap());
    assert_eq!(page.free_bytes(), free);
    assert_eq!(page.get(a).unwrap(), None);
    assert_eq!(page.get(b).unwrap(), Some(b"two".as_slice()));
    let c = page.insert(b"").unwrap();
    assert_eq!(c, 2);
    assert_eq!(page.get(c).unwrap(), Some(b"".as_slice()));
    assert!(matches!(page.get(3), Err(Error::InvalidSlot { .. })));
    let before = page.encode();
    assert!(page.delete(3).is_err());
    assert_eq!(page.encode(), before);
    assert!(before[PAGE_SIZE - 10..].iter().all(|b| *b == 0));
    let decoded = SlottedPage::decode(&before, 2).unwrap();
    assert_eq!(decoded.get(a).unwrap(), None);
    assert_eq!(decoded.get(b).unwrap(), Some(b"two".as_slice()));
}

#[test]
fn corruption_and_truncation_are_rejected() {
    let mut page = SlottedPage::new(2, 0).unwrap();
    page.insert(b"protected bytes").unwrap();
    let bytes = page.encode();
    for length in [0, 7, 63, PAGE_SIZE - 1] {
        assert!(SlottedPage::decode(&bytes[..length], 2).is_err());
    }
    assert!(SlottedPage::decode(&vec![0; PAGE_SIZE + 1], 2).is_err());
    for offset in [0, 8, 16, 24, 32, 36, 64, 128, PAGE_SIZE - 1] {
        let mut corrupted = bytes;
        corrupted[offset] ^= 0x80;
        assert!(
            SlottedPage::decode(&corrupted, 2).is_err(),
            "offset={offset}"
        );
    }
}

#[test]
fn valid_checksum_does_not_bypass_structural_checks() {
    let mut page = SlottedPage::new(2, 0).unwrap();
    page.insert(b"first").unwrap();
    page.insert(b"second").unwrap();
    let base = page.encode();
    let cases: Vec<(usize, Vec<u8>)> = vec![
        (8, 2u16.to_le_bytes().to_vec()),  // unsupported version
        (10, 2u16.to_le_bytes().to_vec()), // unsupported type
        (12, 63u16.to_le_bytes().to_vec()),
        (14, vec![1]),
        (42, vec![1]),
        (44, vec![1]),
        (36, u16::MAX.to_le_bytes().to_vec()),
        (38, 95u16.to_le_bytes().to_vec()),
        (40, 0u16.to_le_bytes().to_vec()),
        (40, u16::MAX.to_le_bytes().to_vec()),
        (64, 1u16.to_le_bytes().to_vec()), // payload points into header
        (66, (MAX_RECORD_SIZE as u16 + 1).to_le_bytes().to_vec()),
        (68, 2u16.to_le_bytes().to_vec()), // flags
        (68, 1u16.to_le_bytes().to_vec()), // nonzero deleted payload
        (70, vec![1]),
        (72, vec![1]),
        (80, base[64..66].to_vec()), // overlap
        (100, vec![1]),              // free space
    ];
    for (offset, value) in cases {
        let mut malformed = base;
        malformed[offset..offset + value.len()].copy_from_slice(&value);
        reseal(&mut malformed);
        assert!(
            SlottedPage::decode(&malformed, 2).is_err(),
            "offset={offset} value={value:?}"
        );
    }
    let mut page = SlottedPage::decode(&base, 2).unwrap();
    page.delete(0).unwrap();
    let mut malformed = page.encode();
    malformed[PAGE_SIZE - 1] = 1;
    reseal(&mut malformed);
    assert!(SlottedPage::decode(&malformed, 2).is_err());
}

#[test]
fn slot_table_exhaustion_with_empty_records() {
    let mut page = SlottedPage::new(2, 0).unwrap();
    let slots = (PAGE_SIZE - HEADER_SIZE) / SLOT_SIZE;
    for expected in 0..slots {
        assert_eq!(page.insert(b"").unwrap(), expected as u16);
    }
    assert!(matches!(page.insert(b""), Err(Error::PageFull)));
    let decoded = SlottedPage::decode(&page.encode(), 2).unwrap();
    assert_eq!(usize::from(decoded.slot_count()), slots);
    assert_eq!(decoded.get(slots as u16 - 1).unwrap(), Some(b"".as_slice()));
}

#[test]
fn seeded_sequences_match_an_independent_record_model() {
    // The model stores records, not byte offsets/headers or the page codec.
    for seed in 1u64..=20 {
        let mut state = seed;
        let mut page = SlottedPage::new(seed + 1, seed * 10).unwrap();
        let mut model: Vec<Option<Vec<u8>>> = Vec::new();
        for step in 0..200 {
            state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
            if state % 4 == 0 && !model.is_empty() {
                let at = (state >> 32) as usize % model.len();
                assert_eq!(page.delete(at as u16).unwrap(), model[at].is_some());
                model[at] = None;
            } else {
                let data: Vec<u8> = (0..((state >> 32) as usize % 130))
                    .map(|i| (state as u8).wrapping_add(i as u8))
                    .collect();
                let before = page.encode();
                match page.insert(&data) {
                    Ok(slot) => {
                        assert_eq!(usize::from(slot), model.len());
                        model.push(Some(data));
                    }
                    Err(Error::PageFull) => assert_eq!(page.encode(), before),
                    Err(other) => panic!("seed={seed} step={step}: {other}"),
                }
            }
            page = SlottedPage::decode(&page.encode(), seed + 1).unwrap();
            assert_eq!(usize::from(page.slot_count()), model.len());
            for (slot, record) in model.iter().enumerate() {
                assert_eq!(
                    page.get(slot as u16).unwrap(),
                    record.as_deref(),
                    "seed={seed} step={step} slot={slot}"
                );
            }
        }
    }
}

#[cfg(unix)]
#[test]
fn encoded_page_survives_file_sync_and_reopen() {
    use kedr_core::storage::{FileStorage, StorageIo, read_exact_at, write_all_at};
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut attempt = NEXT.fetch_add(1, Ordering::Relaxed);
    let (path, mut file) = loop {
        let path =
            std::env::temp_dir().join(format!("kedr-page-{}-{attempt}.tmp", std::process::id()));
        match FileStorage::create_new(&path) {
            Ok(file) => break (path, file),
            Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                attempt += 1
            }
            Err(err) => panic!("{err}"),
        }
    };
    struct Cleanup(std::path::PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }
    let _cleanup = Cleanup(path.clone());
    let mut page = SlottedPage::new(2, 77).unwrap();
    page.insert(b"persistent-page-record").unwrap();
    write_all_at(&mut file, 2 * PAGE_SIZE as u64, &page.encode()).unwrap();
    file.sync().unwrap();
    drop(file);
    let mut reopened = FileStorage::open_existing(&path).unwrap();
    let mut bytes = [0; PAGE_SIZE];
    read_exact_at(&mut reopened, 2 * PAGE_SIZE as u64, &mut bytes).unwrap();
    let decoded = SlottedPage::decode(&bytes, 2).unwrap();
    assert_eq!(decoded.page_lsn(), 77);
    assert_eq!(
        decoded.get(0).unwrap(),
        Some(b"persistent-page-record".as_slice())
    );
}
