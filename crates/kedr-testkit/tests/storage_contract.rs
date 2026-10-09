// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error, Operation, Result,
    storage::{StorageIo, read_exact_at, write_all_at},
};
use kedr_testkit::{Fault, FaultIo, MemoryStorage, SeededRng};
use std::{collections::BTreeMap, io};

#[test]
fn exact_helpers_retry_short_io_and_interrupts() {
    let mut io = FaultIo::new(
        MemoryStorage::new(64),
        [
            (1, Fault::Error(io::ErrorKind::Interrupted)),
            (2, Fault::Short(2)),
            (4, Fault::Short(1)),
            (5, Fault::Error(io::ErrorKind::Interrupted)),
        ],
    )
    .unwrap();
    write_all_at(&mut io, 3, b"abcdef").unwrap();
    let mut bytes = [0; 6];
    read_exact_at(&mut io, 3, &mut bytes).unwrap();
    assert_eq!(&bytes, b"abcdef");
    assert_eq!(io.trace().len(), 6);
    assert_eq!(io.trace()[1].result, Ok(2));
    assert_eq!(io.trace()[3].result, Ok(1));
}

#[test]
fn zero_write_is_an_error_not_an_infinite_loop() {
    let mut io = FaultIo::new(MemoryStorage::new(64), [(1, Fault::Zero)]).unwrap();
    assert!(matches!(
        write_all_at(&mut io, 0, b"abc"),
        Err(Error::WriteZero { offset: 0 })
    ));
    assert!(io.inner_mut().visible_bytes().is_empty());
}

#[test]
fn eof_reports_actual_progress() {
    let mut io = MemoryStorage::new(64);
    write_all_at(&mut io, 0, b"abc").unwrap();
    let mut bytes = [0; 5];
    assert!(matches!(
        read_exact_at(&mut io, 0, &mut bytes),
        Err(Error::UnexpectedEof {
            offset: 0,
            expected: 5,
            actual: 3
        })
    ));
    assert_eq!(&bytes[..3], b"abc");
}

#[test]
fn errors_after_partial_write_leave_partial_bytes() {
    let mut io = FaultIo::new(
        MemoryStorage::new(64),
        [
            (1, Fault::Short(2)),
            (2, Fault::Error(io::ErrorKind::StorageFull)),
        ],
    )
    .unwrap();
    assert_eq!(
        write_all_at(&mut io, 0, b"abcdef").unwrap_err().code(),
        "KD1001"
    );
    assert_eq!(io.inner_mut().visible_bytes(), b"ab");
    io.inner_mut().crash().unwrap();
    assert!(io.inner_mut().visible_bytes().is_empty());
}

#[test]
fn range_overflow_is_rejected_before_backend_calls() {
    let mut io = FaultIo::new(MemoryStorage::new(64), []).unwrap();
    assert_eq!(
        write_all_at(&mut io, u64::MAX, b"ab").unwrap_err().code(),
        "KD1002"
    );
    assert_eq!(
        read_exact_at(&mut io, u64::MAX, &mut [0; 2])
            .unwrap_err()
            .code(),
        "KD1002"
    );
    write_all_at(&mut io, u64::MAX, b"").unwrap();
    read_exact_at(&mut io, u64::MAX, &mut []).unwrap();
    assert!(io.trace().is_empty());
}

#[test]
fn allocation_limit_rejects_mutation_and_sparse_holes_are_zero() {
    let mut io = MemoryStorage::new(8);
    write_all_at(&mut io, 3, b"abc").unwrap();
    assert_eq!(io.visible_bytes(), b"\0\0\0abc");
    assert_eq!(
        write_all_at(&mut io, 7, b"xx").unwrap_err().code(),
        "KD1005"
    );
    assert_eq!(io.set_len(u64::MAX).unwrap_err().code(), "KD1005");
    assert_eq!(io.visible_bytes(), b"\0\0\0abc");
    io.set_len(4).unwrap();
    io.set_len(8).unwrap();
    assert_eq!(io.visible_bytes(), b"\0\0\0a\0\0\0\0");
}

#[test]
fn sync_and_crash_include_file_length() {
    let mut io = MemoryStorage::new(64);
    write_all_at(&mut io, 0, b"confirmed").unwrap();
    io.sync().unwrap();
    io.set_len(2).unwrap();
    io.crash().unwrap();
    assert_eq!(io.visible_bytes(), b"confirmed");
    io.set_len(2).unwrap();
    io.sync().unwrap();
    io.set_len(0).unwrap();
    io.crash().unwrap();
    assert_eq!(io.visible_bytes(), b"co");
}

#[test]
fn sync_failure_can_happen_before_or_after_persistence() {
    for (fault, survives) in [
        (Fault::Error(io::ErrorKind::Other), false),
        (Fault::SyncThenError(io::ErrorKind::Other), true),
    ] {
        let mut io = FaultIo::new(MemoryStorage::new(64), [(2, fault)]).unwrap();
        write_all_at(&mut io, 0, b"commit-candidate").unwrap();
        assert_eq!(io.sync().unwrap_err().code(), "KD1001");
        io.inner_mut().crash().unwrap();
        assert_eq!(
            io.inner_mut().visible_bytes() == b"commit-candidate",
            survives
        );
        assert_eq!(io.trace()[1].operation, Operation::Sync);
    }
}

#[test]
fn invalid_faults_and_duplicate_call_numbers_are_rejected() {
    assert!(FaultIo::new(MemoryStorage::new(8), [(0, Fault::Zero)]).is_err());
    assert!(FaultIo::new(MemoryStorage::new(8), [(1, Fault::Zero), (1, Fault::Zero)]).is_err());
    let mut io = FaultIo::new(MemoryStorage::new(8), [(1, Fault::Short(2))]).unwrap();
    assert_eq!(io.sync().unwrap_err().code(), "KD1008");
}

struct Overreporting;
impl StorageIo for Overreporting {
    fn read_at(&mut self, _: u64, buffer: &mut [u8]) -> Result<usize> {
        Ok(buffer.len() + 1)
    }
    fn write_at(&mut self, _: u64, bytes: &[u8]) -> Result<usize> {
        Ok(bytes.len() + 1)
    }
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_len(&mut self, _: u64) -> Result<()> {
        Ok(())
    }
    fn len(&mut self) -> Result<u64> {
        Ok(0)
    }
}
#[test]
fn broken_backend_byte_counts_are_rejected() {
    assert_eq!(
        write_all_at(&mut Overreporting, 0, b"x")
            .unwrap_err()
            .code(),
        "KD1007"
    );
    assert_eq!(
        read_exact_at(&mut Overreporting, 0, &mut [0])
            .unwrap_err()
            .code(),
        "KD1007"
    );
}

// Intentionally faulty implementation: sync reports success without persistence.
struct MissingSync(MemoryStorage);
impl StorageIo for MissingSync {
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        self.0.read_at(offset, buffer)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        self.0.write_at(offset, bytes)
    }
    fn sync(&mut self) -> Result<()> {
        Ok(())
    }
    fn set_len(&mut self, length: u64) -> Result<()> {
        self.0.set_len(length)
    }
    fn len(&mut self) -> Result<u64> {
        self.0.len()
    }
}

trait Crashable: StorageIo {
    fn simulated_crash(&mut self);
}
impl Crashable for MemoryStorage {
    fn simulated_crash(&mut self) {
        self.crash().unwrap();
    }
}
impl Crashable for MissingSync {
    fn simulated_crash(&mut self) {
        self.0.crash().unwrap();
    }
}
fn survives_sync(io: &mut impl Crashable) -> bool {
    write_all_at(io, 0, b"must-survive").unwrap();
    io.sync().unwrap();
    io.simulated_crash();
    let mut bytes = [0; 12];
    read_exact_at(io, 0, &mut bytes).is_ok() && &bytes == b"must-survive"
}

#[test]
fn harness_detects_intentionally_missing_sync() {
    let mut good = MemoryStorage::new(64);
    assert!(survives_sync(&mut good));
    let mut bad = MissingSync(MemoryStorage::new(64));
    assert!(
        !survives_sync(&mut bad),
        "the harness missed lost synchronized bytes"
    );
}

#[derive(Default, Clone)]
struct Oracle {
    bytes: BTreeMap<u64, u8>,
    length: u64,
}
impl Oracle {
    fn write(&mut self, offset: u64, bytes: &[u8]) {
        for (i, byte) in bytes.iter().enumerate() {
            self.bytes.insert(offset + i as u64, *byte);
        }
        if !bytes.is_empty() {
            self.length = self.length.max(offset + bytes.len() as u64);
        }
    }
    fn resize(&mut self, length: u64) {
        self.bytes.retain(|offset, _| *offset < length);
        self.length = length;
    }
    fn byte(&self, offset: u64) -> u8 {
        *self.bytes.get(&offset).unwrap_or(&0)
    }
    fn matches(&self, actual: &[u8]) -> bool {
        actual.len() as u64 == self.length
            && actual
                .iter()
                .enumerate()
                .all(|(i, byte)| *byte == self.byte(i as u64))
    }
}

#[test]
fn seeded_operations_match_independent_sparse_byte_oracle() {
    let seeds: Vec<u64> = match std::env::var("KEDR_TEST_SEED") {
        Ok(seed) => vec![seed.parse().expect("KEDR_TEST_SEED must be u64")],
        Err(_) => (0..100).collect(),
    };
    for seed in seeds {
        let mut rng = SeededRng::new(seed);
        let mut io = MemoryStorage::new(256);
        let mut visible = Oracle::default();
        let mut durable = Oracle::default();
        for step in 0..2_000 {
            match rng.next_u64() % 6 {
                0 | 1 => {
                    let offset = rng.next_u64() % 224;
                    let count = (rng.next_u64() % 32) as usize;
                    let bytes: Vec<_> = (0..count).map(|_| rng.next_u64() as u8).collect();
                    write_all_at(&mut io, offset, &bytes).unwrap();
                    visible.write(offset, &bytes);
                }
                2 => {
                    let length = rng.next_u64() % 257;
                    io.set_len(length).unwrap();
                    visible.resize(length);
                }
                3 => {
                    io.sync().unwrap();
                    durable = visible.clone();
                }
                4 => {
                    io.crash().unwrap();
                    visible = durable.clone();
                }
                _ => {
                    let offset = rng.next_u64() % 257;
                    let mut bytes = [0xff; 32];
                    let count = io.read_at(offset, &mut bytes).unwrap();
                    let expected = visible.length.saturating_sub(offset).min(32) as usize;
                    assert_eq!(count, expected, "seed={seed}, step={step}");
                    for (i, byte) in bytes[..count].iter().enumerate() {
                        assert_eq!(
                            *byte,
                            visible.byte(offset + i as u64),
                            "seed={seed}, step={step}"
                        );
                    }
                    assert!(bytes[count..].iter().all(|byte| *byte == 0xff));
                }
            }
            assert!(
                visible.matches(io.visible_bytes()),
                "visible mismatch: seed={seed}, step={step}"
            );
            assert!(
                durable.matches(io.durable_bytes()),
                "durable mismatch: seed={seed}, step={step}"
            );
        }
    }
}

#[test]
fn replay_has_identical_trace_and_bytes() {
    fn run() -> (Vec<kedr_testkit::TraceEntry>, Vec<u8>) {
        let mut io = FaultIo::new(
            MemoryStorage::new(64),
            [
                (1, Fault::Short(3)),
                (3, Fault::SyncThenError(io::ErrorKind::Other)),
            ],
        )
        .unwrap();
        write_all_at(&mut io, 0, b"replay").unwrap();
        assert!(io.sync().is_err());
        io.inner_mut().crash().unwrap();
        let trace = io.trace().to_vec();
        (trace, io.into_inner().visible_bytes().to_vec())
    }
    assert_eq!(run(), run());
}
