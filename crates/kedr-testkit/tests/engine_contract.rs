// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error, Operation, Result,
    engine::StorageEngine,
    page::PAGE_SIZE,
    record::primary_key,
    storage::{StorageIo, write_all_at},
    wal::WalConfig,
};
use kedr_testkit::{Fault, FaultIo, MemoryStorage};
use std::sync::{Arc, Mutex};
const LIMIT: usize = 64 * 1024 * 1024;
const CACHE: usize = 4 * PAGE_SIZE;
fn initial() -> (Vec<u8>, Vec<u8>) {
    let engine = StorageEngine::create(
        MemoryStorage::new(LIMIT),
        MemoryStorage::new(LIMIT),
        [4; 16],
        CACHE,
    )
    .unwrap();
    let (data, wal) = engine.into_io();
    (data.visible_bytes().to_vec(), wal.visible_bytes().to_vec())
}
fn copy(bytes: &[u8]) -> MemoryStorage {
    let mut io = MemoryStorage::new(LIMIT);
    write_all_at(&mut io, 0, bytes).unwrap();
    io.sync().unwrap();
    io
}
fn recover(data: &[u8], wal: &[u8]) -> StorageEngine<MemoryStorage, MemoryStorage> {
    StorageEngine::recover(copy(data), copy(wal), WalConfig::first([4; 16]), CACHE)
        .unwrap()
        .0
}

#[test]
fn rollback_drop_aborted_state_and_read_your_writes() {
    let (data, wal) = initial();
    let mut engine = recover(&data, &wal);
    {
        let mut tx = engine.begin().unwrap();
        tx.insert(&primary_key(1), b"private").unwrap();
        assert_eq!(tx.get(&primary_key(1)).unwrap(), Some(b"private".to_vec()));
        tx.rollback();
    }
    assert_eq!(engine.get(&primary_key(1)).unwrap(), None);
    {
        let mut tx = engine.begin().unwrap();
        tx.insert(&primary_key(2), b"dropped").unwrap();
    }
    assert_eq!(engine.get(&primary_key(2)).unwrap(), None);
    {
        let mut tx = engine.begin().unwrap();
        tx.insert(&primary_key(3), b"must roll back").unwrap();
        assert!(matches!(
            tx.insert(&primary_key(3), b"duplicate"),
            Err(Error::DuplicateKey)
        ));
        assert!(tx.get(&primary_key(3)).is_err());
        assert!(tx.commit().is_err());
    }
    assert_eq!(engine.get(&primary_key(3)).unwrap(), None);
    assert!(!engine.is_poisoned());
    let (data_after, wal_after) = engine.into_io();
    assert_eq!(data_after.visible_bytes(), data);
    assert_eq!(wal_after.visible_bytes(), wal);
}

#[test]
fn committed_multiple_pages_recover_repeatedly_after_data_loss() {
    let (data, wal) = initial();
    let mut engine = recover(&data, &wal);
    engine
        .transaction(|tx| {
            for key in 0..100i64 {
                tx.insert(&primary_key(key), &vec![key as u8; 4096])?;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(engine.integrity_check().unwrap().records, 100);
    let (mut data, mut wal) = engine.into_io();
    data.crash().unwrap();
    wal.crash().unwrap();
    let log = wal.durable_bytes().to_vec();
    for _ in 0..2 {
        // All data pages are lost; retained full WAL starts with database creation.
        let (mut engine, summary) = StorageEngine::recover(
            MemoryStorage::new(LIMIT),
            copy(&log),
            WalConfig::first([4; 16]),
            CACHE,
        )
        .unwrap();
        assert_eq!(summary.transactions, 2);
        for key in 0..100i64 {
            assert_eq!(
                engine.get(&primary_key(key)).unwrap(),
                Some(vec![key as u8; 4096])
            );
        }
        assert_eq!(engine.integrity_check().unwrap().records, 100);
    }
}

#[test]
fn private_limit_failure_rolls_back_without_poisoning_database() {
    let (data, wal) = initial();
    let mut engine = recover(&data, &wal);
    engine.set_private_page_limit(2).unwrap();
    assert!(
        engine
            .transaction(|tx| {
                for key in 0..100i64 {
                    tx.insert(&primary_key(key), &vec![key as u8; 4096])?;
                }
                Ok(())
            })
            .is_err()
    );
    assert!(!engine.is_poisoned());
    assert_eq!(engine.integrity_check().unwrap().records, 0);
    let (data_after, wal_after) = engine.into_io();
    assert_eq!(data_after.visible_bytes(), data);
    assert_eq!(wal_after.visible_bytes(), wal);
}

#[test]
fn truncated_valid_journal_cannot_erase_a_newer_durable_commit() {
    let (data, old_wal) = initial();
    let mut engine = recover(&data, &old_wal);
    engine
        .transaction(|tx| tx.insert(&primary_key(99), b"acknowledged"))
        .unwrap();
    let (mut data, _) = engine.into_io();
    let before = data.visible_bytes().to_vec();
    let result =
        StorageEngine::recover(&mut data, copy(&old_wal), WalConfig::first([4; 16]), CACHE);
    assert!(matches!(result, Err(Error::CorruptWal { .. })));
    assert_eq!(data.visible_bytes(), before);
}

#[test]
fn wrong_database_journal_is_rejected_before_touching_source_pages() {
    let (data, _) = initial();
    let other = StorageEngine::create(
        MemoryStorage::new(LIMIT),
        MemoryStorage::new(LIMIT),
        [7; 16],
        CACHE,
    )
    .unwrap();
    let (_, wal) = other.into_io();
    let mut data = copy(&data);
    let before = data.visible_bytes().to_vec();
    let result = StorageEngine::recover(&mut data, wal, WalConfig::first([7; 16]), CACHE);
    assert!(matches!(result, Err(Error::CorruptWal { .. })));
    assert_eq!(data.visible_bytes(), before);
}

#[test]
fn cancellation_and_deadline_interrupt_cached_operations_and_prevent_commit() {
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::{Duration, Instant},
    };
    for timeout in [false, true] {
        let (data, wal) = initial();
        let mut engine = recover(&data, &wal);
        let flag = Arc::new(AtomicBool::new(false));
        let mut tx = engine.begin().unwrap();
        tx.insert(&primary_key(1), b"private").unwrap();
        assert!(tx.get(&primary_key(1)).unwrap().is_some());
        let deadline = if timeout {
            Instant::now() - Duration::from_millis(1)
        } else {
            Instant::now() + Duration::from_secs(5)
        };
        tx.set_interrupt(flag.clone(), deadline);
        if !timeout {
            flag.store(true, Ordering::Release);
        }
        let result = tx.get(&primary_key(1));
        if timeout {
            assert!(matches!(result, Err(Error::TransactionTimeout)));
        } else {
            assert!(matches!(result, Err(Error::Cancelled)));
        }
        assert!(matches!(tx.commit(), Err(Error::TransactionAborted)));
        assert_eq!(engine.get(&primary_key(1)).unwrap(), None);
        let (d, w) = engine.into_io();
        assert_eq!(d.visible_bytes(), data);
        assert_eq!(w.visible_bytes(), wal);
    }
    let (data, wal) = initial();
    let mut engine = recover(&data, &wal);
    let flag = Arc::new(AtomicBool::new(false));
    let mut tx = engine.begin().unwrap();
    tx.insert(&primary_key(1), b"cancel before COMMIT").unwrap();
    tx.set_interrupt(flag.clone(), Instant::now() + Duration::from_secs(5));
    flag.store(true, Ordering::Release);
    assert!(matches!(tx.commit(), Err(Error::Cancelled)));
    let (d, w) = engine.into_io();
    assert_eq!(d.visible_bytes(), data);
    assert_eq!(w.visible_bytes(), wal);
}

#[test]
fn recovery_failure_at_every_io_call_remains_repeatable() {
    let (data, wal) = initial();
    let mut engine = recover(&data, &wal);
    engine
        .transaction(|tx| {
            for key in 0..12 {
                tx.insert(&primary_key(key), &vec![key as u8; 4096])?;
            }
            Ok(())
        })
        .unwrap();
    let (_, wal) = engine.into_io();
    let wal = wal.durable_bytes().to_vec();
    let (engine, _) = StorageEngine::recover(
        FaultIo::new(copy(&data), []).unwrap(),
        FaultIo::new(copy(&wal), []).unwrap(),
        WalConfig::first([4; 16]),
        CACHE,
    )
    .unwrap();
    let (d, w) = engine.into_io();
    for (role, trace) in [(Role::Data, d.trace()), (Role::Wal, w.trace())] {
        for point in trace {
            let faults = if point.operation == Operation::Sync {
                vec![
                    Fault::Error(std::io::ErrorKind::Other),
                    Fault::SyncThenError(std::io::ErrorKind::Other),
                ]
            } else {
                vec![Fault::Error(std::io::ErrorKind::Other)]
            };
            for fault in faults {
                let mut d = FaultIo::new(
                    copy(&data),
                    if role == Role::Data {
                        vec![(point.call, fault)]
                    } else {
                        vec![]
                    },
                )
                .unwrap();
                let mut w = FaultIo::new(
                    copy(&wal),
                    if role == Role::Wal {
                        vec![(point.call, fault)]
                    } else {
                        vec![]
                    },
                )
                .unwrap();
                assert!(
                    StorageEngine::recover(&mut d, &mut w, WalConfig::first([4; 16]), CACHE)
                        .is_err(),
                    "role={role:?} call={}",
                    point.call
                );
                let mut d = d.into_inner();
                let mut w = w.into_inner();
                d.crash().unwrap();
                w.crash().unwrap();
                let (mut engine, _) =
                    StorageEngine::recover(d, w, WalConfig::first([4; 16]), CACHE).unwrap();
                for key in 0..12 {
                    assert_eq!(
                        engine.get(&primary_key(key)).unwrap(),
                        Some(vec![key as u8; 4096])
                    );
                }
                assert_eq!(engine.integrity_check().unwrap().records, 12);
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Role {
    Data,
    Wal,
}
#[derive(Clone, Debug)]
struct Event {
    role: Role,
    operation: Operation,
}
struct Observed {
    io: MemoryStorage,
    role: Role,
    events: Arc<Mutex<Vec<Event>>>,
}
impl Observed {
    fn event(&self, operation: Operation) {
        self.events.lock().unwrap().push(Event {
            role: self.role,
            operation,
        });
    }
}
impl StorageIo for Observed {
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        self.event(Operation::Read);
        self.io.read_at(offset, buffer)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        self.event(Operation::Write);
        self.io.write_at(offset, bytes)
    }
    fn sync(&mut self) -> Result<()> {
        self.event(Operation::Sync);
        self.io.sync()
    }
    fn set_len(&mut self, length: u64) -> Result<()> {
        self.event(Operation::Resize);
        self.io.set_len(length)
    }
    fn len(&mut self) -> Result<u64> {
        self.event(Operation::Length);
        self.io.len()
    }
}

#[test]
fn dirty_eviction_never_writes_base_before_wal_sync() {
    let (data, wal) = initial();
    let events = Arc::new(Mutex::new(Vec::new()));
    let d = Observed {
        io: copy(&data),
        role: Role::Data,
        events: events.clone(),
    };
    let w = Observed {
        io: copy(&wal),
        role: Role::Wal,
        events: events.clone(),
    };
    let (mut engine, _) = StorageEngine::recover(d, w, WalConfig::first([4; 16]), CACHE).unwrap();
    events.lock().unwrap().clear();
    let mut tx = engine.begin().unwrap();
    for key in 0..100i64 {
        tx.insert(&primary_key(key), &vec![key as u8; 4096])
            .unwrap();
    }
    assert!(
        events
            .lock()
            .unwrap()
            .iter()
            .all(|e| e.operation == Operation::Read || e.operation == Operation::Length)
    );
    tx.commit().unwrap();
    let events = events.lock().unwrap();
    let first_data = events
        .iter()
        .position(|e| {
            e.role == Role::Data
                && matches!(
                    e.operation,
                    Operation::Write | Operation::Resize | Operation::Sync
                )
        })
        .unwrap();
    let wal_sync = events
        .iter()
        .position(|e| e.role == Role::Wal && e.operation == Operation::Sync)
        .unwrap();
    assert!(first_data > wal_sync);
}

#[test]
fn commit_faults_never_lose_acknowledged_base_and_ambiguous_sync_is_recovered() {
    let (data, wal) = initial();
    let mut base = recover(&data, &wal);
    base.transaction(|tx| tx.insert(&primary_key(-1), b"already acknowledged"))
        .unwrap();
    let (data, wal) = base.into_io();
    let data = data.visible_bytes().to_vec();
    let wal = wal.visible_bytes().to_vec();
    // First successful trace determines actual call positions; no guessed offsets.
    let d = FaultIo::new(copy(&data), []).unwrap();
    let w = FaultIo::new(copy(&wal), []).unwrap();
    let (mut engine, _) = StorageEngine::recover(d, w, WalConfig::first([4; 16]), CACHE).unwrap();
    engine
        .transaction(|tx| {
            tx.insert(&primary_key(1), b"one")?;
            tx.insert(&primary_key(2), b"two")?;
            Ok(())
        })
        .unwrap();
    let (d, w) = engine.into_io();
    let first_wal_mutation = w
        .trace()
        .iter()
        .find(|e| e.operation == Operation::Write)
        .unwrap()
        .call;
    let wal_points: Vec<_> = w
        .trace()
        .iter()
        .filter(|e| {
            e.call >= first_wal_mutation
                && matches!(e.operation, Operation::Write | Operation::Sync)
        })
        .map(|e| (e.call, e.operation))
        .collect();
    let first_data_mutation = d
        .trace()
        .iter()
        .rfind(|e| e.operation == Operation::Resize)
        .unwrap()
        .call;
    let data_points: Vec<_> = d
        .trace()
        .iter()
        .filter(|e| {
            e.call >= first_data_mutation
                && matches!(
                    e.operation,
                    Operation::Write | Operation::Sync | Operation::Resize
                )
        })
        .map(|e| (e.call, e.operation))
        .collect();
    for (role, points) in [(Role::Wal, wal_points), (Role::Data, data_points)] {
        for (call, operation) in points {
            let faults = if operation == Operation::Sync {
                vec![
                    Fault::Error(std::io::ErrorKind::Other),
                    Fault::SyncThenError(std::io::ErrorKind::Other),
                ]
            } else {
                vec![Fault::Error(std::io::ErrorKind::StorageFull)]
            };
            for fault in faults {
                let ds = if role == Role::Data {
                    vec![(call, fault)]
                } else {
                    vec![]
                };
                let ws = if role == Role::Wal {
                    vec![(call, fault)]
                } else {
                    vec![]
                };
                let (mut engine, _) = StorageEngine::recover(
                    FaultIo::new(copy(&data), ds).unwrap(),
                    FaultIo::new(copy(&wal), ws).unwrap(),
                    WalConfig::first([4; 16]),
                    CACHE,
                )
                .unwrap();
                let result = engine.transaction(|tx| {
                    tx.insert(&primary_key(1), b"one")?;
                    tx.insert(&primary_key(2), b"two")?;
                    Ok(())
                });
                assert!(result.is_err(), "{role:?} call={call} op={operation:?}");
                let (d, w) = engine.into_io();
                let mut d = d.into_inner();
                let mut w = w.into_inner();
                d.crash().unwrap();
                w.crash().unwrap();
                let (mut recovered, _) =
                    StorageEngine::recover(d, w, WalConfig::first([4; 16]), CACHE).unwrap();
                let one = recovered.get(&primary_key(1)).unwrap();
                let two = recovered.get(&primary_key(2)).unwrap();
                assert_eq!(
                    recovered.get(&primary_key(-1)).unwrap(),
                    Some(b"already acknowledged".to_vec())
                );
                assert_eq!(
                    one.is_some(),
                    two.is_some(),
                    "partial transaction {role:?} call={call}"
                );
                if role == Role::Data || matches!(fault, Fault::SyncThenError(_)) {
                    assert!(one.is_some(), "durable commit missing");
                }
            }
        }
    }
}
