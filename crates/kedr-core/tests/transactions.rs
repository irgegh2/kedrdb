// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use kedr_core::{
    Error,
    database::FileDatabase,
    page::PAGE_SIZE,
    record::primary_key,
    transaction::{Mode, SessionLimits, SharedDatabase},
};
use std::{
    fs,
    path::PathBuf,
    sync::{
        Arc, Barrier,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn database(limits: SessionLimits) -> (Scratch, SharedDatabase) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = Scratch(std::env::temp_dir().join(format!(
        "kedr-tx-{}-{stamp}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    let file = FileDatabase::create(&root.0, [38; 16], 4 * PAGE_SIZE).unwrap();
    let db = SharedDatabase::new(file, limits).unwrap();
    (root, db)
}
fn wait_for(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "condition timed out");
        thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn explicit_rollback_aborted_errors_and_disconnect_leave_no_partial_changes() {
    let (_root, db) = database(SessionLimits::default());
    let mut writer = db.connect().unwrap();
    writer.insert(&primary_key(1), b"base").unwrap();
    writer.begin(Mode::ReadWrite).unwrap();
    writer.replace(&primary_key(1), b"private").unwrap();
    writer.insert(&primary_key(2), b"private row").unwrap();
    assert_eq!(
        writer.get(&primary_key(1)).unwrap(),
        Some(b"private".to_vec())
    );
    assert!(matches!(
        writer.insert(&primary_key(1), b"duplicate"),
        Err(Error::DuplicateKey)
    ));
    assert!(matches!(
        writer.get(&primary_key(1)),
        Err(Error::TransactionAborted)
    ));
    assert!(matches!(writer.commit(), Err(Error::TransactionAborted)));
    assert!(matches!(
        writer.get(&primary_key(1)),
        Err(Error::TransactionAborted)
    ));
    writer.rollback().unwrap();
    assert_eq!(writer.get(&primary_key(1)).unwrap(), Some(b"base".to_vec()));
    assert_eq!(writer.get(&primary_key(2)).unwrap(), None);
    writer.begin(Mode::ReadWrite).unwrap();
    writer.insert(&primary_key(3), b"disconnect").unwrap();
    drop(writer);
    let mut reader = db.connect().unwrap();
    assert_eq!(reader.get(&primary_key(3)).unwrap(), None);
    reader.begin(Mode::ReadWrite).unwrap();
    assert!(
        reader
            .fail(Error::InvalidArgument {
                message: "SQL parser error"
            })
            .is_err()
    );
    assert!(matches!(
        reader.replace(&primary_key(1), b"forbidden"),
        Err(Error::TransactionAborted)
    ));
    reader.rollback().unwrap();
    assert!(matches!(
        reader.commit(),
        Err(Error::InvalidTransactionState)
    ));
}

#[test]
fn shared_readers_block_writers_and_new_readers_cannot_jump_the_waiting_writer() {
    let (_root, db) = database(SessionLimits::default());
    let mut first = db.connect().unwrap();
    let mut second = db.connect().unwrap();
    first.insert(&primary_key(1), b"before").unwrap();
    first.begin(Mode::ReadOnly).unwrap();
    second.begin(Mode::ReadOnly).unwrap();
    assert_eq!(db.lock_metrics().readers, 2);
    let writer_db = db.clone();
    let (events_sender, events) = mpsc::channel();
    let sender = events_sender.clone();
    let writer = thread::spawn(move || {
        let mut session = writer_db.connect().unwrap();
        session.begin(Mode::ReadWrite).unwrap();
        sender.send("writer").unwrap();
        session.replace(&primary_key(1), b"after").unwrap();
        session.commit().unwrap();
    });
    wait_for(|| db.lock_metrics().waiting == 1);
    let reader_db = db.clone();
    let later_reader = thread::spawn(move || {
        let mut session = reader_db.connect().unwrap();
        session.begin(Mode::ReadOnly).unwrap();
        events_sender.send("later reader").unwrap();
        assert_eq!(
            session.get(&primary_key(1)).unwrap(),
            Some(b"after".to_vec())
        );
        session.commit().unwrap();
    });
    wait_for(|| db.lock_metrics().waiting == 2);
    assert!(events.try_recv().is_err());
    assert_eq!(
        first.get(&primary_key(1)).unwrap(),
        Some(b"before".to_vec())
    );
    first.commit().unwrap();
    assert_eq!(db.lock_metrics().readers, 1);
    assert!(events.try_recv().is_err());
    second.commit().unwrap();
    assert_eq!(
        events.recv_timeout(Duration::from_secs(5)).unwrap(),
        "writer"
    );
    assert_eq!(
        events.recv_timeout(Duration::from_secs(5)).unwrap(),
        "later reader"
    );
    writer.join().unwrap();
    later_reader.join().unwrap();
    assert!(db.lock_metrics().max_wait_nanos > 0);
}

#[test]
fn read_only_rejects_writes_and_upgrade_and_never_changes_wal() {
    let (root, db) = database(SessionLimits::default());
    let wal_path = root.0.join("wal/0000000000000001.wal");
    let before = fs::read(&wal_path).unwrap();
    let mut session = db.connect().unwrap();
    session.begin(Mode::ReadOnly).unwrap();
    assert!(matches!(
        session.insert(&primary_key(1), b"forbidden"),
        Err(Error::ReadOnlyTransaction)
    ));
    assert!(matches!(
        session.get(&primary_key(1)),
        Err(Error::TransactionAborted)
    ));
    session.rollback().unwrap();
    session.begin(Mode::ReadOnly).unwrap();
    assert!(matches!(
        session.begin(Mode::ReadWrite),
        Err(Error::InvalidTransactionState)
    ));
    session.rollback().unwrap();
    assert_eq!(session.get(&primary_key(1)).unwrap(), None);
    assert_eq!(fs::read(wal_path).unwrap(), before);
}

#[test]
fn lock_timeout_and_cancellation_remove_waiters_without_starving_later_clients() {
    let limits = SessionLimits {
        lock_wait: Duration::from_millis(100),
        ..SessionLimits::default()
    };
    let (_root, db) = database(limits);
    let mut holder = db.connect().unwrap();
    holder.begin(Mode::ReadWrite).unwrap();
    let mut waiter = db.connect().unwrap();
    assert!(matches!(
        waiter.get(&primary_key(1)),
        Err(Error::LockTimeout)
    ));
    assert_eq!(db.lock_metrics().waiting, 0);
    let cancel = waiter.cancel_handle();
    let thread = thread::spawn(move || {
        let result = waiter.begin(Mode::ReadWrite);
        (waiter, result)
    });
    wait_for(|| db.lock_metrics().waiting == 1);
    cancel.cancel();
    let (mut waiter, result) = thread.join().unwrap();
    assert!(matches!(result, Err(Error::Cancelled)));
    assert_eq!(db.lock_metrics().waiting, 0);
    holder.rollback().unwrap();
    waiter.begin(Mode::ReadWrite).unwrap();
    waiter.commit().unwrap();
}

#[test]
fn idle_and_lifetime_expiry_and_active_cancellation_release_locks_without_client_commands() {
    for cause in 0..3 {
        let limits = SessionLimits {
            idle_in_transaction: if cause == 0 {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(5)
            },
            transaction_lifetime: if cause == 1 {
                Duration::from_millis(100)
            } else {
                Duration::from_secs(5)
            },
            ..SessionLimits::default()
        };
        let (_root, db) = database(limits);
        let mut session = db.connect().unwrap();
        session.begin(Mode::ReadWrite).unwrap();
        session.insert(&primary_key(1), b"private").unwrap();
        if cause == 2 {
            session.cancel_handle().cancel();
        }
        wait_for(|| !db.lock_metrics().writer);
        let mut other = db.connect().unwrap();
        assert_eq!(other.get(&primary_key(1)).unwrap(), None);
        let result = session.get(&primary_key(1));
        if cause == 2 {
            assert!(matches!(result, Err(Error::Cancelled)));
        } else {
            assert!(matches!(result, Err(Error::TransactionTimeout)));
        }
        assert!(matches!(
            session.get(&primary_key(1)),
            Err(Error::TransactionAborted)
        ));
        session.rollback().unwrap();
        session.insert(&primary_key(1), b"new").unwrap();
    }
}

#[test]
fn bounded_admission_and_oversized_command_abort_before_copying_payloads() {
    let (_root, db) = database(SessionLimits {
        max_sessions: 1,
        ..SessionLimits::default()
    });
    let mut session = db.connect().unwrap();
    assert!(matches!(db.connect(), Err(Error::LimitExceeded { .. })));
    session.begin(Mode::ReadWrite).unwrap();
    session.insert(&primary_key(1), b"private").unwrap();
    assert!(matches!(
        session.insert(&primary_key(2), &vec![1; 8193]),
        Err(Error::RecordTooLarge { .. })
    ));
    assert!(matches!(session.commit(), Err(Error::TransactionAborted)));
    session.rollback().unwrap();
    assert_eq!(session.get(&primary_key(1)).unwrap(), None);
    drop(session);
    let _session = db.connect().unwrap();
}

#[test]
fn private_page_limit_failure_aborts_whole_session_and_allows_next_writer() {
    let (_root, db) = database(SessionLimits::default());
    let mut session = db.connect().unwrap();
    session.insert(&primary_key(-1), b"acknowledged").unwrap();
    session.begin(Mode::ReadWrite).unwrap();
    let mut failed = false;
    for key in 0..300 {
        match session.insert(&primary_key(key), &vec![key as u8; 8192]) {
            Ok(()) => {}
            Err(Error::LimitExceeded { .. }) => {
                failed = true;
                break;
            }
            Err(error) => panic!("unexpected {error}"),
        }
    }
    assert!(failed);
    assert!(matches!(session.commit(), Err(Error::TransactionAborted)));
    session.rollback().unwrap();
    assert_eq!(session.get(&primary_key(0)).unwrap(), None);
    assert_eq!(
        session.get(&primary_key(-1)).unwrap(),
        Some(b"acknowledged".to_vec())
    );
    let mut next = db.connect().unwrap();
    next.insert(&primary_key(1), b"new writer").unwrap();
}

#[test]
fn read_only_idle_expiry_and_cancel_release_shared_permits() {
    for cancel in [false, true] {
        let (_root, db) = database(SessionLimits {
            idle_in_transaction: Duration::from_millis(100),
            ..SessionLimits::default()
        });
        let mut reader = db.connect().unwrap();
        reader.insert(&primary_key(1), b"base").unwrap();
        reader.begin(Mode::ReadOnly).unwrap();
        assert_eq!(db.lock_metrics().readers, 1);
        if cancel {
            reader.cancel_handle().cancel();
        }
        wait_for(|| db.lock_metrics().readers == 0);
        let mut writer = db.connect().unwrap();
        writer.replace(&primary_key(1), b"after").unwrap();
        let result = reader.get(&primary_key(1));
        if cancel {
            assert!(matches!(result, Err(Error::Cancelled)));
        } else {
            assert!(matches!(result, Err(Error::TransactionTimeout)));
        }
        reader.rollback().unwrap();
        assert_eq!(
            reader.get(&primary_key(1)).unwrap(),
            Some(b"after".to_vec())
        );
    }
}

#[test]
fn concurrent_acknowledged_histories_are_serial_with_one_two_eight_and_thirty_two_clients() {
    for clients in [1, 2, 8, 32] {
        let (root, db) = database(SessionLimits {
            max_sessions: 64,
            lock_wait: Duration::from_secs(20),
            transaction_lifetime: Duration::from_secs(20),
            ..SessionLimits::default()
        });
        let mut setup = db.connect().unwrap();
        setup.insert(&primary_key(0), &0u64.to_le_bytes()).unwrap();
        drop(setup);
        let start = Arc::new(Barrier::new(clients));
        let (sender, events) = mpsc::channel();
        let threads: Vec<_> = (0..clients)
            .map(|client| {
                let db = db.clone();
                let start = start.clone();
                let sender = sender.clone();
                thread::spawn(move || {
                    let mut session = db.connect().unwrap();
                    start.wait();
                    for operation in 0..12 {
                        let row = (client * 1000 + operation + 1) as i64;
                        session.begin(Mode::ReadWrite).unwrap();
                        let bytes = session.get(&primary_key(0)).unwrap().unwrap();
                        let before = u64::from_le_bytes(bytes.try_into().unwrap());
                        session
                            .replace(&primary_key(0), &(before + 1).to_le_bytes())
                            .unwrap();
                        session
                            .insert(&primary_key(row), &(before + 1).to_le_bytes())
                            .unwrap();
                        match operation % 6 {
                            0 => session.rollback().unwrap(),
                            1 => {
                                drop(session);
                                session = db.connect().unwrap();
                            }
                            2 => {
                                session.cancel_handle().cancel();
                                assert!(session.commit().is_err());
                                session.rollback().unwrap();
                            }
                            _ => {
                                session.commit().unwrap();
                                sender.send((row, before + 1)).unwrap();
                            }
                        }
                        session.begin(Mode::ReadOnly).unwrap();
                        let _ = session.get(&primary_key(0)).unwrap();
                        session.commit().unwrap();
                    }
                })
            })
            .collect();
        drop(sender);
        for thread in threads {
            thread.join().unwrap();
        }
        let mut history: Vec<_> = events.into_iter().collect();
        history.sort_unstable_by_key(|(_, sequence)| *sequence);
        assert_eq!(history.len(), clients * 6);
        for (index, (_, sequence)) in history.iter().enumerate() {
            assert_eq!(*sequence, index as u64 + 1);
        }
        let mut verify = db.connect().unwrap();
        assert_eq!(
            verify.get(&primary_key(0)).unwrap(),
            Some((history.len() as u64).to_le_bytes().to_vec())
        );
        for (row, sequence) in &history {
            assert_eq!(
                verify.get(&primary_key(*row)).unwrap(),
                Some(sequence.to_le_bytes().to_vec())
            );
        }
        let records = verify.range(None, None, 1000).unwrap();
        assert_eq!(records.len(), history.len() + 1);
        let metrics = db.lock_metrics();
        assert_eq!(metrics.waiting, 0);
        println!(
            "clients={clients} acknowledged={} grants={} total_wait_ns={} max_wait_ns={}",
            history.len(),
            metrics.grants,
            metrics.total_wait_nanos,
            metrics.max_wait_nanos
        );
        drop(verify);
        drop(db);
        let mut file = FileDatabase::open(&root.0, 4 * PAGE_SIZE).unwrap();
        assert_eq!(
            file.integrity_check().unwrap().records,
            history.len() as u64 + 1
        );
    }
}
