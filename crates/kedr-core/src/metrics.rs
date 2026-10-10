// SPDX-License-Identifier: Apache-2.0
//! Process cumulative storage counters. No SQL, paths or identifiers are collected.
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
static READ: AtomicU64 = AtomicU64::new(0);
static DATA_WRITE: AtomicU64 = AtomicU64::new(0);
static WAL_WRITE: AtomicU64 = AtomicU64::new(0);
static SYNCS: AtomicU64 = AtomicU64::new(0);
static SYNC_NS: AtomicU64 = AtomicU64::new(0);
static IO_ERRORS: AtomicU64 = AtomicU64::new(0);
static CHECKPOINTS: AtomicU64 = AtomicU64::new(0);
static CHECKPOINT_NS: AtomicU64 = AtomicU64::new(0);
static CHECKPOINT_ERRORS: AtomicU64 = AtomicU64::new(0);
#[derive(Clone, Copy, Debug)]
pub struct StorageMetrics {
    pub read_bytes: u64,
    pub data_write_bytes: u64,
    pub wal_write_bytes: u64,
    pub fsync_calls: u64,
    pub fsync_nanos: u64,
    pub io_errors: u64,
    pub checkpoints: u64,
    pub checkpoint_nanos: u64,
    pub checkpoint_errors: u64,
}
pub fn storage_metrics() -> StorageMetrics {
    let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
    StorageMetrics {
        read_bytes: get(&READ),
        data_write_bytes: get(&DATA_WRITE),
        wal_write_bytes: get(&WAL_WRITE),
        fsync_calls: get(&SYNCS),
        fsync_nanos: get(&SYNC_NS),
        io_errors: get(&IO_ERRORS),
        checkpoints: get(&CHECKPOINTS),
        checkpoint_nanos: get(&CHECKPOINT_NS),
        checkpoint_errors: get(&CHECKPOINT_ERRORS),
    }
}
pub(crate) fn read(result: &std::io::Result<usize>) {
    record(&READ, result);
}
pub(crate) fn write(wal: bool, result: &std::io::Result<usize>) {
    record(if wal { &WAL_WRITE } else { &DATA_WRITE }, result);
}
fn record(counter: &AtomicU64, result: &std::io::Result<usize>) {
    match result {
        Ok(bytes) => {
            counter.fetch_add(*bytes as u64, Ordering::Relaxed);
        }
        Err(_) => {
            IO_ERRORS.fetch_add(1, Ordering::Relaxed);
        }
    }
}
pub(crate) fn sync(began: Instant, ok: bool) {
    SYNCS.fetch_add(1, Ordering::Relaxed);
    SYNC_NS.fetch_add(
        began.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );
    if !ok {
        IO_ERRORS.fetch_add(1, Ordering::Relaxed);
    }
}
pub(crate) fn checkpoint(began: Instant, ok: bool) {
    CHECKPOINTS.fetch_add(1, Ordering::Relaxed);
    CHECKPOINT_NS.fetch_add(
        began.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        Ordering::Relaxed,
    );
    if !ok {
        CHECKPOINT_ERRORS.fetch_add(1, Ordering::Relaxed);
    }
}
