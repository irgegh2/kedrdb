// SPDX-License-Identifier: Apache-2.0
//! One-owner atomic storage transactions. SQL/session concurrency and segmented
//! WAL lifecycle are separate layers. File callers must synchronize WAL names.
mod private_io;
use crate::{
    Error, Result,
    checksum::crc32c,
    page::PAGE_SIZE,
    storage::{StorageIo, write_all_at},
    tree::{BPlusTree, IntegrityReport, KeyValue},
    wal::{MAX_IMAGES, PageImage, RecoverySummary, WalConfig, WalLog},
};
use private_io::PrivateIo;
use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Instant,
};

pub struct StorageEngine<D: StorageIo, W: StorageIo> {
    data: D,
    wal: WalLog<W>,
    cache_budget: usize,
    page_limit: usize,
    poisoned: bool,
}

/// Inspect an offline checkpoint without recovery or writes. The caller must
/// independently validate that its WAL has the same identity and frontier.
pub fn inspect_checkpoint<I: StorageIo>(
    mut io: I,
    cache_budget: usize,
) -> Result<([u8; 16], u64, IntegrityReport)> {
    let length = io.len()?;
    let mut tree = BPlusTree::open(PrivateIo::new(io, length, 1)?, cache_budget)?;
    Ok((
        tree.database_id(),
        tree.checkpoint_lsn(),
        tree.integrity_check()?,
    ))
}

impl<D: StorageIo, W: StorageIo> StorageEngine<D, W> {
    pub fn create(mut data: D, wal: W, database_id: [u8; 16], cache_budget: usize) -> Result<Self> {
        if data.len()? != 0 {
            return Err(Error::InvalidArgument {
                message: "new engine requires empty data storage",
            });
        }
        let mut wal = WalLog::create(wal, WalConfig::first(database_id))?;
        let private = PrivateIo::new(&mut data, 0, MAX_IMAGES)?;
        let mut tree = BPlusTree::create(private, database_id, cache_budget)?;
        tree.checkpoint()?;
        let mut private = tree.into_io();
        let commit_lsn = wal.planned_commit_lsn(private.images.len())?;
        stamp_metadata(&mut private.images, commit_lsn);
        wal.commit(1, &private.images, private.length)
            .map_err(uncertain)?;
        apply_pages(&mut private.base, &private.images, private.length)
            .and_then(|()| private.base.sync())
            .map_err(uncertain)?;
        drop(private);
        Ok(Self {
            data,
            wal,
            cache_budget,
            page_limit: MAX_IMAGES,
            poisoned: false,
        })
    }
    pub fn recover(
        mut data: D,
        wal: W,
        config: WalConfig,
        cache_budget: usize,
    ) -> Result<(Self, RecoverySummary)> {
        // Validate the complete journal before changing data, and refuse to
        // replay a truncated log over a newer durable metadata frontier.
        let frontier = crate::tree::read_frontier(&mut data)?;
        let (validated, summary) = WalLog::recover(wal, config, |_| Ok(()))?;
        if frontier
            .is_some_and(|(id, lsn)| id != config.database_id || lsn > summary.last_commit_lsn)
        {
            return Err(Error::CorruptWal {
                reason: "journal does not cover durable data frontier",
            });
        }
        if config.previous_commit_lsn > 0
            && frontier.is_none_or(|(_, lsn)| lsn < config.previous_commit_lsn)
        {
            return Err(Error::CorruptStore {
                reason: "missing durable checkpoint base",
            });
        }
        let (mut wal, summary) = WalLog::recover(validated.into_io(), config, |group| {
            apply_pages(&mut data, &group.images, group.final_data_length)
        })?;
        data.sync()?;
        let length = data.len()?;
        let private = PrivateIo::new(&mut data, length, MAX_IMAGES)?;
        let mut tree = BPlusTree::open(private, cache_budget)?;
        if tree.database_id() != config.database_id {
            return Err(Error::CorruptStore {
                reason: "data/WAL database identity mismatch",
            });
        }
        if tree.checkpoint_lsn() != summary.last_commit_lsn {
            return Err(Error::CorruptStore {
                reason: "recovered metadata frontier mismatch",
            });
        }
        tree.integrity_check()?;
        drop(tree);
        wal.clear_uncommitted_tail()?;
        Ok((
            Self {
                data,
                wal,
                cache_budget,
                page_limit: MAX_IMAGES,
                poisoned: false,
            },
            summary,
        ))
    }
    pub fn set_private_page_limit(&mut self, limit: usize) -> Result<()> {
        if limit == 0 || limit > MAX_IMAGES {
            return Err(Error::InvalidArgument {
                message: "private page limit must be 1..256",
            });
        }
        self.page_limit = limit;
        Ok(())
    }
    pub fn begin(&mut self) -> Result<StorageTransaction<'_, D, W>> {
        self.healthy()?;
        let length = self.data.len()?;
        let private = PrivateIo::new(&mut self.data, length, self.page_limit)?;
        let tree = BPlusTree::open(private, self.cache_budget)?;
        Ok(StorageTransaction {
            tree: Some(tree),
            wal: &mut self.wal,
            poisoned: &mut self.poisoned,
            aborted: false,
        })
    }
    pub fn transaction<T>(
        &mut self,
        operation: impl FnOnce(&mut StorageTransaction<'_, D, W>) -> Result<T>,
    ) -> Result<T> {
        let mut tx = self.begin()?;
        let result = operation(&mut tx)?;
        tx.commit()?;
        Ok(result)
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut tx = self.begin()?;
        tx.get(key)
    }
    pub fn integrity_check(&mut self) -> Result<IntegrityReport> {
        let mut tx = self.begin()?;
        tx.integrity_check()
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn into_io(self) -> (D, W) {
        (self.data, self.wal.into_io())
    }
    pub(crate) fn into_parts(self) -> (D, WalLog<W>) {
        (self.data, self.wal)
    }
    pub(crate) fn resume(data: D, wal: WalLog<W>, cache_budget: usize) -> Self {
        Self {
            data,
            wal,
            cache_budget,
            page_limit: MAX_IMAGES,
            poisoned: false,
        }
    }
    pub fn wal_config(&self) -> WalConfig {
        self.wal.config()
    }
    pub fn wal_length(&self) -> u64 {
        self.wal.length()
    }
    pub fn next_transaction_id(&self) -> Result<u64> {
        self.wal.next_transaction_id()
    }
    pub fn last_commit_lsn(&self) -> u64 {
        self.wal.last_commit_lsn()
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
}

pub struct StorageTransaction<'a, D: StorageIo, W: StorageIo> {
    tree: Option<BPlusTree<PrivateIo<&'a mut D>>>,
    wal: &'a mut WalLog<W>,
    poisoned: &'a mut bool,
    aborted: bool,
}
impl<D: StorageIo, W: StorageIo> StorageTransaction<'_, D, W> {
    #[cfg(unix)]
    pub(crate) fn check_interrupt(&mut self) -> Result<()> {
        self.apply(|tree| tree.io_mut().check_interrupt())
    }
    pub fn set_interrupt(&mut self, cancelled: Arc<AtomicBool>, deadline: Instant) {
        self.tree
            .as_mut()
            .expect("active transaction")
            .io_mut()
            .interrupt = Some(crate::interrupt::Interrupt {
            cancelled,
            deadline,
        });
    }
    pub fn abort(&mut self) {
        self.aborted = true;
    }
    fn apply<T>(
        &mut self,
        operation: impl FnOnce(&mut BPlusTree<PrivateIo<&mut D>>) -> Result<T>,
    ) -> Result<T> {
        if self.aborted {
            return Err(Error::TransactionAborted);
        }
        let tree = self.tree.as_mut().expect("active transaction");
        let result = tree
            .io_mut()
            .check_interrupt()
            .and_then(|()| operation(tree))
            .and_then(|value| tree.io_mut().check_interrupt().map(|()| value));
        if result.is_err() {
            self.aborted = true;
        }
        result
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.apply(|tree| tree.get(key))
    }
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.apply(|tree| tree.insert(key, value))
    }
    pub fn replace(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.apply(|tree| tree.replace(key, value))
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.apply(|tree| tree.delete(key))
    }
    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<KeyValue>> {
        self.apply(|tree| tree.range(start, end, limit))
    }
    pub fn integrity_check(&mut self) -> Result<IntegrityReport> {
        self.apply(|tree| tree.integrity_check())
    }
    pub fn rollback(self) {} // Dropping the private overlay performs no base writes.
    pub fn commit(mut self) -> Result<u64> {
        if self.aborted {
            return Err(Error::TransactionAborted);
        }
        let mut tree = self.tree.take().expect("active transaction");
        tree.checkpoint()?; // May fail safely before any durable WAL write.
        let mut private = tree.into_io();
        let commit_lsn = self.wal.planned_commit_lsn(private.images.len())?;
        stamp_metadata(&mut private.images, commit_lsn);
        private.check_interrupt()?; // Last cancellable point, before any WAL append.
        let tx = self.wal.next_transaction_id()?;
        let lsn = match self.wal.commit(tx, &private.images, private.length) {
            Ok(lsn) => lsn,
            Err(error) => {
                if self.wal.is_poisoned() {
                    *self.poisoned = true;
                    return Err(uncertain(error));
                }
                return Err(error);
            }
        };
        if let Err(error) = apply_pages(&mut private.base, &private.images, private.length)
            .and_then(|()| private.base.sync())
        {
            *self.poisoned = true;
            return Err(uncertain(error));
        }
        Ok(lsn)
    }
}

pub(crate) struct ReadView<I: StorageIo> {
    tree: BPlusTree<PrivateIo<I>>,
}
impl<I: StorageIo> ReadView<I> {
    pub(crate) fn check_interrupt(&mut self) -> Result<()> {
        self.tree.io_mut().check_interrupt()
    }
    pub fn open(mut io: I, cache_budget: usize) -> Result<Self> {
        let length = io.len()?;
        Ok(Self {
            tree: BPlusTree::open(PrivateIo::new(io, length, 1)?, cache_budget)?,
        })
    }
    pub fn set_interrupt(&mut self, cancelled: Arc<AtomicBool>, deadline: Instant) {
        self.tree.io_mut().interrupt = Some(crate::interrupt::Interrupt {
            cancelled,
            deadline,
        });
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.tree.io_mut().check_interrupt()?;
        let result = self.tree.get(key)?;
        self.tree.io_mut().check_interrupt()?;
        Ok(result)
    }
    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<KeyValue>> {
        self.tree.io_mut().check_interrupt()?;
        let result = self.tree.range(start, end, limit)?;
        self.tree.io_mut().check_interrupt()?;
        Ok(result)
    }
}
fn uncertain(source: Error) -> Error {
    Error::CommitUncertain {
        source: Box::new(source),
    }
}
pub(crate) fn apply_pages<I: StorageIo>(
    data: &mut I,
    images: &[PageImage],
    length: u64,
) -> Result<()> {
    data.set_len(length)?;
    for image in images {
        write_all_at(data, image.page_id * PAGE_SIZE as u64, &image.bytes)?;
    }
    Ok(())
}
fn stamp_metadata(images: &mut [PageImage], lsn: u64) {
    for image in images.iter_mut().filter(|image| image.page_id < 2) {
        image.bytes[8..10].copy_from_slice(&2u16.to_le_bytes());
        image.bytes[64..72].copy_from_slice(&lsn.to_le_bytes());
        image.bytes[72..80].copy_from_slice(&1u64.to_le_bytes());
        image.bytes[80..84].fill(0);
        let crc = crc32c(&image.bytes);
        image.bytes[80..84].copy_from_slice(&crc.to_le_bytes());
    }
}
