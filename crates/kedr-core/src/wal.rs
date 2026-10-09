// SPDX-License-Identifier: Apache-2.0
//! Bounded full-page redo records in one WAL segment. Directory/rotation is a
//! separate layer. Full corrupt records fail closed; only incomplete EOF tails
//! and uncommitted image groups are discarded after successful recovery.

use crate::{
    Error, Result,
    checksum::crc32c,
    page::PAGE_SIZE,
    storage::{StorageIo, read_exact_at, write_all_at},
};
pub const SEGMENT_HEADER: usize = 128;
pub const RECORD_HEADER: usize = 48;
pub const MAX_IMAGES: usize = 256;
const IMAGE_RECORD: usize = RECORD_HEADER + 8 + PAGE_SIZE;
const COMMIT_RECORD: usize = RECORD_HEADER + 32;

#[derive(Clone)]
pub struct PageImage {
    pub page_id: u64,
    pub bytes: [u8; PAGE_SIZE],
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalConfig {
    pub database_id: [u8; 16],
    pub segment_id: u64,
    pub first_lsn: u64,
    pub previous_transaction_id: u64,
    pub previous_commit_lsn: u64,
}
impl WalConfig {
    pub fn first(database_id: [u8; 16]) -> Self {
        Self {
            database_id,
            segment_id: 1,
            first_lsn: 1,
            previous_transaction_id: 0,
            previous_commit_lsn: 0,
        }
    }
    fn validate(&self) -> Result<()> {
        if self.database_id == [0; 16]
            || self.segment_id == 0
            || self.previous_commit_lsn.checked_add(1) != Some(self.first_lsn)
            || self.first_lsn == 0
        {
            return Err(bad("segment configuration"));
        }
        Ok(())
    }
    fn encode(self) -> Result<[u8; SEGMENT_HEADER]> {
        self.validate()?;
        let mut b = [0; SEGMENT_HEADER];
        b[..8].copy_from_slice(b"KEDRWAL1");
        put16(&mut b, 8, 1);
        put16(&mut b, 10, SEGMENT_HEADER as u16);
        b[16..32].copy_from_slice(&self.database_id);
        put64(&mut b, 32, self.segment_id);
        put64(&mut b, 40, self.first_lsn);
        put64(&mut b, 48, self.previous_transaction_id);
        put64(&mut b, 56, self.previous_commit_lsn);
        let checksum = crc32c(&b);
        put32(&mut b, 64, checksum);
        Ok(b)
    }
    fn decode(b: &[u8; SEGMENT_HEADER]) -> Result<Self> {
        if &b[..8] != b"KEDRWAL1"
            || get16(b, 8) != 1
            || get16(b, 10) != SEGMENT_HEADER as u16
            || b[12..16].iter().chain(b[68..].iter()).any(|v| *v != 0)
        {
            return Err(bad("segment header"));
        }
        let mut clean = *b;
        clean[64..68].fill(0);
        if crc32c(&clean) != get32(b, 64) {
            return Err(bad("segment checksum"));
        }
        let config = Self {
            database_id: b[16..32].try_into().expect("width"),
            segment_id: get64(b, 32),
            first_lsn: get64(b, 40),
            previous_transaction_id: get64(b, 48),
            previous_commit_lsn: get64(b, 56),
        };
        config.validate()?;
        Ok(config)
    }
}

pub struct CommittedGroup {
    pub transaction_id: u64,
    pub commit_lsn: u64,
    pub final_data_length: u64,
    pub images: Vec<PageImage>,
}
#[derive(Debug, Clone, Copy)]
pub struct RecoverySummary {
    pub transactions: u64,
    pub last_transaction_id: u64,
    pub last_commit_lsn: u64,
    pub valid_end: u64,
    pub ignored_tail_bytes: u64,
}

pub struct WalLog<I: StorageIo> {
    io: I,
    config: WalConfig,
    end: u64,
    next_lsn: u64,
    last_transaction: u64,
    last_commit: u64,
    poisoned: bool,
    tail_pending: bool,
}
impl<I: StorageIo> WalLog<I> {
    pub fn read_config(io: &mut I) -> Result<WalConfig> {
        let mut header = [0; SEGMENT_HEADER];
        read_exact_at(io, 0, &mut header)?;
        WalConfig::decode(&header)
    }
    pub fn next_record_lsn(&self) -> u64 {
        self.next_lsn
    }
    pub fn planned_commit_lsn(&self, images: usize) -> Result<u64> {
        self.next_lsn
            .checked_add(images as u64)
            .filter(|lsn| *lsn < u64::MAX)
            .ok_or(bad("LSN exhausted"))
    }
    pub fn last_commit_lsn(&self) -> u64 {
        self.last_commit
    }
    pub fn length(&self) -> u64 {
        self.end
    }
    pub fn create(mut io: I, config: WalConfig) -> Result<Self> {
        let bytes = config.encode()?;
        if io.len()? != 0 {
            return Err(Error::InvalidArgument {
                message: "new WAL requires empty exclusive storage",
            });
        }
        write_all_at(&mut io, 0, &bytes)?;
        io.sync()?;
        Ok(Self {
            io,
            config,
            end: SEGMENT_HEADER as u64,
            next_lsn: config.first_lsn,
            last_transaction: config.previous_transaction_id,
            last_commit: config.previous_commit_lsn,
            poisoned: false,
            tail_pending: false,
        })
    }
    /// callback must publish only complete groups. Caller must sync the data file
    /// before clearing the returned tail or acknowledging recovered availability.
    pub fn recover(
        mut io: I,
        expected: WalConfig,
        mut apply: impl FnMut(&CommittedGroup) -> Result<()>,
    ) -> Result<(Self, RecoverySummary)> {
        expected.validate()?;
        let length = io.len()?;
        if length < SEGMENT_HEADER as u64 {
            return Err(bad("truncated segment header"));
        }
        let mut header = [0; SEGMENT_HEADER];
        read_exact_at(&mut io, 0, &mut header)?;
        let config = WalConfig::decode(&header)?;
        if config != expected {
            return Err(bad("wrong database or segment chain"));
        }
        let mut at = SEGMENT_HEADER as u64;
        let mut next_lsn = config.first_lsn;
        let mut last_tx = config.previous_transaction_id;
        let mut last_commit = config.previous_commit_lsn;
        let mut valid_end = at;
        let mut transactions = 0;
        let mut group_tx = 0;
        let mut first_lsn = 0;
        let mut images: Vec<PageImage> = Vec::new();
        while at < length {
            if length - at < RECORD_HEADER as u64 {
                break;
            }
            let mut bytes = [0; IMAGE_RECORD];
            read_exact_at(&mut io, at, &mut bytes[..RECORD_HEADER])?;
            if &bytes[..8] != b"KEDRWREC"
                || get16(&bytes, 8) != 1
                || get32(&bytes, 12) != RECORD_HEADER as u32
                || get32(&bytes, 20) != 0
                || get32(&bytes, 44) != 0
            {
                return Err(bad("record header"));
            }
            let total = get32(&bytes, 16) as usize;
            let kind = get16(&bytes, 10);
            if !matches!((kind, total), (1, IMAGE_RECORD) | (2, COMMIT_RECORD)) {
                return Err(bad("record kind or length"));
            }
            if length - at < total as u64 {
                break;
            }
            read_exact_at(
                &mut io,
                at + RECORD_HEADER as u64,
                &mut bytes[RECORD_HEADER..total],
            )?;
            let stored = get32(&bytes, 40);
            bytes[40..44].fill(0);
            if crc32c(&bytes[..total]) != stored {
                return Err(bad("record checksum"));
            }
            let lsn = get64(&bytes, 24);
            let tx = get64(&bytes, 32);
            if lsn != next_lsn || Some(tx) != last_tx.checked_add(1) {
                return Err(bad("LSN or transaction order"));
            }
            next_lsn = next_lsn.checked_add(1).ok_or(bad("LSN exhausted"))?;
            if kind == 1 {
                if images.is_empty() {
                    group_tx = tx;
                    first_lsn = lsn;
                } else if group_tx != tx {
                    return Err(bad("interleaved transactions"));
                }
                if images.len() == MAX_IMAGES {
                    return Err(bad("transaction image limit"));
                }
                let page_id = get64(&bytes, RECORD_HEADER);
                if images.iter().any(|image| image.page_id == page_id) {
                    return Err(bad("duplicate page image"));
                }
                images.try_reserve(1).map_err(|_| Error::Allocation {
                    requested: (images.len() + 1) * std::mem::size_of::<PageImage>(),
                })?;
                images.push(PageImage {
                    page_id,
                    bytes: bytes[RECORD_HEADER + 8..total]
                        .try_into()
                        .expect("page width"),
                });
            } else {
                if images.is_empty()
                    || tx != group_tx
                    || get64(&bytes, 48) != first_lsn
                    || get64(&bytes, 56) != lsn - 1
                    || get32(&bytes, 64) as usize != images.len()
                    || get32(&bytes, 68) != 0
                {
                    return Err(bad("COMMIT does not match images"));
                }
                let data_length = get64(&bytes, 72);
                check_images(&images, data_length)?;
                let group = CommittedGroup {
                    transaction_id: tx,
                    commit_lsn: lsn,
                    final_data_length: data_length,
                    images: std::mem::take(&mut images),
                };
                apply(&group)?;
                last_tx = tx;
                last_commit = lsn;
                transactions += 1;
                valid_end = at + total as u64;
            }
            at += total as u64;
        }
        let next_lsn = last_commit.checked_add(1).ok_or(bad("LSN exhausted"))?;
        let summary = RecoverySummary {
            transactions,
            last_transaction_id: last_tx,
            last_commit_lsn: last_commit,
            valid_end,
            ignored_tail_bytes: length - valid_end,
        };
        Ok((
            Self {
                io,
                config,
                end: valid_end,
                next_lsn,
                last_transaction: last_tx,
                last_commit,
                poisoned: false,
                tail_pending: length != valid_end,
            },
            summary,
        ))
    }
    /// Call only after recovered data sync. A failed truncate/sync poisons WAL.
    pub fn clear_uncommitted_tail(&mut self) -> Result<()> {
        self.healthy()?;
        let result = (|| {
            self.io.set_len(self.end)?;
            self.io.sync()
        })();
        if result.is_err() {
            self.poisoned = true;
        } else {
            self.tail_pending = false;
        }
        result
    }
    pub fn next_transaction_id(&self) -> Result<u64> {
        self.last_transaction
            .checked_add(1)
            .ok_or(bad("transaction ID exhausted"))
    }
    pub fn config(&self) -> WalConfig {
        self.config
    }
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }
    pub fn into_io(self) -> I {
        self.io
    }
    pub fn commit(
        &mut self,
        transaction_id: u64,
        images: &[PageImage],
        data_length: u64,
    ) -> Result<u64> {
        self.healthy()?;
        if self.tail_pending {
            return Err(Error::RecoveryRequired);
        }
        check_images(images, data_length)?;
        if transaction_id != self.next_transaction_id()? {
            return Err(bad("transaction ID must be next"));
        }
        let commit_lsn = self
            .next_lsn
            .checked_add(images.len() as u64)
            .filter(|v| *v < u64::MAX)
            .ok_or(bad("LSN exhausted"))?;
        let first = self.next_lsn;
        let result = (|| {
            for image in images {
                let mut bytes = [0; IMAGE_RECORD];
                record_header(&mut bytes, 1, IMAGE_RECORD, self.next_lsn, transaction_id);
                put64(&mut bytes, 48, image.page_id);
                bytes[56..].copy_from_slice(&image.bytes);
                seal_record(&mut bytes);
                write_all_at(&mut self.io, self.end, &bytes)?;
                self.end += IMAGE_RECORD as u64;
                self.next_lsn += 1;
            }
            let mut bytes = [0; COMMIT_RECORD];
            record_header(&mut bytes, 2, COMMIT_RECORD, commit_lsn, transaction_id);
            put64(&mut bytes, 48, first);
            put64(&mut bytes, 56, commit_lsn - 1);
            put32(&mut bytes, 64, images.len() as u32);
            put64(&mut bytes, 72, data_length);
            seal_record(&mut bytes);
            write_all_at(&mut self.io, self.end, &bytes)?;
            self.end += COMMIT_RECORD as u64;
            self.io.sync()?;
            self.next_lsn = commit_lsn + 1;
            self.last_transaction = transaction_id;
            self.last_commit = commit_lsn;
            Ok(commit_lsn)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn healthy(&self) -> Result<()> {
        if self.poisoned {
            return Err(Error::RecoveryRequired);
        }
        Ok(())
    }
}

fn check_images(images: &[PageImage], data_length: u64) -> Result<()> {
    if images.is_empty()
        || images.len() > MAX_IMAGES
        || data_length < 3 * PAGE_SIZE as u64
        || !data_length.is_multiple_of(PAGE_SIZE as u64)
    {
        return Err(bad("transaction images or file length"));
    }
    for (at, image) in images.iter().enumerate() {
        if image.page_id >= data_length / PAGE_SIZE as u64
            || images[..at].iter().any(|old| old.page_id == image.page_id)
        {
            return Err(bad("page ID bounds or duplicate"));
        }
    }
    Ok(())
}
fn record_header(bytes: &mut [u8], kind: u16, length: usize, lsn: u64, tx: u64) {
    bytes[..8].copy_from_slice(b"KEDRWREC");
    put16(bytes, 8, 1);
    put16(bytes, 10, kind);
    put32(bytes, 12, RECORD_HEADER as u32);
    put32(bytes, 16, length as u32);
    put64(bytes, 24, lsn);
    put64(bytes, 32, tx);
}
fn seal_record(bytes: &mut [u8]) {
    bytes[40..44].fill(0);
    let checksum = crc32c(bytes);
    put32(bytes, 40, checksum);
}
fn bad(reason: &'static str) -> Error {
    Error::CorruptWal { reason }
}
fn get16(b: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(b[at..at + 2].try_into().expect("width"))
}
fn get32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("width"))
}
fn get64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("width"))
}
fn put16(b: &mut [u8], at: usize, v: u16) {
    b[at..at + 2].copy_from_slice(&v.to_le_bytes());
}
fn put32(b: &mut [u8], at: usize, v: u32) {
    b[at..at + 4].copy_from_slice(&v.to_le_bytes());
}
fn put64(b: &mut [u8], at: usize, v: u64) {
    b[at..at + 8].copy_from_slice(&v.to_le_bytes());
}
