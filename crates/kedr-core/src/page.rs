// SPDX-License-Identifier: Apache-2.0
//! Versioned slotted-page codec. This is not a page store or a transaction layer.
//! Records are opaque bytes; typed SQL rows and B+ tree nodes are separate codecs.

use crate::{Error, Result, checksum::crc32c};

pub const PAGE_SIZE: usize = 16 * 1024;
pub const HEADER_SIZE: usize = 64;
pub const SLOT_SIZE: usize = 16;
pub const MAX_RECORD_SIZE: usize = 8 * 1024;
pub const FORMAT_VERSION: u16 = 1;
const MAGIC: &[u8; 8] = b"KEDRPAGE";
const ROW_PAGE: u16 = 1;
const DELETED: u16 = 1;

/// Validated in-memory page. Slots stay stable and are never reused.
/// Delete clears the record, but space reclamation/compaction is not implemented.
#[derive(Clone)]
pub struct SlottedPage {
    bytes: [u8; PAGE_SIZE],
}

impl SlottedPage {
    /// IDs 0 and 1 are reserved for future metadata copies.
    pub fn new(page_id: u64, page_lsn: u64) -> Result<Self> {
        if page_id < 2 {
            return Err(Error::InvalidArgument {
                message: "data page ID must be at least 2",
            });
        }
        let mut bytes = [0; PAGE_SIZE];
        bytes[..8].copy_from_slice(MAGIC);
        put_u16(&mut bytes, 8, FORMAT_VERSION);
        put_u16(&mut bytes, 10, ROW_PAGE);
        put_u16(&mut bytes, 12, HEADER_SIZE as u16);
        put_u64(&mut bytes, 16, page_id);
        put_u64(&mut bytes, 24, page_lsn);
        put_u16(&mut bytes, 38, HEADER_SIZE as u16);
        put_u16(&mut bytes, 40, PAGE_SIZE as u16);
        Ok(Self { bytes })
    }

    pub fn page_id(&self) -> u64 {
        read_u64(&self.bytes, 16)
    }
    pub fn page_lsn(&self) -> u64 {
        read_u64(&self.bytes, 24)
    }
    pub fn slot_count(&self) -> u16 {
        read_u16(&self.bytes, 36)
    }

    /// Includes space needed by a new slot; not a maximum record length.
    pub fn free_bytes(&self) -> usize {
        usize::from(read_u16(&self.bytes, 40)) - usize::from(read_u16(&self.bytes, 38))
    }

    /// Failure leaves this page unchanged. Empty records are distinct from deletion.
    pub fn insert(&mut self, record: &[u8]) -> Result<u16> {
        if record.len() > MAX_RECORD_SIZE {
            return Err(Error::RecordTooLarge {
                length: record.len(),
                limit: MAX_RECORD_SIZE,
            });
        }
        if record.len() + SLOT_SIZE > self.free_bytes() {
            return Err(Error::PageFull);
        }
        let slot = self.slot_count();
        let start = usize::from(read_u16(&self.bytes, 38));
        let end = usize::from(read_u16(&self.bytes, 40)) - record.len();
        self.bytes[end..end + record.len()].copy_from_slice(record);
        put_u16(&mut self.bytes, start, end as u16);
        put_u16(&mut self.bytes, start + 2, record.len() as u16);
        // Remaining slot fields were zero in the free area.
        put_u16(&mut self.bytes, 36, slot + 1);
        put_u16(&mut self.bytes, 38, (start + SLOT_SIZE) as u16);
        put_u16(&mut self.bytes, 40, end as u16);
        Ok(slot)
    }

    /// None means a deleted slot; an out-of-range slot is an error.
    pub fn get(&self, slot: u16) -> Result<Option<&[u8]>> {
        let at = self.slot_offset(slot)?;
        if read_u16(&self.bytes, at + 4) == DELETED {
            return Ok(None);
        }
        let offset = usize::from(read_u16(&self.bytes, at));
        let length = usize::from(read_u16(&self.bytes, at + 2));
        Ok(Some(&self.bytes[offset..offset + length]))
    }

    /// Returns false for an already deleted slot. Does not reclaim its space.
    pub fn delete(&mut self, slot: u16) -> Result<bool> {
        let at = self.slot_offset(slot)?;
        if read_u16(&self.bytes, at + 4) == DELETED {
            return Ok(false);
        }
        let offset = usize::from(read_u16(&self.bytes, at));
        let length = usize::from(read_u16(&self.bytes, at + 2));
        self.bytes[offset..offset + length].fill(0);
        self.bytes[at..at + SLOT_SIZE].fill(0);
        put_u16(&mut self.bytes, at + 4, DELETED);
        Ok(true)
    }

    pub fn encode(&self) -> [u8; PAGE_SIZE] {
        let mut bytes = self.bytes;
        bytes[32..36].fill(0);
        let checksum = crc32c(&bytes);
        bytes[32..36].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    /// Validates all offsets before exposing any record. No heap allocations.
    /// The expected ID prevents accepting a valid page at the wrong file offset.
    pub fn decode(bytes: &[u8], expected_page_id: u64) -> Result<Self> {
        let bad = |reason| Error::CorruptPage { reason };
        if bytes.len() != PAGE_SIZE {
            return Err(bad("wrong page size"));
        }
        if &bytes[..8] != MAGIC {
            return Err(bad("magic"));
        }
        if read_u16(bytes, 8) != FORMAT_VERSION {
            return Err(bad("unsupported format version"));
        }
        if read_u16(bytes, 10) != ROW_PAGE {
            return Err(bad("unsupported page type"));
        }
        if read_u16(bytes, 12) != HEADER_SIZE as u16 {
            return Err(bad("header size"));
        }
        if read_u16(bytes, 14) != 0
            || read_u16(bytes, 42) != 0
            || bytes[44..64].iter().any(|b| *b != 0)
        {
            return Err(bad("header flags or reserved fields"));
        }
        if expected_page_id < 2 || read_u64(bytes, 16) != expected_page_id {
            return Err(bad("page ID"));
        }
        let stored_checksum =
            u32::from_le_bytes(bytes[32..36].try_into().expect("fixed-width field"));
        let mut owned = [0; PAGE_SIZE];
        owned.copy_from_slice(bytes);
        owned[32..36].fill(0);
        if crc32c(&owned) != stored_checksum {
            return Err(bad("checksum"));
        }
        let count = usize::from(read_u16(bytes, 36));
        let start = usize::from(read_u16(bytes, 38));
        let end = usize::from(read_u16(bytes, 40));
        if start != HEADER_SIZE + count * SLOT_SIZE || start > end || end > PAGE_SIZE {
            return Err(bad("free space bounds or slot count"));
        }
        if bytes[start..end].iter().any(|b| *b != 0) {
            return Err(bad("nonzero free space"));
        }
        // A bit per byte bounds validation work and catches overlap without sorting.
        let mut occupied = [0u64; PAGE_SIZE / 64];
        for slot in 0..count {
            let at = HEADER_SIZE + slot * SLOT_SIZE;
            let offset = usize::from(read_u16(bytes, at));
            let length = usize::from(read_u16(bytes, at + 2));
            let flags = read_u16(bytes, at + 4);
            if bytes[at + 6..at + SLOT_SIZE].iter().any(|b| *b != 0) {
                return Err(bad("slot reserved fields"));
            }
            match flags {
                DELETED => {
                    if offset != 0 || length != 0 {
                        return Err(bad("deleted slot payload"));
                    }
                }
                0 => {
                    if length > MAX_RECORD_SIZE || offset < end || offset + length > PAGE_SIZE {
                        return Err(bad("record bounds"));
                    }
                    for byte in offset..offset + length {
                        let mask = 1u64 << (byte % 64);
                        if occupied[byte / 64] & mask != 0 {
                            return Err(bad("overlapping records"));
                        }
                        occupied[byte / 64] |= mask;
                    }
                }
                _ => return Err(bad("unsupported slot flags")),
            }
        }
        // Deleted payload and all gaps must be cleared, so stale data is not exposed.
        for byte in end..PAGE_SIZE {
            if occupied[byte / 64] & (1u64 << (byte % 64)) == 0 && bytes[byte] != 0 {
                return Err(bad("nonzero unused payload"));
            }
        }
        Ok(Self { bytes: owned })
    }

    fn slot_offset(&self, slot: u16) -> Result<usize> {
        let count = self.slot_count();
        if slot >= count {
            return Err(Error::InvalidSlot { slot, count });
        }
        Ok(HEADER_SIZE + usize::from(slot) * SLOT_SIZE)
    }
}

fn read_u16(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().expect("fixed-width field"))
}
fn read_u64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("fixed-width field"))
}
fn put_u16(bytes: &mut [u8], at: usize, value: u16) {
    bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
}
fn put_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}
