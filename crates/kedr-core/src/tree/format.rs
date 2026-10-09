// SPDX-License-Identifier: Apache-2.0
use crate::{
    Error, Result,
    checksum::crc32c,
    page::{MAX_RECORD_SIZE, PAGE_SIZE},
    record::{Cursor, MAX_KEY_SIZE},
};

const HEADER: usize = 64;
#[derive(Clone)]
pub(super) struct Entry {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}
#[derive(Clone)]
pub(super) enum Node {
    Leaf(Vec<Entry>),
    Branch {
        level: u16,
        keys: Vec<Vec<u8>>,
        children: Vec<u64>,
    },
    Free(u64),
}
fn bad(reason: &'static str) -> Error {
    Error::CorruptStore { reason }
}
impl Node {
    pub fn level(&self) -> u16 {
        match self {
            Self::Branch { level, .. } => *level,
            _ => 0,
        }
    }
    pub fn encoded_len(&self) -> usize {
        HEADER
            + match self {
                Self::Leaf(entries) => entries
                    .iter()
                    .map(|e| 4 + e.key.len() + e.value.len())
                    .sum::<usize>(),
                Self::Branch { keys, .. } => keys.iter().map(|k| 10 + k.len()).sum(),
                Self::Free(_) => 0,
            }
    }
    pub fn encode(&self, id: u64) -> Result<[u8; PAGE_SIZE]> {
        if self.encoded_len() > PAGE_SIZE {
            return Err(Error::PageFull);
        }
        let mut bytes = [0; PAGE_SIZE];
        bytes[..8].copy_from_slice(b"KEDRNODE");
        put16(&mut bytes, 8, 1);
        put16(&mut bytes, 12, HEADER as u16);
        put64(&mut bytes, 16, id);
        put16(&mut bytes, 36, self.encoded_len() as u16);
        put16(&mut bytes, 38, self.level());
        let mut at = HEADER;
        match self {
            Self::Leaf(entries) => {
                put16(&mut bytes, 10, 1);
                put16(&mut bytes, 14, entries.len() as u16);
                for entry in entries {
                    if entry.key.is_empty()
                        || entry.key.len() > MAX_KEY_SIZE
                        || entry.value.len() > MAX_RECORD_SIZE
                    {
                        return Err(bad("invalid leaf record"));
                    }
                    put16(&mut bytes, at, entry.key.len() as u16);
                    put16(&mut bytes, at + 2, entry.value.len() as u16);
                    at += 4;
                    bytes[at..at + entry.key.len()].copy_from_slice(&entry.key);
                    at += entry.key.len();
                    bytes[at..at + entry.value.len()].copy_from_slice(&entry.value);
                    at += entry.value.len();
                }
            }
            Self::Branch {
                level,
                keys,
                children,
            } => {
                if *level == 0 || children.len() != keys.len() + 1 {
                    return Err(bad("branch shape"));
                }
                put16(&mut bytes, 10, 2);
                put16(&mut bytes, 14, keys.len() as u16);
                put64(&mut bytes, 40, children[0]);
                for (key, child) in keys.iter().zip(&children[1..]) {
                    if key.is_empty() || key.len() > MAX_KEY_SIZE {
                        return Err(bad("invalid separator"));
                    }
                    put16(&mut bytes, at, key.len() as u16);
                    at += 2;
                    bytes[at..at + key.len()].copy_from_slice(key);
                    at += key.len();
                    put64(&mut bytes, at, *child);
                    at += 8;
                }
            }
            Self::Free(next) => {
                put16(&mut bytes, 10, 3);
                put64(&mut bytes, 40, *next);
            }
        }
        let checksum = crc32c(&bytes);
        bytes[32..36].copy_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }
    pub fn decode(bytes: &[u8; PAGE_SIZE], id: u64, page_count: u64) -> Result<Self> {
        if &bytes[..8] != b"KEDRNODE"
            || get16(bytes, 8) != 1
            || get16(bytes, 12) != HEADER as u16
            || get64(bytes, 16) != id
            || id < 2
            || id >= page_count
        {
            return Err(bad("node header or identity"));
        }
        if bytes[24..32]
            .iter()
            .chain(bytes[48..64].iter())
            .any(|b| *b != 0)
        {
            return Err(bad("node reserved fields"));
        }
        let mut clean = *bytes;
        clean[32..36].fill(0);
        if crc32c(&clean) != u32::from_le_bytes(bytes[32..36].try_into().expect("width")) {
            return Err(bad("node checksum"));
        }
        let end = usize::from(get16(bytes, 36));
        let count = usize::from(get16(bytes, 14));
        let level = get16(bytes, 38);
        let first = get64(bytes, 40);
        if !(HEADER..=PAGE_SIZE).contains(&end) || bytes[end..].iter().any(|b| *b != 0) {
            return Err(bad("node payload bounds or padding"));
        }
        let mut cursor = Cursor::new(&bytes[HEADER..end]);
        let node = match get16(bytes, 10) {
            1 => {
                if level != 0 || first != 0 || count > (end - HEADER) / 5 {
                    return Err(bad("leaf shape"));
                }
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let key_len = usize::from(cursor.u16()?);
                    let value_len = usize::from(cursor.u16()?);
                    if key_len == 0 || key_len > MAX_KEY_SIZE || value_len > MAX_RECORD_SIZE {
                        return Err(bad("leaf length limit"));
                    }
                    let key = cursor.take(key_len)?.to_vec();
                    let value = cursor.take(value_len)?.to_vec();
                    if entries.last().is_some_and(|e: &Entry| e.key >= key) {
                        return Err(bad("leaf keys not strictly ordered"));
                    }
                    entries.push(Entry { key, value });
                }
                Self::Leaf(entries)
            }
            2 => {
                if level == 0 || level >= 64 || count > (end - HEADER) / 11 {
                    return Err(bad("branch shape"));
                }
                check_child(first, page_count)?;
                let mut keys: Vec<Vec<u8>> = Vec::with_capacity(count);
                let mut children = Vec::with_capacity(count + 1);
                children.push(first);
                for _ in 0..count {
                    let len = usize::from(cursor.u16()?);
                    if len == 0 || len > MAX_KEY_SIZE {
                        return Err(bad("separator length limit"));
                    }
                    let key = cursor.take(len)?.to_vec();
                    let child = cursor.u64()?;
                    check_child(child, page_count)?;
                    if keys.last().is_some_and(|old| *old >= key) {
                        return Err(bad("separator keys not strictly ordered"));
                    }
                    keys.push(key);
                    children.push(child);
                }
                Self::Branch {
                    level,
                    keys,
                    children,
                }
            }
            3 => {
                if count != 0 || level != 0 || end != HEADER || first == id {
                    return Err(bad("free page shape"));
                }
                if first != 0 {
                    check_child(first, page_count)?;
                }
                Self::Free(first)
            }
            _ => return Err(bad("unknown node type")),
        };
        if !cursor.done() {
            return Err(bad("trailing node payload"));
        }
        Ok(node)
    }
    pub fn split(self) -> Result<(Self, Vec<u8>, Self)> {
        match self {
            Self::Leaf(mut entries) => {
                let total: usize = entries
                    .iter()
                    .map(|e| 4 + e.key.len() + e.value.len())
                    .sum();
                let mut left = 0;
                let mut best = None;
                for at in 1..entries.len() {
                    left += 4 + entries[at - 1].key.len() + entries[at - 1].value.len();
                    let right = total - left;
                    if HEADER + left <= PAGE_SIZE && HEADER + right <= PAGE_SIZE {
                        let score = left.abs_diff(right);
                        if best.is_none_or(|(_, old)| score < old) {
                            best = Some((at, score));
                        }
                    }
                }
                let (at, _) = best.ok_or(bad("unsplittable leaf"))?;
                let right = entries.split_off(at);
                let key = right[0].key.clone();
                Ok((Self::Leaf(entries), key, Self::Leaf(right)))
            }
            Self::Branch {
                level,
                mut keys,
                mut children,
            } => {
                let total: usize = keys.iter().map(|k| 10 + k.len()).sum();
                let mut left_size = 0;
                let mut best = None;
                for (at, key) in keys.iter().enumerate() {
                    let right_size = total - left_size - 10 - key.len();
                    if HEADER + left_size <= PAGE_SIZE && HEADER + right_size <= PAGE_SIZE {
                        let score = left_size.abs_diff(right_size);
                        if best.is_none_or(|(_, old)| score < old) {
                            best = Some((at, score));
                        }
                    }
                    left_size += 10 + key.len();
                }
                let (middle, _) = best.ok_or(bad("unsplittable branch"))?;
                let right_keys = keys.split_off(middle + 1);
                let key = keys.pop().ok_or(bad("unsplittable branch"))?;
                let right_children = children.split_off(middle + 1);
                let left = Self::Branch {
                    level,
                    keys,
                    children,
                };
                let right = Self::Branch {
                    level,
                    keys: right_keys,
                    children: right_children,
                };
                if left.encoded_len() > PAGE_SIZE || right.encoded_len() > PAGE_SIZE {
                    return Err(bad("unbalanced branch split"));
                }
                Ok((left, key, right))
            }
            Self::Free(_) => Err(bad("cannot split free page")),
        }
    }
}
fn check_child(id: u64, count: u64) -> Result<()> {
    if id < 2 || id >= count {
        return Err(bad("child pointer bounds"));
    }
    Ok(())
}
pub(super) fn get16(bytes: &[u8], at: usize) -> u16 {
    u16::from_le_bytes(bytes[at..at + 2].try_into().expect("width"))
}
pub(super) fn get64(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().expect("width"))
}
pub(super) fn put16(bytes: &mut [u8], at: usize, value: u16) {
    bytes[at..at + 2].copy_from_slice(&value.to_le_bytes());
}
pub(super) fn put64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}
