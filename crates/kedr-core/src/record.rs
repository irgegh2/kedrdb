// SPDX-License-Identifier: Apache-2.0
//! Explicit typed row codec and order-preserving keys; no Rust ABI serialization.
use crate::{Error, Result, page::MAX_RECORD_SIZE};

pub const MAX_COLUMNS: usize = 256;
pub const MAX_KEY_SIZE: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Value {
    Null,
    Int(i32),
    BigInt(i64),
    Bool(bool),
    Text(String),
}

fn bad(reason: &'static str) -> Error {
    Error::InvalidRecord { reason }
}

pub fn encode_row(values: &[Value]) -> Result<Vec<u8>> {
    if values.len() > MAX_COLUMNS {
        return Err(bad("too many columns"));
    }
    let mut size = 3;
    for value in values {
        size += match value {
            Value::Null => 1,
            Value::Int(_) => 5,
            Value::BigInt(_) => 9,
            Value::Bool(_) => 2,
            Value::Text(s) => {
                if s.len() > MAX_RECORD_SIZE {
                    return Err(bad("text exceeds row limit"));
                }
                5 + s.len()
            }
        };
        if size > MAX_RECORD_SIZE {
            return Err(Error::RecordTooLarge {
                length: size,
                limit: MAX_RECORD_SIZE,
            });
        }
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(size)
        .map_err(|_| Error::Allocation { requested: size })?;
    bytes.push(1);
    bytes.extend_from_slice(&(values.len() as u16).to_le_bytes());
    for value in values {
        match value {
            Value::Null => bytes.push(0),
            Value::Int(v) => {
                bytes.push(1);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::BigInt(v) => {
                bytes.push(2);
                bytes.extend_from_slice(&v.to_le_bytes());
            }
            Value::Bool(v) => {
                bytes.push(3);
                bytes.push(u8::from(*v));
            }
            Value::Text(v) => {
                bytes.push(4);
                bytes.extend_from_slice(&(v.len() as u32).to_le_bytes());
                bytes.extend_from_slice(v.as_bytes());
            }
        }
    }
    Ok(bytes)
}

pub fn decode_row(bytes: &[u8]) -> Result<Vec<Value>> {
    if bytes.len() > MAX_RECORD_SIZE {
        return Err(bad("row exceeds limit"));
    }
    let mut cursor = Cursor::new(bytes);
    if cursor.byte()? != 1 {
        return Err(bad("unsupported row version"));
    }
    let count = usize::from(cursor.u16()?);
    if count > MAX_COLUMNS {
        return Err(bad("too many columns"));
    }
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| Error::Allocation {
            requested: count * std::mem::size_of::<Value>(),
        })?;
    for _ in 0..count {
        values.push(match cursor.byte()? {
            0 => Value::Null,
            1 => Value::Int(i32::from_le_bytes(
                cursor.take(4)?.try_into().expect("width"),
            )),
            2 => Value::BigInt(i64::from_le_bytes(
                cursor.take(8)?.try_into().expect("width"),
            )),
            3 => match cursor.byte()? {
                0 => Value::Bool(false),
                1 => Value::Bool(true),
                _ => return Err(bad("invalid boolean")),
            },
            4 => {
                let len = u32::from_le_bytes(cursor.take(4)?.try_into().expect("width")) as usize;
                let text =
                    std::str::from_utf8(cursor.take(len)?).map_err(|_| bad("invalid UTF-8"))?;
                Value::Text(text.to_owned())
            }
            _ => return Err(bad("unknown value tag")),
        });
    }
    if !cursor.done() {
        return Err(bad("trailing row bytes"));
    }
    Ok(values)
}

/// Lexicographic byte order is signed BIGINT order.
pub fn primary_key(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1u64 << 63)).to_be_bytes()
}

/// Nulls sort first. PK suffix distinguishes duplicate secondary values.
/// SQL ORDER BY NULLS LAST is a separate executor policy.
pub fn secondary_key(value: &Value, primary: i64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match value {
        Value::Null => bytes.push(0),
        Value::Int(v) => {
            bytes.push(1);
            bytes.extend_from_slice(&((*v as u32) ^ (1u32 << 31)).to_be_bytes());
        }
        Value::BigInt(v) => {
            bytes.push(2);
            bytes.extend_from_slice(&primary_key(*v));
        }
        Value::Bool(v) => {
            bytes.push(3);
            bytes.push(u8::from(*v));
        }
        Value::Text(s) => {
            if s.len() > MAX_KEY_SIZE {
                return Err(bad("indexed text exceeds key limit"));
            }
            bytes.push(4);
            for b in s.bytes() {
                bytes.push(b);
                if b == 0 {
                    bytes.push(0xff);
                }
            }
            bytes.extend_from_slice(&[0, 0]);
        }
    }
    bytes.extend_from_slice(&primary_key(primary));
    if bytes.len() > MAX_KEY_SIZE {
        return Err(bad("encoded index key exceeds limit"));
    }
    Ok(bytes)
}

pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    at: usize,
}
impl<'a> Cursor<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }
    pub(crate) fn take(&mut self, length: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(length).ok_or(bad("length overflow"))?;
        if end > self.bytes.len() {
            return Err(bad("truncated record"));
        }
        let result = &self.bytes[self.at..end];
        self.at = end;
        Ok(result)
    }
    pub(crate) fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("width")))
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("width")))
    }
    pub(crate) fn done(&self) -> bool {
        self.at == self.bytes.len()
    }
}
