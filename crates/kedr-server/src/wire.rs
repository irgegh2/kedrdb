// SPDX-License-Identifier: Apache-2.0
//! Bounded PostgreSQL v3 framing; no allocation from an unchecked length.
use kedr_core::{
    Error,
    record::Value,
    sql::{DataType, ResultColumn},
};
use std::{io, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const MAX_FRAME: usize = 256 * 1024;
pub(crate) type Result<T> = std::result::Result<T, Fault>;
#[derive(Debug)]
pub(crate) struct Fault {
    pub state: &'static str,
    pub message: String,
}
impl Fault {
    pub fn new(state: &'static str, message: impl Into<String>) -> Self {
        Self {
            state,
            message: message.into(),
        }
    }
}
impl From<io::Error> for Fault {
    fn from(value: io::Error) -> Self {
        Self::new("08006", value.to_string())
    }
}
impl From<Error> for Fault {
    fn from(error: Error) -> Self {
        let state = match &error {
            Error::Sql { state, .. } => *state,
            Error::TransactionAborted => "25P02",
            Error::ReadOnlyTransaction => "25006",
            Error::LockTimeout => "55P03",
            Error::Cancelled => "57014",
            Error::TransactionTimeout => "25P03",
            Error::InvalidTransactionState => "25000",
            Error::CommitUncertain { .. } => "40003",
            Error::LimitExceeded { .. } | Error::RecordTooLarge { .. } => "54000",
            _ => "58000",
        };
        Self::new(state, error.to_string())
    }
}
pub(crate) fn protocol() -> Fault {
    Fault::new("08P01", "invalid or unsupported wire message")
}
pub(crate) async fn frame(reader: &mut (impl AsyncRead + Unpin)) -> Result<(u8, Vec<u8>)> {
    frame_limited(reader, MAX_FRAME).await
}
pub(crate) async fn frame_limited(
    reader: &mut (impl AsyncRead + Unpin),
    limit: usize,
) -> Result<(u8, Vec<u8>)> {
    let tag = tokio::time::timeout(Duration::from_secs(300), reader.read_u8())
        .await
        .map_err(|_| Fault::new("08006", "client idle timeout"))??;
    tokio::time::timeout(Duration::from_secs(5), async {
        let length = reader.read_u32().await? as usize;
        if !(4..=limit).contains(&length) {
            return Err(protocol());
        }
        let mut bytes = vec![0; length - 4];
        reader.read_exact(&mut bytes).await?;
        Ok((tag, bytes))
    })
    .await
    .map_err(|_| Fault::new("08006", "partial message timeout"))?
}
pub(crate) async fn startup(reader: &mut (impl AsyncRead + Unpin)) -> Result<Vec<u8>> {
    let length = reader.read_u32().await? as usize;
    if !(8..=8192).contains(&length) {
        return Err(protocol());
    }
    let mut bytes = vec![0; length - 4];
    reader.read_exact(&mut bytes).await?;
    Ok(bytes)
}
pub(crate) async fn send(
    writer: &mut (impl AsyncWrite + Unpin),
    tag: u8,
    bytes: &[u8],
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        writer.write_u8(tag).await?;
        writer.write_u32((bytes.len() + 4) as u32).await?;
        writer.write_all(bytes).await?;
        Ok::<_, io::Error>(())
    })
    .await
    .map_err(|_| Fault::new("08006", "client write timeout"))??;
    Ok(())
}
pub(crate) fn cstring(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(0);
}
pub(crate) async fn error(
    writer: &mut (impl AsyncWrite + Unpin),
    fault: &Fault,
    fatal: bool,
) -> Result<()> {
    let mut bytes = vec![b'S'];
    cstring(&mut bytes, if fatal { "FATAL" } else { "ERROR" });
    bytes.push(b'C');
    cstring(&mut bytes, fault.state);
    bytes.push(b'M');
    // A message can contain SQL names but never authentication payloads or verifiers.
    cstring(&mut bytes, &fault.message.replace('\0', "?"));
    bytes.push(0);
    send(writer, b'E', &bytes).await
}
pub(crate) struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}
impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }
    pub fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self.pos.checked_add(size).ok_or_else(protocol)?;
        let bytes = self.bytes.get(self.pos..end).ok_or_else(protocol)?;
        self.pos = end;
        Ok(bytes)
    }
    pub fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(
            self.take(2)?.try_into().map_err(|_| protocol())?,
        ))
    }
    pub fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(
            self.take(4)?.try_into().map_err(|_| protocol())?,
        ))
    }
    pub fn string(&mut self, max: usize) -> Result<String> {
        let size = self.bytes[self.pos..]
            .iter()
            .position(|byte| *byte == 0)
            .ok_or_else(protocol)?;
        if size > max {
            return Err(Fault::new("54000", "wire string limit"));
        }
        let text = std::str::from_utf8(self.take(size)?)
            .map_err(|_| protocol())?
            .to_owned();
        self.take(1)?;
        Ok(text)
    }
    pub fn end(&self) -> Result<()> {
        if self.pos == self.bytes.len() {
            Ok(())
        } else {
            Err(protocol())
        }
    }
    pub fn count(&mut self) -> Result<usize> {
        let n = self.i16()?;
        if !(0..=256).contains(&n) {
            Err(protocol())
        } else {
            Ok(n as usize)
        }
    }
}
pub(crate) fn oid(kind: DataType) -> i32 {
    match kind {
        DataType::Int => 23,
        DataType::BigInt => 20,
        DataType::Text => 25,
        DataType::Boolean => 16,
    }
}
pub(crate) fn kind(oid: i32) -> Result<Option<DataType>> {
    match oid {
        0 => Ok(None),
        21 | 23 => Ok(Some(DataType::Int)),
        20 => Ok(Some(DataType::BigInt)),
        25 => Ok(Some(DataType::Text)),
        16 => Ok(Some(DataType::Boolean)),
        _ => Err(Fault::new("0A000", "unsupported parameter OID")),
    }
}
pub(crate) fn decode(bytes: &[u8], kind: DataType, binary: bool) -> Result<Value> {
    if binary {
        return match kind {
            DataType::Int => Ok(Value::Int(i32::from_be_bytes(
                bytes.try_into().map_err(|_| protocol())?,
            ))),
            DataType::BigInt => Ok(Value::BigInt(i64::from_be_bytes(
                bytes.try_into().map_err(|_| protocol())?,
            ))),
            DataType::Boolean => match bytes {
                [0] => Ok(Value::Bool(false)),
                [1] => Ok(Value::Bool(true)),
                _ => Err(protocol()),
            },
            DataType::Text => Ok(Value::Text(
                std::str::from_utf8(bytes)
                    .map_err(|_| protocol())?
                    .to_owned(),
            )),
        };
    }
    let value = std::str::from_utf8(bytes).map_err(|_| protocol())?;
    let invalid = || Fault::new("22P02", "invalid text parameter representation");
    match kind {
        DataType::Int => value.parse().map(Value::Int).map_err(|_| invalid()),
        DataType::BigInt => value.parse().map(Value::BigInt).map_err(|_| invalid()),
        DataType::Text => Ok(Value::Text(value.to_owned())),
        DataType::Boolean => match value.to_ascii_lowercase().as_str() {
            "t" | "true" | "1" => Ok(Value::Bool(true)),
            "f" | "false" | "0" => Ok(Value::Bool(false)),
            _ => Err(invalid()),
        },
    }
}
pub(crate) fn description(columns: &[ResultColumn], formats: &[i16]) -> Vec<u8> {
    let mut bytes = (columns.len() as i16).to_be_bytes().to_vec();
    for (index, column) in columns.iter().enumerate() {
        cstring(&mut bytes, &column.name);
        bytes.extend_from_slice(&0i32.to_be_bytes());
        bytes.extend_from_slice(&0i16.to_be_bytes());
        bytes.extend_from_slice(&oid(column.data_type).to_be_bytes());
        let size: i16 = match column.data_type {
            DataType::Int => 4,
            DataType::BigInt => 8,
            DataType::Boolean => 1,
            DataType::Text => -1,
        };
        bytes.extend_from_slice(&size.to_be_bytes());
        bytes.extend_from_slice(&(-1i32).to_be_bytes());
        bytes.extend_from_slice(&formats.get(index).copied().unwrap_or(0).to_be_bytes());
    }
    bytes
}
pub(crate) fn row(values: &[Value], formats: &[i16]) -> Vec<u8> {
    let mut bytes = (values.len() as i16).to_be_bytes().to_vec();
    for (index, value) in values.iter().enumerate() {
        if matches!(value, Value::Null) {
            bytes.extend_from_slice(&(-1i32).to_be_bytes());
            continue;
        }
        let binary = formats.get(index).copied().unwrap_or(0) == 1;
        let value = match value {
            Value::Int(n) if binary => n.to_be_bytes().to_vec(),
            Value::BigInt(n) if binary => n.to_be_bytes().to_vec(),
            Value::Bool(n) if binary => vec![u8::from(*n)],
            Value::Int(n) => n.to_string().into_bytes(),
            Value::BigInt(n) => n.to_string().into_bytes(),
            Value::Bool(n) => {
                if *n {
                    b"t".to_vec()
                } else {
                    b"f".to_vec()
                }
            }
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Null => unreachable!(),
        };
        bytes.extend_from_slice(&(value.len() as i32).to_be_bytes());
        bytes.extend(value);
    }
    bytes
}
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn allocation_limits_and_partial_eof() {
        for length in [0u32, 3, u32::MAX, MAX_FRAME as u32 + 1] {
            let mut bytes = vec![b'Q'];
            bytes.extend_from_slice(&length.to_be_bytes());
            assert!(frame(&mut bytes.as_slice()).await.is_err());
        }
        assert!(frame(&mut b"Q\0\0\0\x08ab".as_slice()).await.is_err());
        assert!(
            startup(&mut u32::MAX.to_be_bytes().as_slice())
                .await
                .is_err()
        );
    }
    #[test]
    fn counts_strings_and_binary_are_bounded() {
        for bytes in [b"\xff\xff".as_slice(), b"\x01\x01", b"\xff\x00"] {
            assert!(Cursor::new(bytes).count().is_err());
        }
        assert!(Cursor::new(b"\xff\0").string(64).is_err());
        assert!(decode(&[2], DataType::Boolean, true).is_err());
        assert!(decode(&[1, 2, 3], DataType::Int, true).is_err());
        assert!(decode(b"2147483648", DataType::Int, false).is_err());
        assert_eq!(
            decode(b"-9223372036854775808", DataType::BigInt, false).unwrap(),
            Value::BigInt(i64::MIN)
        );
    }
}
