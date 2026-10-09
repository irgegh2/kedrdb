// SPDX-License-Identifier: Apache-2.0
use crate::{Error, Operation, Result};

#[cfg(unix)]
mod file;
#[cfg(unix)]
pub use file::FileStorage;

/// Positional file I/O. Reads/writes may be short; callers must use exact helpers.
///
/// `sync` requests synchronization of this file's data and metadata. It does not
/// synchronize its parent directory or certify hardware power-loss behavior.
/// A sync error does not imply that no bytes reached durable storage.
pub trait StorageIo {
    /// Cached access must check this too, so cancellation does not depend on I/O.
    fn check_interrupt(&self) -> Result<()> {
        Ok(())
    }
    /// Only the private transaction backend may open WAL-protected data pages.
    fn allows_wal_pages(&self) -> bool {
        false
    }
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize>;
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize>;
    fn sync(&mut self) -> Result<()>;
    fn set_len(&mut self, length: u64) -> Result<()>;
    fn len(&mut self) -> Result<u64>;

    fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
}

impl<I: StorageIo + ?Sized> StorageIo for &mut I {
    fn check_interrupt(&self) -> Result<()> {
        (**self).check_interrupt()
    }
    fn allows_wal_pages(&self) -> bool {
        (**self).allows_wal_pages()
    }
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        (**self).read_at(offset, buffer)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        (**self).write_at(offset, bytes)
    }
    fn sync(&mut self) -> Result<()> {
        (**self).sync()
    }
    fn set_len(&mut self, length: u64) -> Result<()> {
        (**self).set_len(length)
    }
    fn len(&mut self) -> Result<u64> {
        (**self).len()
    }
}

pub fn validate_range(offset: u64, length: usize) -> Result<u64> {
    u64::try_from(length)
        .ok()
        .and_then(|length| offset.checked_add(length))
        .ok_or(Error::InvalidRange { offset, length })
}

/// On failure the buffer may contain a partial read; callers must not consume it.
pub fn read_exact_at<I: StorageIo + ?Sized>(
    io: &mut I,
    offset: u64,
    buffer: &mut [u8],
) -> Result<()> {
    validate_range(offset, buffer.len())?;
    let expected = buffer.len();
    let mut done = 0;
    while done < expected {
        let current = offset + done as u64;
        let requested = expected - done;
        let read = match io.read_at(current, &mut buffer[done..]) {
            Err(error) if error.is_interrupted() => continue,
            result => result?,
        };
        if read > requested {
            return Err(Error::InvalidBackend {
                operation: Operation::Read,
                reported: read,
                requested,
            });
        }
        if read == 0 {
            return Err(Error::UnexpectedEof {
                offset,
                expected,
                actual: done,
            });
        }
        done += read;
    }
    Ok(())
}

/// On failure some bytes may have been written. This helper is not a transaction.
pub fn write_all_at<I: StorageIo + ?Sized>(io: &mut I, offset: u64, bytes: &[u8]) -> Result<()> {
    validate_range(offset, bytes.len())?;
    let mut done = 0;
    while done < bytes.len() {
        let current = offset + done as u64;
        let requested = bytes.len() - done;
        let written = match io.write_at(current, &bytes[done..]) {
            Err(error) if error.is_interrupted() => continue,
            result => result?,
        };
        if written > requested {
            return Err(Error::InvalidBackend {
                operation: Operation::Write,
                reported: written,
                requested,
            });
        }
        if written == 0 {
            return Err(Error::WriteZero { offset: current });
        }
        done += written;
    }
    Ok(())
}
