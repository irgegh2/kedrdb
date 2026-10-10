// SPDX-License-Identifier: Apache-2.0
use std::{
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
};

use crate::{
    Error, Operation, Result,
    storage::{StorageIo, validate_range},
};

/// Owns a file opened without append or truncate mode. Multi-process ownership
/// and parent-directory synchronization are later filesystem-layer concerns.
pub struct FileStorage {
    file: File,
    wal: bool,
}

impl FileStorage {
    /// Read-only maintenance verification; writes through StorageIo fail at the OS.
    pub fn open_readonly(path: impl AsRef<Path>) -> Result<Self> {
        let file =
            File::open(path.as_ref()).map_err(|source| Error::io(Operation::Open, source))?;
        Ok(Self {
            file,
            wal: path.as_ref().extension().is_some_and(|e| e == "wal"),
        })
    }
    /// Advisory OS lock; callers owning a database must hold it for its lifetime.
    pub fn try_lock_exclusive(&self) -> Result<()> {
        self.file.try_lock().map_err(|error| {
            let source = match error {
                std::fs::TryLockError::WouldBlock => std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "database file is already locked",
                ),
                std::fs::TryLockError::Error(source) => source,
            };
            Error::io(Operation::Open, source)
        })
    }
    /// Creates a fresh file; existing files are never overwritten.
    pub fn create_new(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path.as_ref())
            .map_err(|source| Error::io(Operation::Open, source))?;
        Ok(Self {
            file,
            wal: path.as_ref().extension().is_some_and(|e| e == "wal"),
        })
    }

    /// Opens an existing file for positional reading/writing, preserving content.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path.as_ref())
            .map_err(|source| Error::io(Operation::Open, source))?;
        Ok(Self {
            file,
            wal: path.as_ref().extension().is_some_and(|e| e == "wal"),
        })
    }
}

impl StorageIo for FileStorage {
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        validate_range(offset, buffer.len())?;
        let result = self.file.read_at(buffer, offset);
        crate::metrics::read(&result);
        result.map_err(|source| Error::io(Operation::Read, source))
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        validate_range(offset, bytes.len())?;
        let result = self.file.write_at(bytes, offset);
        crate::metrics::write(self.wal, &result);
        result.map_err(|source| Error::io(Operation::Write, source))
    }

    fn sync(&mut self) -> Result<()> {
        let began = std::time::Instant::now();
        let result = self.file.sync_all();
        crate::metrics::sync(began, result.is_ok());
        result.map_err(|source| Error::io(Operation::Sync, source))
    }

    fn set_len(&mut self, length: u64) -> Result<()> {
        self.file
            .set_len(length)
            .map_err(|source| Error::io(Operation::Resize, source))
    }

    fn len(&mut self) -> Result<u64> {
        self.file
            .metadata()
            .map(|metadata| metadata.len())
            .map_err(|source| Error::io(Operation::Length, source))
    }
}
