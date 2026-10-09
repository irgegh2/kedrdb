// SPDX-License-Identifier: Apache-2.0
//! Deterministic single-file simulation, not a model of every filesystem failure.
use std::{collections::BTreeMap, io};
pub mod crash;

use kedr_core::{
    Error, Operation, Result,
    storage::{StorageIo, validate_range},
};

/// Unsynchronized bytes are volatile. A simulated crash restores the last sync.
#[derive(Debug)]
pub struct MemoryStorage {
    volatile: Vec<u8>,
    durable: Vec<u8>,
    limit: usize,
}

impl MemoryStorage {
    pub fn new(limit: usize) -> Self {
        Self {
            volatile: Vec::new(),
            durable: Vec::new(),
            limit,
        }
    }

    pub fn durable_bytes(&self) -> &[u8] {
        &self.durable
    }
    pub fn visible_bytes(&self) -> &[u8] {
        &self.volatile
    }

    pub fn crash(&mut self) -> Result<()> {
        self.volatile = copy_bytes(&self.durable)?;
        Ok(())
    }

    fn checked_length(&self, length: u64) -> Result<usize> {
        let length = usize::try_from(length).map_err(|_| Error::LimitExceeded {
            limit: self.limit,
            requested: length,
        })?;
        if length > self.limit {
            return Err(Error::LimitExceeded {
                limit: self.limit,
                requested: length as u64,
            });
        }
        Ok(length)
    }

    fn resize(&mut self, length: usize) -> Result<()> {
        if length > self.volatile.len() {
            self.volatile
                .try_reserve_exact(length - self.volatile.len())
                .map_err(|_| Error::Allocation { requested: length })?;
        }
        self.volatile.resize(length, 0);
        Ok(())
    }
}

fn copy_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    let mut copy = Vec::new();
    copy.try_reserve_exact(bytes.len())
        .map_err(|_| Error::Allocation {
            requested: bytes.len(),
        })?;
    copy.extend_from_slice(bytes);
    Ok(copy)
}

impl StorageIo for MemoryStorage {
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        validate_range(offset, buffer.len())?;
        if offset >= self.volatile.len() as u64 {
            return Ok(0);
        }
        let start = offset as usize;
        let count = buffer.len().min(self.volatile.len() - start);
        buffer[..count].copy_from_slice(&self.volatile[start..start + count]);
        Ok(count)
    }

    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        let end = validate_range(offset, bytes.len())?;
        if bytes.is_empty() {
            return Ok(0);
        }
        let end = self.checked_length(end)?;
        if end > self.volatile.len() {
            self.resize(end)?;
        }
        self.volatile[offset as usize..end].copy_from_slice(bytes);
        Ok(bytes.len())
    }

    fn sync(&mut self) -> Result<()> {
        self.durable = copy_bytes(&self.volatile)?;
        Ok(())
    }
    fn set_len(&mut self, length: u64) -> Result<()> {
        self.resize(self.checked_length(length)?)
    }
    fn len(&mut self) -> Result<u64> {
        Ok(self.volatile.len() as u64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    Error(io::ErrorKind),
    Short(usize),
    Zero,
    /// Sync really persists bytes, then reports an error: outcome is uncertain.
    SyncThenError(io::ErrorKind),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceEntry {
    pub call: u64,
    pub operation: Operation,
    pub offset: Option<u64>,
    pub requested: Option<usize>,
    pub fault: Option<Fault>,
    pub result: std::result::Result<u64, &'static str>,
}

/// Faults are assigned to a one-based count of *all* I/O calls, including len.
/// Limit the operation count in a test: the trace is intentionally kept in RAM.
pub struct FaultIo<I> {
    inner: I,
    faults: BTreeMap<u64, Fault>,
    calls: u64,
    trace: Vec<TraceEntry>,
}

impl<I> FaultIo<I> {
    pub fn new(inner: I, faults: impl IntoIterator<Item = (u64, Fault)>) -> Result<Self> {
        let mut schedule = BTreeMap::new();
        for (call, fault) in faults {
            if call == 0 || schedule.insert(call, fault).is_some() {
                return Err(Error::InvalidArgument {
                    message: "fault call numbers must be positive and unique",
                });
            }
        }
        Ok(Self {
            inner,
            faults: schedule,
            calls: 0,
            trace: Vec::new(),
        })
    }

    pub fn inner_mut(&mut self) -> &mut I {
        &mut self.inner
    }
    pub fn trace(&self) -> &[TraceEntry] {
        &self.trace
    }
    pub fn into_inner(self) -> I {
        self.inner
    }

    fn begin(&mut self) -> Option<Fault> {
        self.calls += 1;
        self.faults.remove(&self.calls)
    }

    fn record<T>(
        &mut self,
        operation: Operation,
        offset: Option<u64>,
        requested: Option<usize>,
        fault: Option<Fault>,
        result: &Result<T>,
        success: impl FnOnce(&T) -> u64,
    ) {
        self.trace.push(TraceEntry {
            call: self.calls,
            operation,
            offset,
            requested,
            fault,
            result: result.as_ref().map(success).map_err(Error::code),
        });
    }
}

fn injected(operation: Operation, kind: io::ErrorKind) -> Error {
    Error::io(operation, io::Error::new(kind, "injected storage fault"))
}

impl<I: StorageIo> StorageIo for FaultIo<I> {
    fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        validate_range(offset, buffer.len())?;
        let fault = self.begin();
        let requested = buffer.len();
        let result = match fault {
            Some(Fault::Error(kind)) => Err(injected(Operation::Read, kind)),
            Some(Fault::Zero) => Ok(0),
            Some(Fault::Short(count)) => self
                .inner
                .read_at(offset, &mut buffer[..count.min(requested)]),
            Some(Fault::SyncThenError(_)) => Err(Error::InvalidFault {
                operation: Operation::Read,
            }),
            None => self.inner.read_at(offset, buffer),
        };
        self.record(
            Operation::Read,
            Some(offset),
            Some(requested),
            fault,
            &result,
            |n| *n as u64,
        );
        result
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        validate_range(offset, bytes.len())?;
        let fault = self.begin();
        let result = match fault {
            Some(Fault::Error(kind)) => Err(injected(Operation::Write, kind)),
            Some(Fault::Zero) => Ok(0),
            Some(Fault::Short(count)) => self
                .inner
                .write_at(offset, &bytes[..count.min(bytes.len())]),
            Some(Fault::SyncThenError(_)) => Err(Error::InvalidFault {
                operation: Operation::Write,
            }),
            None => self.inner.write_at(offset, bytes),
        };
        self.record(
            Operation::Write,
            Some(offset),
            Some(bytes.len()),
            fault,
            &result,
            |n| *n as u64,
        );
        result
    }
    fn sync(&mut self) -> Result<()> {
        let fault = self.begin();
        let result = match fault {
            Some(Fault::Error(kind)) => Err(injected(Operation::Sync, kind)),
            Some(Fault::SyncThenError(kind)) => self
                .inner
                .sync()
                .and_then(|()| Err(injected(Operation::Sync, kind))),
            Some(_) => Err(Error::InvalidFault {
                operation: Operation::Sync,
            }),
            None => self.inner.sync(),
        };
        self.record(Operation::Sync, None, None, fault, &result, |()| 0);
        result
    }
    fn set_len(&mut self, length: u64) -> Result<()> {
        let fault = self.begin();
        let result = match fault {
            Some(Fault::Error(kind)) => Err(injected(Operation::Resize, kind)),
            Some(_) => Err(Error::InvalidFault {
                operation: Operation::Resize,
            }),
            None => self.inner.set_len(length),
        };
        self.record(
            Operation::Resize,
            Some(length),
            None,
            fault,
            &result,
            |()| 0,
        );
        result
    }
    fn len(&mut self) -> Result<u64> {
        let fault = self.begin();
        let result = match fault {
            Some(Fault::Error(kind)) => Err(injected(Operation::Length, kind)),
            Some(_) => Err(Error::InvalidFault {
                operation: Operation::Length,
            }),
            None => self.inner.len(),
        };
        self.record(Operation::Length, None, None, fault, &result, |n| *n);
        result
    }
}

/// Stable test generator, not suitable for cryptography or production IDs.
pub struct SeededRng(u64);
impl SeededRng {
    pub fn new(seed: u64) -> Self {
        Self(if seed == 0 { 0x9e3779b97f4a7c15 } else { seed })
    }
    pub fn next_u64(&mut self) -> u64 {
        let mut value = self.0;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.0 = value;
        value
    }
}
