// SPDX-License-Identifier: Apache-2.0
//! Two-file crash model: successful sync is durable; pending writes may be lost,
//! torn or reach storage out of order. A crash stops all subsequent I/O.
use crate::{MemoryStorage, SeededRng};
use kedr_core::{Error, Operation, Result, storage::StorageIo};
use std::{
    io,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    calls: Vec<Operation>,
    cut: Option<(u64, bool)>,
    crashed: bool,
}
#[derive(Clone, Default)]
pub struct CrashControl(Arc<Mutex<State>>);
impl CrashControl {
    /// Cut before or after one global I/O call across both files.
    pub fn arm(&self, cut: Option<(u64, bool)>) {
        *self.0.lock().unwrap() = State {
            cut,
            ..State::default()
        };
    }
    pub fn trace(&self) -> Vec<Operation> {
        self.0.lock().unwrap().calls.clone()
    }
    fn enter(&self, operation: Operation) -> Result<bool> {
        let mut state = self.0.lock().unwrap();
        if state.crashed {
            return Err(stopped(operation));
        }
        state.calls.push(operation);
        if state.cut == Some((state.calls.len() as u64, false)) {
            state.crashed = true;
            return Err(stopped(operation));
        }
        Ok(state.cut == Some((state.calls.len() as u64, true)))
    }
    fn leave<T>(&self, after: bool, operation: Operation, result: Result<T>) -> Result<T> {
        if after {
            self.0.lock().unwrap().crashed = true;
            Err(stopped(operation))
        } else {
            result
        }
    }
}
fn stopped(operation: Operation) -> Error {
    Error::io(operation, io::Error::other("simulated machine crash"))
}
enum Pending {
    Write(usize, Vec<u8>),
    Resize(usize),
}
#[derive(Clone, Copy, Debug)]
pub enum Persistence {
    Lost,
    OrderedPrefix,
    ReorderedTorn,
}
pub struct CrashStorage {
    inner: MemoryStorage,
    control: CrashControl,
    pending: Vec<Pending>,
}
impl CrashStorage {
    pub fn new(inner: MemoryStorage, control: CrashControl) -> Self {
        Self {
            inner,
            control,
            pending: Vec::new(),
        }
    }
    /// Independent seeds give independent pending persistence in each file.
    pub fn crash_image(&self, persistence: Persistence, seed: u64) -> Vec<u8> {
        let mut bytes = self.inner.durable_bytes().to_vec();
        if matches!(persistence, Persistence::Lost) {
            return bytes;
        }
        let mut rng = SeededRng::new(seed);
        let mut order: Vec<_> = (0..self.pending.len()).collect();
        if matches!(persistence, Persistence::ReorderedTorn) {
            for i in (1..order.len()).rev() {
                let j = rng.next_u64() as usize % (i + 1);
                order.swap(i, j);
            }
        } else if !order.is_empty() {
            order.truncate(rng.next_u64() as usize % (order.len() + 1));
        }
        for index in order {
            if matches!(persistence, Persistence::ReorderedTorn) && rng.next_u64().is_multiple_of(3)
            {
                continue;
            }
            match &self.pending[index] {
                Pending::Resize(length) => bytes.resize(*length, 0),
                Pending::Write(offset, value) => {
                    let count = match persistence {
                        Persistence::ReorderedTorn => rng.next_u64() as usize % (value.len() + 1),
                        _ => value.len(),
                    };
                    if count > 0 {
                        bytes.resize(bytes.len().max(offset + count), 0);
                        bytes[*offset..*offset + count].copy_from_slice(&value[..count]);
                    }
                }
            }
        }
        bytes
    }
}
impl StorageIo for CrashStorage {
    fn read_at(&mut self, offset: u64, bytes: &mut [u8]) -> Result<usize> {
        let after = self.control.enter(Operation::Read)?;
        let result = self.inner.read_at(offset, bytes);
        self.control.leave(after, Operation::Read, result)
    }
    fn write_at(&mut self, offset: u64, bytes: &[u8]) -> Result<usize> {
        let after = self.control.enter(Operation::Write)?;
        let result = self.inner.write_at(offset, bytes);
        if let Ok(count) = result {
            self.pending
                .push(Pending::Write(offset as usize, bytes[..count].to_vec()));
        }
        self.control.leave(after, Operation::Write, result)
    }
    fn sync(&mut self) -> Result<()> {
        let after = self.control.enter(Operation::Sync)?;
        let result = self.inner.sync();
        if result.is_ok() {
            self.pending.clear();
        }
        self.control.leave(after, Operation::Sync, result)
    }
    fn set_len(&mut self, length: u64) -> Result<()> {
        let after = self.control.enter(Operation::Resize)?;
        let result = self.inner.set_len(length);
        if result.is_ok() {
            self.pending.push(Pending::Resize(length as usize));
        }
        self.control.leave(after, Operation::Resize, result)
    }
    fn len(&mut self) -> Result<u64> {
        let after = self.control.enter(Operation::Length)?;
        let result = self.inner.len();
        self.control.leave(after, Operation::Length, result)
    }
}
