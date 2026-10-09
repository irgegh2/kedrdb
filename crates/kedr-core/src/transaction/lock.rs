// SPDX-License-Identifier: Apache-2.0
use super::Mode;
use crate::{Error, Result};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
#[derive(Default)]
struct State {
    next: u64,
    waiting: VecDeque<(u64, Mode)>,
    readers: usize,
    writer: bool,
    grants: u64,
    wait_nanos: u64,
    max_wait_nanos: u64,
}
#[derive(Default)]
pub(super) struct Gate {
    state: Mutex<State>,
    changed: Condvar,
}
#[derive(Clone, Copy, Debug)]
pub struct LockMetrics {
    pub readers: usize,
    pub writer: bool,
    pub waiting: usize,
    pub grants: u64,
    pub total_wait_nanos: u64,
    pub max_wait_nanos: u64,
}
pub(super) struct Permit {
    gate: Arc<Gate>,
    mode: Mode,
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = self.gate.state.lock().unwrap();
        match self.mode {
            Mode::ReadOnly => state.readers -= 1,
            Mode::ReadWrite => state.writer = false,
        }
        self.gate.changed.notify_all();
    }
}
impl Gate {
    pub fn acquire(
        self: &Arc<Self>,
        mode: Mode,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Permit> {
        let began = Instant::now();
        let mut state = self.state.lock().unwrap();
        let ticket = state.next;
        state.next = state.next.checked_add(1).ok_or(Error::InvalidArgument {
            message: "lock ticket exhausted",
        })?;
        state.waiting.push_back((ticket, mode));
        loop {
            let error = if cancelled.load(Ordering::Acquire) {
                Some(Error::Cancelled)
            } else if Instant::now() >= deadline {
                Some(Error::LockTimeout)
            } else {
                None
            };
            if let Some(error) = error {
                state.waiting.retain(|(id, _)| *id != ticket);
                self.changed.notify_all();
                return Err(error);
            }
            let before = state.waiting.iter().take_while(|(id, _)| *id != ticket);
            let allowed = !state.writer
                && match mode {
                    Mode::ReadOnly => !before.clone().any(|(_, m)| *m == Mode::ReadWrite),
                    Mode::ReadWrite => {
                        state.readers == 0
                            && state.waiting.front().is_some_and(|(id, _)| *id == ticket)
                    }
                };
            if allowed {
                state.waiting.retain(|(id, _)| *id != ticket);
                match mode {
                    Mode::ReadOnly => state.readers += 1,
                    Mode::ReadWrite => state.writer = true,
                }
                let nanos = began.elapsed().as_nanos().min(u64::MAX as u128) as u64;
                state.grants = state.grants.saturating_add(1);
                state.wait_nanos = state.wait_nanos.saturating_add(nanos);
                state.max_wait_nanos = state.max_wait_nanos.max(nanos);
                self.changed.notify_all();
                return Ok(Permit {
                    gate: self.clone(),
                    mode,
                });
            }
            let remaining = deadline
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10));
            state = self.changed.wait_timeout(state, remaining).unwrap().0;
        }
    }
    pub fn metrics(&self) -> LockMetrics {
        let state = self.state.lock().unwrap();
        LockMetrics {
            readers: state.readers,
            writer: state.writer,
            waiting: state.waiting.len(),
            grants: state.grants,
            total_wait_nanos: state.wait_nanos,
            max_wait_nanos: state.max_wait_nanos,
        }
    }
}
