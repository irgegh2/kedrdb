// SPDX-License-Identifier: Apache-2.0
use crate::{Error, Result};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
#[derive(Clone)]
pub(crate) struct Interrupt {
    pub cancelled: Arc<AtomicBool>,
    pub deadline: Instant,
}
impl Interrupt {
    pub fn check(&self) -> Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        if Instant::now() >= self.deadline {
            return Err(Error::TransactionTimeout);
        }
        Ok(())
    }
}
