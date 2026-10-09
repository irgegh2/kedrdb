// SPDX-License-Identifier: Apache-2.0
//! Core storage contracts for KedrDB. SQL and transactions are later milestones.

pub mod checksum;
#[cfg(unix)]
pub mod database;
pub mod engine;
pub mod error;
mod interrupt;
pub mod page;
pub mod record;
pub mod storage;
#[cfg(unix)]
pub mod transaction;
pub mod tree;
pub mod wal;

pub use error::{Error, Operation, Result};
