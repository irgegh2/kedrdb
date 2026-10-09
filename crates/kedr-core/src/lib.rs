// SPDX-License-Identifier: Apache-2.0
//! Storage, WAL, concurrent sessions and bounded SQL for KedrDB.

pub mod checksum;
#[cfg(unix)]
pub mod database;
pub mod engine;
pub mod error;
mod interrupt;
pub mod page;
pub mod record;
#[cfg(unix)]
pub mod sql;
pub mod storage;
#[cfg(unix)]
pub mod transaction;
pub mod tree;
pub mod wal;

pub use error::{Error, Operation, Result};
