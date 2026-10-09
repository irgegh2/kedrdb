// SPDX-License-Identifier: Apache-2.0
//! Core storage contracts for KedrDB. SQL and transactions are later milestones.

pub mod checksum;
pub mod error;
pub mod page;
pub mod storage;

pub use error::{Error, Operation, Result};
