// SPDX-License-Identifier: Apache-2.0
//! Core storage contracts for KedrDB. SQL and transactions are later milestones.

pub mod checksum;
pub mod error;
pub mod page;
pub mod record;
pub mod storage;
pub mod tree;

pub use error::{Error, Operation, Result};
