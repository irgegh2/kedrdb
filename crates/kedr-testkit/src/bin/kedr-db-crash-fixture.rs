// SPDX-License-Identifier: Apache-2.0
//! Emits acknowledgements only after data/WAL/directory synchronization.
use kedr_core::{database::FileDatabase, page::PAGE_SIZE, record::primary_key};
use std::{
    io::{self, Write},
    path::PathBuf,
};
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected new directory path")?,
    );
    let mut db = FileDatabase::create(path, [23; 16], 4 * PAGE_SIZE)?;
    db.set_segment_bytes(32 * 1024)?;
    for key in 0..10000i64 {
        db.transaction(|tx| {
            tx.insert(&primary_key(key * 2), &vec![key as u8; 4096])?;
            tx.insert(&primary_key(key * 2 + 1), &vec![key as u8; 4096])
        })?;
        println!("ACK {key}");
        io::stdout().flush()?;
    }
    Ok(())
}
