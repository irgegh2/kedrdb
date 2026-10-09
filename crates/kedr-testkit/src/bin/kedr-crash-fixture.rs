// SPDX-License-Identifier: Apache-2.0
//! Child process used only by the crash harness; its file must not exist yet.
use kedr_core::storage::{FileStorage, StorageIo, write_all_at};
use std::{
    io::{self, Write},
    path::PathBuf,
    time::Duration,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .ok_or("expected fresh file path")?,
    );
    let mut file = FileStorage::create_new(path)?;
    write_all_at(&mut file, 0, b"synchronized-prefix")?;
    file.sync()?;
    write_all_at(&mut file, 128, b"unsynchronized-tail")?;
    println!("READY");
    io::stdout().flush()?;
    loop {
        std::thread::sleep(Duration::from_secs(1));
    }
}
