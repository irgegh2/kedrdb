// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error, Operation, Result,
    storage::{FileStorage, StorageIo, read_exact_at, write_all_at},
};
use std::{
    env,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

const PAYLOAD: &[u8] = b"KedrDB stage 1 storage check\n";

struct ScratchFile(Option<PathBuf>);
impl Drop for ScratchFile {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn storage_check(directory: &Path) -> Result<()> {
    let metadata =
        std::fs::metadata(directory).map_err(|error| Error::io(Operation::Open, error))?;
    if !metadata.is_dir() {
        return Err(Error::InvalidArgument {
            message: "storage-check requires an existing directory",
        });
    }
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| Error::InvalidArgument {
            message: "system clock is before Unix epoch",
        })?
        .as_nanos();
    let path = directory.join(format!(".kedr-check-{}-{stamp}", std::process::id()));
    let mut file = FileStorage::create_new(&path)?;
    let mut scratch = ScratchFile(Some(path.clone()));
    write_all_at(&mut file, 7, PAYLOAD)?;
    file.sync()?;
    drop(file);
    let mut file = FileStorage::open_existing(&path)?;
    let mut bytes = vec![0; 7 + PAYLOAD.len()];
    read_exact_at(&mut file, 0, &mut bytes)?;
    if bytes[..7] != [0; 7] || &bytes[7..] != PAYLOAD || file.len()? != bytes.len() as u64 {
        return Err(Error::InvalidArgument {
            message: "storage-check byte verification failed",
        });
    }
    drop(file);
    std::fs::remove_file(&path).map_err(|error| Error::io(Operation::Write, error))?;
    scratch.0 = None;
    println!("Storage check passed: positional write, sync, reopen, exact read, cleanup.");
    println!("This is a file I/O check; SQL and transaction recovery are not implemented.");
    Ok(())
}

fn run() -> Result<()> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    match args.as_slice() {
        [arg] if arg == "--version" => {
            println!("KedrDB {} (stage 1 foundation)", env!("CARGO_PKG_VERSION"))
        }
        [arg] if arg == "doctor" => {
            println!("KedrDB {}", env!("CARGO_PKG_VERSION"));
            println!("Platform: {}/{}", env::consts::OS, env::consts::ARCH);
            println!("License: Apache-2.0; storage: positional file I/O + sync_all.");
            println!("Slotted-page codec and CRC32C: available.");
            println!("SQL server, page store, indexes, WAL and transactions: not implemented.");
        }
        [command, directory] if command == "storage-check" => storage_check(Path::new(directory))?,
        [] => println!("Usage: kedr --version | doctor | storage-check <existing-directory>"),
        [arg] if arg == "--help" || arg == "-h" => {
            println!("Usage: kedr --version | doctor | storage-check <existing-directory>")
        }
        _ => {
            return Err(Error::InvalidArgument {
                message: "usage: kedr --version | doctor | storage-check <existing-directory>",
            });
        }
    }
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
