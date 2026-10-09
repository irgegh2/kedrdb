// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error, Operation, Result,
    record::primary_key,
    storage::{FileStorage, StorageIo, read_exact_at, write_all_at},
    tree::BPlusTree,
};
use std::{
    env,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{SystemTime, UNIX_EPOCH},
};

const PAYLOAD: &[u8] = b"KedrDB stage 1 storage check\n";
const CACHE_BUDGET: usize = 4 * 16 * 1024;
const USAGE: &str = "Usage: kedr --version | doctor | storage-check <directory> | create-store <new-file> | put <file> <bigint-key> <text> | get <file> <bigint-key> | delete <file> <bigint-key> | integrity-check <file>";

fn open_tree(path: &Path) -> Result<BPlusTree<FileStorage>> {
    let file = FileStorage::open_existing(path)?;
    file.try_lock_exclusive()?;
    BPlusTree::open(file, CACHE_BUDGET)
}
fn key(argument: &std::ffi::OsStr) -> Result<[u8; 8]> {
    let value = argument
        .to_str()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(Error::InvalidArgument {
            message: "key must be a signed BIGINT",
        })?;
    Ok(primary_key(value))
}
fn create_store(path: &Path) -> Result<()> {
    use std::io::Read;
    let mut id = [0; 16];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut id))
        .map_err(|e| Error::io(Operation::Read, e))?;
    let file = FileStorage::create_new(path)?;
    file.try_lock_exclusive()?;
    let _tree = BPlusTree::create(file, id, CACHE_BUDGET)?;
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    std::fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(|e| Error::io(Operation::Sync, e))?;
    println!("Created development B+ tree store. SQL and crash recovery are not available yet.");
    Ok(())
}

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
            println!("KedrDB {} (development)", env!("CARGO_PKG_VERSION"))
        }
        [arg] if arg == "doctor" => {
            println!("KedrDB {}", env!("CARGO_PKG_VERSION"));
            println!("Platform: {}/{}", env::consts::OS, env::consts::ARCH);
            println!("License: Apache-2.0; storage: positional file I/O + sync_all.");
            println!("Slotted-page codec and CRC32C: available.");
            println!(
                "B+ tree, bounded page cache, typed row codec and integrity check: available."
            );
            println!("SQL server, WAL and transactions: not implemented.");
        }
        [command, directory] if command == "storage-check" => storage_check(Path::new(directory))?,
        [command, path] if command == "create-store" => create_store(Path::new(path))?,
        [command, path, key_arg, value] if command == "put" => {
            let key = key(key_arg)?;
            let value = value.to_str().ok_or(Error::InvalidArgument {
                message: "value must be UTF-8",
            })?;
            let mut tree = open_tree(Path::new(path))?;
            tree.replace(&key, value.as_bytes())?;
            tree.checkpoint()?;
            println!("Stored (clean checkpoint; stage 2 has no crash-atomic transactions).");
        }
        [command, path, key_arg] if command == "get" => {
            let mut tree = open_tree(Path::new(path))?;
            match tree.get(&key(key_arg)?)? {
                Some(bytes) => println!("{}", String::from_utf8_lossy(&bytes)),
                None => println!("Key not found."),
            }
        }
        [command, path, key_arg] if command == "delete" => {
            let mut tree = open_tree(Path::new(path))?;
            let deleted = tree.delete(&key(key_arg)?)?;
            tree.checkpoint()?;
            println!("Deleted: {deleted}");
        }
        [command, path] if command == "integrity-check" => {
            let mut tree = open_tree(Path::new(path))?;
            let report = tree.integrity_check()?;
            println!(
                "Integrity check passed: {} records, {} tree pages, {} free pages, height {}.",
                report.records, report.tree_pages, report.free_pages, report.height
            );
        }
        [] => println!("{USAGE}"),
        [arg] if arg == "--help" || arg == "-h" => {
            println!("{USAGE}")
        }
        _ => {
            return Err(Error::InvalidArgument { message: USAGE });
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
