// SPDX-License-Identifier: Apache-2.0
use crate::maintenance::{digest, new_file, regular, sync_dir};
use kedr_core::{
    Error, Operation, Result,
    database::FileDatabase,
    sql::{MAX_SQL_BYTES, SqlSession, dump},
    transaction::{SessionLimits, SharedDatabase},
};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::Path,
    time::Duration,
};
struct CompareWriter(std::io::BufReader<std::fs::File>);
impl Write for CompareWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut offset = 0;
        let mut buffer = [0; 65536];
        while offset < bytes.len() {
            let n = (bytes.len() - offset).min(buffer.len());
            self.0.read_exact(&mut buffer[..n])?;
            if buffer[..n] != bytes[offset..offset + n] {
                return Err(std::io::Error::other("canonical SQL/data mismatch"));
            }
            offset += n;
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        if self.0.read(&mut [0; 1])? != 0 {
            return Err(std::io::Error::other("extra canonical SQL/data"));
        }
        Ok(())
    }
}
const CACHE: usize = 4 * kedr_core::page::PAGE_SIZE;
fn invalid(message: &'static str) -> Error {
    Error::InvalidArgument { message }
}
fn io(e: std::io::Error) -> Error {
    Error::io(Operation::Read, e)
}
fn session(db: FileDatabase) -> Result<(SharedDatabase, SqlSession)> {
    let shared = SharedDatabase::new(
        db,
        SessionLimits {
            transaction_lifetime: Duration::from_secs(3600),
            idle_in_transaction: Duration::from_secs(300),
            ..Default::default()
        },
    )?;
    let sql = SqlSession::new(shared.connect()?);
    Ok((shared, sql))
}
// Only our bounded export dialect, not arbitrary PostgreSQL scripts or psql meta-commands.
fn statements(
    mut input: impl BufRead,
    mut visit: impl FnMut(u64, &str) -> Result<()>,
) -> Result<u64> {
    let mut line = Vec::new();
    input
        .by_ref()
        .take((MAX_SQL_BYTES + 2) as u64)
        .read_until(b'\n', &mut line)
        .map_err(io)?;
    if line != dump::HEADER.as_bytes() {
        return Err(invalid("SQL input requires KedrDB export header"));
    }
    let mut statement = Vec::new();
    let mut quote = None;
    let mut ordinal = 0;
    loop {
        line.clear();
        let n = input
            .by_ref()
            .take((MAX_SQL_BYTES + 2) as u64)
            .read_until(b'\n', &mut line)
            .map_err(io)?;
        if n == 0 {
            return Err(invalid("SQL export has no completion footer"));
        }
        if line.len() > MAX_SQL_BYTES + 1 {
            return Err(invalid("SQL export line exceeds 64 KiB"));
        }
        if quote.is_none()
            && statement.iter().all(u8::is_ascii_whitespace)
            && line == dump::FOOTER.as_bytes()
        {
            if input.read(&mut [0; 1]).map_err(io)? != 0 {
                return Err(invalid("bytes after SQL export footer"));
            }
            return Ok(ordinal);
        }
        let mut i = 0;
        while i < line.len() {
            let b = line[i];
            statement.push(b);
            if let Some(q) = quote {
                if b == q {
                    if line.get(i + 1) == Some(&q) {
                        i += 1;
                        statement.push(q);
                    } else {
                        quote = None;
                    }
                }
            } else {
                match b {
                    b'\'' | b'"' => quote = Some(b),
                    b';' => {
                        let text = std::str::from_utf8(&statement)
                            .map_err(|_| invalid("SQL export must be UTF-8"))?;
                        dump::validate_statement(text)?;
                        ordinal += 1;
                        visit(ordinal, text)?;
                        statement.clear();
                    }
                    b'-' if line.get(i + 1) == Some(&b'-') => {
                        return Err(invalid(
                            "comments outside export header/footer are unsupported",
                        ));
                    }
                    b'/' if line.get(i + 1) == Some(&b'*') => {
                        return Err(invalid(
                            "comments outside export header/footer are unsupported",
                        ));
                    }
                    _ => {}
                }
            }
            if statement.len() > MAX_SQL_BYTES {
                return Err(invalid("SQL export statement exceeds 64 KiB"));
            }
            i += 1;
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Job {
    import_version: u32,
    sha256: String,
    statements: u64,
}
fn job_id(hash: &str) -> Result<[u8; 32]> {
    if hash.len() != 64 {
        return Err(invalid("invalid import hash"));
    }
    let mut id = [0; 32];
    for (i, part) in hash.as_bytes().chunks_exact(2).enumerate() {
        id[i] = u8::from_str_radix(
            std::str::from_utf8(part).map_err(|_| invalid("invalid import hash"))?,
            16,
        )
        .map_err(|_| invalid("invalid import hash"))?;
    }
    Ok(id)
}
pub fn export(source: &Path, target: &Path) -> Result<()> {
    let db = FileDatabase::open(source, CACHE)?;
    let (_shared, mut sql) = session(db)?;
    let file = new_file(target)?;
    let sync = file.try_clone().map_err(io)?;
    let result = sql.export_sql(std::io::BufWriter::new(file))?;
    sync.sync_all().map_err(io)?;
    sync_dir(
        target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new(".")),
    )?;
    println!(
        "Exported {} rows; SQL file completed and synced.",
        result.affected
    );
    Ok(())
}
pub fn import(source: &Path, target: &Path, resume: bool, migration: bool) -> Result<()> {
    let mut file = regular(source)?;
    let (_, hash) = digest(file.try_clone().map_err(io)?)?;
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let total = statements(BufReader::new(file.try_clone().map_err(io)?), |_, _| Ok(()))?;
    let id = job_id(&hash)?;
    let marker = target.join(".import-incomplete");
    let db = if resume {
        let mut bytes = Vec::new();
        regular(&marker)?
            .take(4097)
            .read_to_end(&mut bytes)
            .map_err(io)?;
        if bytes.len() > 4096 {
            return Err(invalid("import marker size limit"));
        }
        let job: Job =
            serde_json::from_slice(&bytes).map_err(|_| invalid("invalid import marker"))?;
        if job.import_version != 1 || job.sha256 != hash || job.statements != total {
            return Err(invalid("resume requires the same verified SQL export"));
        }
        FileDatabase::open_importing(target, CACHE)?
    } else {
        let db = FileDatabase::create(target, crate::random_id()?, CACHE)?;
        if migration {
            let mut guard = new_file(&target.join(".migration-incomplete"))?;
            guard
                .write_all(b"PostgreSQL migration pending verification\n")
                .map_err(io)?;
            guard.sync_all().map_err(io)?;
            sync_dir(target)?;
        }
        let mut f = new_file(&marker)?;
        serde_json::to_writer(
            &mut f,
            &Job {
                import_version: 1,
                sha256: hash.clone(),
                statements: total,
            },
        )
        .map_err(|_| invalid("cannot write import marker"))?;
        f.sync_all().map_err(io)?;
        sync_dir(target)?;
        db
    };
    let (shared, mut sql) = session(db)?;
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let applied = statements(
        BufReader::new(file.try_clone().map_err(io)?),
        |ordinal, text| {
            sql.import_statement(text, id, ordinal)?;
            if ordinal % 256 == 0 {
                println!(
                    "{}",
                    serde_json::json!({"event":"import_progress","durable_statements":ordinal,"total_statements":total})
                );
                std::io::stdout().flush().map_err(io)?;
            }
            Ok(())
        },
    )?;
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let (_, after) = digest(file)?;
    if after != hash || applied != total {
        return Err(invalid(
            "SQL export changed during import; target remains quarantined",
        ));
    }
    let result = sql.finish_import(id, total)?;
    // Verify the canonical full data/schema, including self-FK cycles, before publication.
    sql.export_sql(CompareWriter(BufReader::new(regular(source)?)))?;
    drop(sql);
    drop(shared);
    {
        let mut db = FileDatabase::open_importing(target, CACHE)?;
        db.integrity_check()?;
        db.checkpoint()?;
    }
    fs::remove_file(marker).map_err(io)?;
    sync_dir(target)?;
    println!(
        "{}",
        serde_json::json!({"event":"import_complete","rows":result.affected,"statements":total,"sha256":hash})
    );
    Ok(())
}

pub fn complete_migration(source: &Path, target: &Path) -> Result<()> {
    let marker = target.join(".migration-incomplete");
    regular(&marker)?;
    if target.join(".import-incomplete").try_exists().map_err(io)? {
        return Err(invalid("resume SQL import before completing migration"));
    }
    let db = FileDatabase::open_importing(target, CACHE)?;
    let (shared, mut sql) = session(db)?;
    let result = sql.export_sql(CompareWriter(BufReader::new(regular(source)?)))?;
    drop(sql);
    drop(shared);
    let mut db = FileDatabase::open_importing(target, CACHE)?;
    db.integrity_check()?;
    db.checkpoint()?;
    fs::remove_file(marker).map_err(io)?;
    sync_dir(target)?;
    println!(
        "{}",
        serde_json::json!({"event":"migration_verified","rows":result.affected,"comparison":"all_canonical_schema_and_value_bytes"})
    );
    Ok(())
}
