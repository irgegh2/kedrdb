// SPDX-License-Identifier: Apache-2.0
//! Child process for relational SIGKILL tests, never a user server.
use kedr_core::{
    database::FileDatabase,
    page::PAGE_SIZE,
    record::Value,
    sql::SqlSession,
    transaction::{SessionLimits, SharedDatabase},
};
use std::{
    io::{self, Write},
    path::PathBuf,
};
fn main() {
    let path = std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .expect("new data directory");
    let mut file = FileDatabase::create(&path, [72; 16], 4 * PAGE_SIZE).unwrap();
    file.set_segment_bytes(32 * 1024).unwrap();
    let shared = SharedDatabase::new(file, SessionLimits::default()).unwrap();
    let mut sql = SqlSession::new(shared.connect().unwrap());
    sql.execute("BEGIN", &[]).unwrap();
    sql.execute(
        "CREATE TABLE counter (id BIGINT PRIMARY KEY,n INT UNIQUE)",
        &[],
    )
    .unwrap();
    sql.execute("CREATE TABLE audit (id BIGINT PRIMARY KEY,applied INT UNIQUE,counter_id BIGINT REFERENCES counter(id))",&[]).unwrap();
    sql.execute("INSERT INTO counter (id,n) VALUES (1,0)", &[])
        .unwrap();
    sql.execute("COMMIT", &[]).unwrap();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    writeln!(output, "READY").unwrap();
    output.flush().unwrap();
    for n in 1..=10000 {
        sql.execute("BEGIN", &[]).unwrap();
        sql.execute("UPDATE counter SET n=n+1 WHERE id=1", &[])
            .unwrap();
        sql.execute(
            "INSERT INTO audit (id,applied,counter_id) VALUES ($1,$2,1)",
            &[Value::BigInt(i64::from(n)), Value::Int(n)],
        )
        .unwrap();
        sql.execute("COMMIT", &[]).unwrap();
        writeln!(output, "ACK {n}").unwrap();
        output.flush().unwrap();
    }
}
