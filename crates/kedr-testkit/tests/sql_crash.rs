// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use kedr_core::{
    database::FileDatabase,
    page::PAGE_SIZE,
    record::Value,
    sql::SqlSession,
    transaction::{SessionLimits, SharedDatabase},
};
use std::{
    fs,
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
#[test]
fn acknowledged_sql_catalog_rows_indices_and_foreign_keys_survive_sigkill_during_rotation() {
    for acknowledged in [0, 4, 24] {
        let root = Scratch(std::env::temp_dir().join(format!(
                "kedr-sql-crash-{}-{acknowledged}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            )));
        let mut child = Command::new(env!("CARGO_BIN_EXE_kedr-sql-crash-fixture"))
            .arg(&root.0)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut reader = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        assert_eq!(line.trim(), "READY");
        for n in 1..=acknowledged {
            line.clear();
            reader.read_line(&mut line).unwrap();
            assert_eq!(line.trim(), format!("ACK {n}"));
        }
        child.kill().unwrap();
        child.wait().unwrap();
        let mut recovered = None;
        for _ in 0..2 {
            let mut file = FileDatabase::open(&root.0, 2 * PAGE_SIZE).unwrap();
            file.integrity_check().unwrap();
            let db = SharedDatabase::new(file, SessionLimits::default()).unwrap();
            let mut sql = SqlSession::new(db.connect().unwrap());
            let rows = sql
                .execute("SELECT applied FROM audit ORDER BY id", &[])
                .unwrap()
                .rows;
            let n = match sql
                .execute("SELECT n FROM counter WHERE id=1", &[])
                .unwrap()
                .rows[0][0]
            {
                Value::Int(n) => n,
                _ => panic!("counter type"),
            };
            assert!(n >= acknowledged);
            assert_eq!(
                rows,
                (1..=n).map(|i| vec![Value::Int(i)]).collect::<Vec<_>>()
            );
            assert_eq!(
                sql.execute(&format!("SELECT id FROM counter WHERE n={n}"), &[])
                    .unwrap()
                    .rows,
                vec![vec![Value::BigInt(1)]]
            );
            assert_eq!(
                sql.execute(
                    "SELECT COUNT(*) FROM audit AS a INNER JOIN counter AS c ON a.counter_id=c.id",
                    &[]
                )
                .unwrap()
                .rows,
                vec![vec![Value::BigInt(i64::from(n))]]
            );
            if let Some(previous) = recovered {
                assert_eq!(n, previous);
            }
            recovered = Some(n);
        }
    }
}
