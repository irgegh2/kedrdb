// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use kedr_core::{
    Error,
    database::FileDatabase,
    page::PAGE_SIZE,
    record::Value,
    sql::SqlSession,
    transaction::{SessionLimits, SharedDatabase},
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn database() -> (Scratch, SharedDatabase, SqlSession) {
    let root = Scratch(std::env::temp_dir().join(format!(
            "kedr-sql-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
    let file = FileDatabase::create(&root.0, [51; 16], 4 * PAGE_SIZE).unwrap();
    let shared = SharedDatabase::new(file, SessionLimits::default()).unwrap();
    let sql = SqlSession::new(shared.connect().unwrap());
    (root, shared, sql)
}
fn state(result: kedr_core::Result<kedr_core::sql::QueryResult>) -> &'static str {
    match result {
        Err(Error::Sql { state, .. }) => state,
        other => panic!("expected SQL error, got {other:?}"),
    }
}
fn rows(session: &mut SqlSession, sql: &str) -> Vec<Vec<Value>> {
    session.execute(sql, &[]).unwrap().rows
}

#[test]
fn prepared_sql_rebinds_current_catalog_and_keeps_transaction_control() {
    let (_root, _shared, mut sql) = database();
    sql.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, value INT)", &[])
        .unwrap();
    sql.execute("INSERT INTO items (id,value) VALUES (1,7)", &[])
        .unwrap();
    let select = sql
        .prepare("SELECT value FROM items WHERE id=$1", &[])
        .unwrap();
    assert!(select.read_only());
    assert_eq!(
        sql.execute_prepared(&select, &[Value::BigInt(1)])
            .unwrap()
            .rows,
        vec![vec![Value::Int(7)]]
    );
    sql.execute("DROP TABLE items", &[]).unwrap();
    assert_eq!(
        state(sql.execute_prepared(&select, &[Value::BigInt(1)])),
        "42P01"
    );
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, value TEXT)",
        &[],
    )
    .unwrap();
    sql.execute("INSERT INTO items (id,value) VALUES (1,'fresh')", &[])
        .unwrap();
    let result = sql.execute_prepared(&select, &[Value::BigInt(1)]).unwrap();
    assert_eq!(result.rows, vec![vec![Value::Text("fresh".into())]]);
    assert_ne!(result.columns, select.description().columns);
    let begin = sql.prepare("/* cached control */ BEGIN", &[]).unwrap();
    let rollback = sql.prepare("/* cached control */ ROLLBACK", &[]).unwrap();
    assert_eq!(begin.control_tag(), Some("BEGIN"));
    assert!(!begin.read_only());
    sql.execute_prepared(&begin, &[]).unwrap();
    sql.execute("INSERT INTO items (id,value) VALUES (2,'private')", &[])
        .unwrap();
    sql.execute_prepared(&rollback, &[]).unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM items"),
        vec![vec![Value::BigInt(1)]]
    );
}

#[test]
fn describe_and_catalog_share_snapshot_without_executing_writes() {
    use kedr_core::sql::DataType;
    let (_root, _shared, mut sql) = database();
    sql.execute(
        "CREATE TABLE items (id BIGINT PRIMARY KEY, stock INT, name TEXT)",
        &[],
    )
    .unwrap();
    let insert = sql
        .describe("INSERT INTO items (id,stock,name) VALUES ($1,$2,$3)", &[])
        .unwrap();
    assert_eq!(
        insert.parameters,
        [DataType::BigInt, DataType::Int, DataType::Text]
    );
    assert!(insert.columns.is_empty());
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM items"),
        vec![vec![Value::BigInt(0)]]
    );
    let select = sql
        .describe(
            "SELECT id,stock,$1 FROM items WHERE id=$2 LIMIT $3",
            &[Some(DataType::Text)],
        )
        .unwrap();
    assert_eq!(
        select.parameters,
        [DataType::Text, DataType::BigInt, DataType::BigInt]
    );
    assert_eq!(
        select
            .columns
            .iter()
            .map(|column| column.data_type)
            .collect::<Vec<_>>(),
        [DataType::BigInt, DataType::Int, DataType::Text]
    );
    assert!(
        sql.describe("UPDATE items SET stock=$1", &[Some(DataType::Boolean)])
            .is_err()
    );
    assert!(
        sql.describe("DELETE FROM items WHERE $1", &[Some(DataType::Int)])
            .is_err()
    );
    sql.execute("BEGIN", &[]).unwrap();
    sql.execute("CREATE TABLE private (id BIGINT PRIMARY KEY)", &[])
        .unwrap();
    assert_eq!(rows(&mut sql, "SHOW TABLES").len(), 2);
    assert_eq!(
        sql.describe("SELECT id FROM private", &[]).unwrap().columns[0].data_type,
        DataType::BigInt
    );
    sql.execute("ROLLBACK", &[]).unwrap();
    assert_eq!(
        rows(&mut sql, "SHOW TABLES"),
        vec![vec![Value::Text("items".into())]]
    );
    sql.execute("BEGIN", &[]).unwrap();
    assert!(sql.execute("SELECT missing FROM items", &[]).is_err());
    sql.describe("ROLLBACK", &[]).unwrap();
    sql.execute("ROLLBACK", &[]).unwrap();
    let Err(Error::Sql { message, .. }) = sql.execute("SELECT id\nFROM", &[]) else {
        panic!("expected syntax error");
    };
    assert!(message.contains("line 2, column 5"), "{message}");
    sql.execute("BEGIN READ ONLY", &[]).unwrap();
    assert_eq!(rows(&mut sql, "SHOW COLUMNS FROM items").len(), 3);
    sql.describe("INSERT INTO items (id) VALUES ($1)", &[])
        .unwrap();
    sql.execute("ROLLBACK", &[]).unwrap();
}

#[test]
fn tables_crud_parameters_nulls_indices_and_reopen() {
    let (root, shared, mut sql) = database();
    sql.execute("CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT NOT NULL, stock INT, active BOOLEAN, code TEXT UNIQUE)",&[]).unwrap();
    sql.execute(
        "INSERT INTO items (id,name,stock,active,code) VALUES ($1,$2,$3,$4,$5)",
        &[
            Value::BigInt(1),
            Value::Text("Кедр '\"".into()),
            Value::Int(4),
            Value::Bool(true),
            Value::Text("a".into()),
        ],
    )
    .unwrap();
    sql.execute("INSERT INTO items (id,name,stock,active,code) VALUES (2,'second',NULL,false,NULL),(3,'third',8,true,NULL)",&[]).unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT id,stock FROM items ORDER BY stock"),
        vec![
            vec![Value::BigInt(1), Value::Int(4)],
            vec![Value::BigInt(3), Value::Int(8)],
            vec![Value::BigInt(2), Value::Null]
        ]
    );
    assert!(rows(&mut sql, "SELECT id FROM items WHERE stock=NULL").is_empty());
    assert_eq!(
        rows(
            &mut sql,
            "SELECT COUNT(*) FROM items WHERE active=true AND stock IS NOT NULL"
        ),
        vec![vec![Value::BigInt(2)]]
    );
    sql.execute("CREATE INDEX stock_idx ON items (stock)", &[])
        .unwrap();
    assert_eq!(
        rows(&mut sql, "EXPLAIN SELECT id FROM items WHERE stock=4"),
        vec![vec![Value::Text("IndexLookup stock_idx".into())]]
    );
    assert_eq!(
        rows(&mut sql, "SELECT id FROM items WHERE stock=4"),
        vec![vec![Value::BigInt(1)]]
    );
    sql.execute("UPDATE items SET stock=stock-1, code='b' WHERE id=1", &[])
        .unwrap();
    assert!(rows(&mut sql, "SELECT id FROM items WHERE code='a'").is_empty());
    assert_eq!(
        rows(&mut sql, "SELECT id FROM items WHERE code='b'"),
        vec![vec![Value::BigInt(1)]]
    );
    sql.execute("DELETE FROM items WHERE id=3", &[]).unwrap();
    sql.execute("ALTER TABLE items ADD COLUMN note TEXT", &[])
        .unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT note FROM items ORDER BY id"),
        vec![vec![Value::Null], vec![Value::Null]]
    );
    drop(sql);
    drop(shared);
    let file = FileDatabase::open(&root.0, 2 * PAGE_SIZE).unwrap();
    let shared = SharedDatabase::new(file, SessionLimits::default()).unwrap();
    let mut sql = SqlSession::new(shared.connect().unwrap());
    assert_eq!(
        rows(&mut sql, "SELECT id,stock,code FROM items ORDER BY id"),
        vec![
            vec![Value::BigInt(1), Value::Int(3), Value::Text("b".into())],
            vec![Value::BigInt(2), Value::Null, Value::Null]
        ]
    );
}
#[test]
fn errors_abort_whole_statement_and_explicit_ddl_until_rollback() {
    let (_root, _shared, mut sql) = database();
    sql.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, n INT UNIQUE)", &[])
        .unwrap();
    assert_eq!(
        state(sql.execute("INSERT INTO t (id,n) VALUES (1,1),(2,1)", &[])),
        "23505"
    );
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::BigInt(0)]]
    );
    sql.execute("BEGIN", &[]).unwrap();
    sql.execute("INSERT INTO t (id,n) VALUES (1,1)", &[])
        .unwrap();
    sql.execute("ALTER TABLE t ADD COLUMN x TEXT", &[]).unwrap();
    assert_eq!(
        state(sql.execute("SELECT missing FROM t LIMIT 0", &[])),
        "42703"
    );
    assert!(matches!(
        sql.execute("COMMIT", &[]),
        Err(Error::TransactionAborted)
    ));
    sql.execute("ROLLBACK", &[]).unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::BigInt(0)]]
    );
    assert_eq!(state(sql.execute("SELECT x FROM t", &[])), "42703");
    sql.execute("BEGIN", &[]).unwrap();
    sql.execute("CREATE TABLE other (id BIGINT PRIMARY KEY)", &[])
        .unwrap();
    assert_eq!(
        state(sql.execute("SELECT id FROM other OR 1=1", &[])),
        "0A000"
    );
    sql.execute("ROLLBACK", &[]).unwrap();
    assert_eq!(state(sql.execute("SELECT id FROM other", &[])), "42P01");
}
#[test]
fn foreign_keys_join_self_reference_and_atomic_parent_changes() {
    let (_root, _shared, mut sql) = database();
    sql.execute("CREATE TABLE users (id BIGINT PRIMARY KEY, name TEXT)", &[])
        .unwrap();
    sql.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, user_id BIGINT REFERENCES users(id), amount INT)",&[]).unwrap();
    assert_eq!(
        state(sql.execute(
            "INSERT INTO orders (id,user_id,amount) VALUES (1,9,100)",
            &[]
        )),
        "23503"
    );
    sql.execute("INSERT INTO users (id,name) VALUES (9,'Анна')", &[])
        .unwrap();
    sql.execute(
        "INSERT INTO orders (id,user_id,amount) VALUES (1,9,100),(2,NULL,200)",
        &[],
    )
    .unwrap();
    assert_eq!(
        rows(
            &mut sql,
            "SELECT u.name,o.amount FROM users AS u INNER JOIN orders AS o ON u.id=o.user_id ORDER BY o.id"
        ),
        vec![vec![Value::Text("Анна".into()), Value::Int(100)]]
    );
    assert_eq!(
        state(sql.execute("DELETE FROM users WHERE id=9", &[])),
        "23503"
    );
    assert_eq!(
        state(sql.execute("UPDATE users SET id=10 WHERE id=9", &[])),
        "23503"
    );
    assert_eq!(state(sql.execute("DROP TABLE users", &[])), "2BP01");
    sql.execute(
        "CREATE TABLE nodes (id BIGINT PRIMARY KEY, parent BIGINT REFERENCES nodes(id))",
        &[],
    )
    .unwrap();
    sql.execute("INSERT INTO nodes (id,parent) VALUES (1,2),(2,1)", &[])
        .unwrap();
    sql.execute("DELETE FROM nodes", &[]).unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM nodes"),
        vec![vec![Value::BigInt(0)]]
    );
    sql.execute("DROP TABLE nodes", &[]).unwrap();
}
#[test]
fn binding_checks_empty_tables_and_integer_boundaries() {
    let (_root, _shared, mut sql) = database();
    sql.execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY,n INT,s TEXT,b BOOLEAN)",
        &[],
    )
    .unwrap();
    for query in [
        "SELECT missing FROM t",
        "SELECT id FROM t WHERE missing=0",
        "SELECT id FROM t ORDER BY missing",
        "UPDATE t SET missing=1",
        "DELETE FROM t WHERE missing=0",
    ] {
        assert_eq!(state(sql.execute(query, &[])), "42703", "{query}");
    }
    for query in [
        "SELECT id FROM t WHERE n='bad'",
        "SELECT id FROM t WHERE b=1",
        "SELECT id FROM t WHERE s+1=0",
        "UPDATE t SET n='bad'",
        "SELECT id FROM t WHERE 1",
    ] {
        let state = state(sql.execute(query, &[]));
        assert!(matches!(state, "42804" | "42883"), "{query}: {state}");
    }
    assert_eq!(
        state(sql.execute("UPDATE t SET n=$1", &[Value::BigInt(i64::MAX)])),
        "22003"
    );
    assert_eq!(
        state(sql.execute("SELECT id FROM t WHERE n=$1", &[])),
        "42P02"
    );
    sql.execute("INSERT INTO t (id,n,s,b) VALUES (-9223372036854775808,-2147483648,'a',true),(9223372036854775807,2147483647,'z',false)",&[]).unwrap();
    assert_eq!(
        state(sql.execute("UPDATE t SET n=n+1 WHERE id=9223372036854775807", &[])),
        "22003"
    );
    assert_eq!(
        state(sql.execute("UPDATE t SET id=id-1 WHERE id=-9223372036854775808", &[])),
        "22003"
    );
    assert_eq!(
        rows(&mut sql, "SELECT id FROM t ORDER BY id"),
        vec![vec![Value::BigInt(i64::MIN)], vec![Value::BigInt(i64::MAX)]]
    );
    assert_eq!(
        state(sql.execute("SELECT id FROM t LIMIT -1", &[])),
        "2201W"
    );
}
#[test]
fn read_only_parser_bounds_and_unsupported_clauses() {
    let (_root, _shared, mut sql) = database();
    sql.execute("CREATE TABLE t (id BIGINT PRIMARY KEY)", &[])
        .unwrap();
    sql.execute("BEGIN READ ONLY", &[]).unwrap();
    assert!(matches!(
        sql.execute("INSERT INTO t (id) VALUES (1)", &[]),
        Err(Error::ReadOnlyTransaction)
    ));
    sql.execute("ROLLBACK", &[]).unwrap();
    for query in [
        "SELECT id FROM t GROUP BY id",
        "SELECT id FROM t WHERE id=1 OR id=2",
        "SELECT id FROM t; DELETE FROM t",
        "CREATE TABLE q (id BIGINT PRIMARY KEY, n INT DEFAULT 0)",
        "INSERT INTO t (id) VALUES (1) RETURNING id",
    ] {
        assert!(sql.execute(query, &[]).is_err(), "accepted {query}");
    }
    let deep = format!("SELECT {}1{}", "(".repeat(1000), ")".repeat(1000));
    assert_eq!(state(sql.execute(&deep, &[])), "54000");
    assert_eq!(
        rows(&mut sql, "SELECT ';' AS s -- comment\n"),
        vec![vec![Value::Text(";".into())]]
    );
    sql.execute("CREATE TABLE \"Case\" (\"ID\" BIGINT PRIMARY KEY)", &[])
        .unwrap();
    sql.execute("INSERT INTO \"Case\" (\"ID\") VALUES (1)", &[])
        .unwrap();
    assert_eq!(
        rows(&mut sql, "SELECT \"ID\" FROM \"Case\""),
        vec![vec![Value::BigInt(1)]]
    );
}

#[test]
fn adjacent_operator_signs_and_reserved_names_match_postgres_lexing() {
    let (_root, _shared, mut sql) = database();
    for expression in ["1<>-1", "1<=-1", "1>=-1", "1=-1", "1<-1", "1>-1"] {
        let expected = !matches!(expression, "1<=-1" | "1=-1" | "1<-1");
        assert_eq!(
            rows(&mut sql, &format!("SELECT {expression} AS answer")),
            vec![vec![Value::Bool(expected)]],
            "{expression}"
        );
    }
    assert_eq!(
        state(sql.execute("CREATE TABLE select (id BIGINT PRIMARY KEY)", &[])),
        "42601"
    );
    sql.execute("CREATE TABLE \"select\" (id BIGINT PRIMARY KEY)", &[])
        .unwrap();
}

#[test]
fn concurrent_sql_catalog_primary_secondary_and_foreign_histories_are_serial() {
    for clients in [1, 2, 8, 32] {
        let (root, shared, mut setup) = database();
        setup
            .execute(
                "CREATE TABLE counter (id BIGINT PRIMARY KEY,n INT UNIQUE)",
                &[],
            )
            .unwrap();
        setup
            .execute("INSERT INTO counter (id,n) VALUES (1,0)", &[])
            .unwrap();
        setup.execute("CREATE TABLE audit (id BIGINT PRIMARY KEY,applied INT UNIQUE,counter_id BIGINT REFERENCES counter(id))",&[]).unwrap();
        drop(setup);
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(clients));
        let mut workers = Vec::new();
        for client in 0..clients {
            let db = shared.clone();
            let barrier = barrier.clone();
            workers.push(std::thread::spawn(move || {
                let mut sql = SqlSession::new(db.connect().unwrap());
                let mut acknowledgements = Vec::new();
                barrier.wait();
                for operation in 0..8 {
                    sql.execute("BEGIN", &[]).unwrap();
                    let old = match rows(&mut sql, "SELECT n FROM counter WHERE id=1")[0][0] {
                        Value::Int(n) => n,
                        _ => panic!("counter type"),
                    };
                    sql.execute("UPDATE counter SET n=n+1 WHERE id=1", &[])
                        .unwrap();
                    sql.execute(
                        "INSERT INTO audit (id,applied,counter_id) VALUES ($1,$2,1)",
                        &[Value::BigInt(i64::from(old) + 1), Value::Int(old + 1)],
                    )
                    .unwrap();
                    match operation % 4 {
                        0 => {
                            sql.execute(
                                &format!("CREATE TABLE private_{client} (id BIGINT PRIMARY KEY)"),
                                &[],
                            )
                            .unwrap();
                            sql.execute("ROLLBACK", &[]).unwrap();
                        }
                        1 => {
                            drop(sql);
                            sql = SqlSession::new(db.connect().unwrap());
                        }
                        2 => {
                            sql.cancel_handle().cancel();
                            assert!(sql.execute("COMMIT", &[]).is_err());
                            sql.execute("ROLLBACK", &[]).unwrap();
                        }
                        _ => {
                            sql.execute("COMMIT", &[]).unwrap();
                            acknowledgements.push(old + 1);
                        }
                    }
                }
                acknowledgements
            }));
        }
        let mut acknowledged = workers
            .into_iter()
            .flat_map(|w| w.join().unwrap())
            .collect::<Vec<_>>();
        acknowledged.sort();
        assert_eq!(acknowledged, (1..=(clients * 2) as i32).collect::<Vec<_>>());
        drop(shared);
        let file = FileDatabase::open(&root.0, 2 * PAGE_SIZE).unwrap();
        let db = SharedDatabase::new(file, SessionLimits::default()).unwrap();
        let mut sql = SqlSession::new(db.connect().unwrap());
        assert_eq!(
            rows(&mut sql, "SELECT applied FROM audit ORDER BY id"),
            acknowledged
                .iter()
                .map(|n| vec![Value::Int(*n)])
                .collect::<Vec<_>>()
        );
        assert_eq!(
            rows(&mut sql, "SELECT id FROM counter WHERE n=0"),
            Vec::<Vec<Value>>::new()
        );
        assert_eq!(
            rows(
                &mut sql,
                &format!("SELECT id FROM counter WHERE n={}", clients * 2)
            ),
            vec![vec![Value::BigInt(1)]]
        );
        for client in 0..clients {
            assert_eq!(
                state(sql.execute(&format!("SELECT id FROM private_{client}"), &[])),
                "42P01"
            );
        }
        eprintln!(
            "SQL clients={clients} acknowledged={} catalog/primary/secondary/FK consistent",
            acknowledged.len()
        );
    }
}

#[test]
fn parameterized_type_and_null_matrix_is_checked_before_empty_scan() {
    let (_root, _shared, mut sql) = database();
    sql.execute(
        "CREATE TABLE t (id BIGINT PRIMARY KEY,n INT,s TEXT,b BOOLEAN)",
        &[],
    )
    .unwrap();
    for i in 0..250 {
        let n = Value::Int(i - 125);
        let id = Value::BigInt(i64::from(i));
        assert!(
            sql.execute("SELECT id FROM t WHERE id=$1 AND n=$2", &[id, n])
                .unwrap()
                .rows
                .is_empty()
        );
        assert_eq!(
            state(sql.execute("UPDATE t SET n=$1", &[Value::Text(format!("wrong-{i}"))])),
            "42804"
        );
        assert_eq!(
            state(sql.execute("SELECT id FROM t WHERE b=$1", &[Value::Int(i)])),
            "42883"
        );
        assert_eq!(
            sql.execute(
                "SELECT $1 IS NULL AS answer",
                &[if i % 2 == 0 {
                    Value::Null
                } else {
                    Value::Bool(true)
                }]
            )
            .unwrap()
            .rows,
            vec![vec![Value::Bool(i % 2 == 0)]]
        );
    }
}

#[test]
fn failed_index_build_and_result_memory_bound_preserve_acknowledged_rows() {
    let (_root, _shared, mut sql) = database();
    sql.execute("CREATE TABLE t (id BIGINT PRIMARY KEY,s TEXT)", &[])
        .unwrap();
    for i in 0..80 {
        sql.execute(
            "INSERT INTO t (id,s) VALUES ($1,$2)",
            &[Value::BigInt(i), Value::Text("x".repeat(7000))],
        )
        .unwrap();
    }
    assert_eq!(
        state(sql.execute("CREATE INDEX huge_idx ON t(s)", &[])),
        "54000"
    );
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::BigInt(80)]]
    );
    assert_eq!(
        state(sql.execute(&format!("SELECT {} FROM t", vec!["s"; 16].join(",")), &[])),
        "54000"
    );
    assert_eq!(
        rows(&mut sql, "SELECT COUNT(*) FROM t"),
        vec![vec![Value::BigInt(80)]]
    );
    // LIMIT can bound an unsorted result without materializing the whole scan.
    assert_eq!(
        sql.execute("SELECT s FROM t LIMIT 1", &[])
            .unwrap()
            .rows
            .len(),
        1
    );
}

#[test]
fn export_io_failure_never_mutates_database_and_releases_read_snapshot() {
    use std::io::Write;
    struct FullDisk;
    impl Write for FullDisk {
        fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
            Err(std::io::Error::from_raw_os_error(28))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    struct FailedFlush;
    impl Write for FailedFlush {
        fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
            Ok(b.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::other("injected final flush failure"))
        }
    }
    let (_root, _shared, mut sql) = database();
    sql.execute("CREATE TABLE t (id BIGINT PRIMARY KEY, name TEXT)", &[])
        .unwrap();
    sql.execute("INSERT INTO t (id,name) VALUES (1,'acknowledged')", &[])
        .unwrap();
    assert!(sql.export_sql(FullDisk).is_err());
    assert!(sql.export_sql(FailedFlush).is_err());
    assert_eq!(
        rows(&mut sql, "SELECT name FROM t"),
        vec![vec![Value::Text("acknowledged".into())]]
    );
    sql.execute(
        "INSERT INTO t (id,name) VALUES (2,'writer still admitted')",
        &[],
    )
    .unwrap();
    assert_eq!(sql.export_sql(std::io::sink()).unwrap().affected, 2);
}
