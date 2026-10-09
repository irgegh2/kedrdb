// SPDX-License-Identifier: Apache-2.0
use kedr_core::{
    Error, Operation, Result,
    database::FileDatabase,
    record::Value,
    sql::{DataType, MAX_SQL_BYTES, QueryResult, SqlSession},
    transaction::{SessionLimits, SharedDatabase},
};
use std::{
    io::{self, BufRead, Read, Write},
    path::Path,
};

fn quote(text: &str) -> String {
    let mut output = String::from("\"");
    for c in text.chars() {
        match c {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            c if c < ' ' => output.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => output.push(c),
        }
    }
    output.push('"');
    output
}
fn value(value: &Value) -> String {
    match value {
        Value::Null => "null".into(),
        Value::Int(v) => v.to_string(),
        Value::BigInt(v) => v.to_string(),
        Value::Bool(v) => v.to_string(),
        Value::Text(s) => quote(s),
    }
}
fn result_json(result: &QueryResult) -> String {
    let columns = result
        .columns
        .iter()
        .map(|c| {
            format!(
                "{{\"name\":{},\"type\":{}}}",
                quote(&c.name),
                quote(match c.data_type {
                    DataType::Int => "int4",
                    DataType::BigInt => "int8",
                    DataType::Text => "text",
                    DataType::Boolean => "bool",
                })
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let rows = result
        .rows
        .iter()
        .map(|r| format!("[{}]", r.iter().map(value).collect::<Vec<_>>().join(",")))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"columns\":[{columns}],\"rows\":[{rows}],\"affected\":{},\"tag\":{}}}",
        result.affected,
        quote(&result.tag)
    )
}
fn error_json(error: &Error) -> String {
    let state = match error {
        Error::Sql { state, .. } => *state,
        Error::TransactionAborted => "25P02",
        Error::ReadOnlyTransaction => "25006",
        Error::InvalidTransactionState => "25000",
        Error::Cancelled => "57014",
        Error::TransactionTimeout => "57014",
        Error::LockTimeout => "55P03",
        Error::CommitUncertain { .. } => "08007",
        _ => "XX000",
    };
    format!(
        "{{\"error\":{},\"code\":{},\"message\":{}}}",
        quote(state),
        quote(error.code()),
        quote(&error.to_string())
    )
}
fn connect(path: &Path) -> Result<(SharedDatabase, SqlSession)> {
    let db = SharedDatabase::new(
        FileDatabase::open(path, super::CACHE_BUDGET)?,
        SessionLimits::default(),
    )?;
    let session = SqlSession::new(db.connect()?);
    Ok((db, session))
}
pub(super) fn single(path: &Path, query: &str) -> Result<()> {
    let (_database, mut sql) = connect(path)?;
    let result = sql.execute(query, &[])?;
    println!("{}", result_json(&result));
    Ok(())
}
/// A bounded line-oriented local SQL session, useful for reproducible scripts.
/// Each input line is one complete statement and each output line one JSON result.
pub(super) fn stream(path: &Path) -> Result<()> {
    let (_database, mut sql) = connect(path)?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    loop {
        let mut text = String::new();
        let n = input
            .by_ref()
            .take((MAX_SQL_BYTES + 2) as u64)
            .read_line(&mut text)
            .map_err(|e| Error::io(Operation::Read, e))?;
        if n == 0 {
            return Ok(());
        }
        if text.len() > MAX_SQL_BYTES + 1 || (!text.ends_with('\n') && text.len() > MAX_SQL_BYTES) {
            return Err(Error::InvalidArgument {
                message: "SQL input line exceeds 64 KiB",
            });
        }
        let text = text.trim_end_matches(['\n', '\r']);
        let result = match sql.execute(text, &[]) {
            Ok(result) => result_json(&result),
            Err(error) => error_json(&error),
        };
        writeln!(output, "{result}")
            .and_then(|_| output.flush())
            .map_err(|e| Error::io(Operation::Write, e))?;
    }
}
