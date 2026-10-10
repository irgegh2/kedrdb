// SPDX-License-Identifier: Apache-2.0
//! Canonical streaming SQL export and resumable quarantine import.
use super::{
    DataType, QueryResult, Store,
    catalog::{self, Table},
    error, executor,
    parser::{self, Expr, Statement},
};
use crate::{Error, Operation, Result, record::Value};
use std::{collections::BTreeSet, io::Write};
pub const HEADER: &str = "-- KedrDB SQL export 1\n";
pub const FOOTER: &str = "-- KedrDB SQL export end\n";
const PROGRESS: &[u8] = &[2];
fn identifier(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".into(),
        Value::Int(n) => n.to_string(),
        Value::BigInt(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Text(s) => format!("'{}'", s.replace('\'', "''")),
    }
}
fn write(w: &mut dyn Write, s: &str) -> Result<()> {
    w.write_all(s.as_bytes())
        .map_err(|e| Error::io(Operation::Write, e))
}
fn order(mut remaining: Vec<Table>) -> Result<Vec<Table>> {
    let mut done = BTreeSet::new();
    let mut ordered = Vec::new();
    while !remaining.is_empty() {
        let i = remaining
            .iter()
            .position(|t| {
                t.columns.iter().all(|c| {
                    c.foreign
                        .as_ref()
                        .is_none_or(|f| f.table == t.name || done.contains(&f.table))
                })
            })
            .ok_or_else(|| error("0A000", "export cannot order cyclic table dependencies"))?;
        let t = remaining.remove(i);
        done.insert(t.name.clone());
        ordered.push(t);
    }
    Ok(ordered)
}
pub(super) fn validate(store: &mut dyn Store) -> Result<u64> {
    if store.get(&[1])?.is_none() && !store.range(None, None, 1)?.is_empty() {
        return Err(error(
            "0A000",
            "raw key/value database cannot be exported as SQL",
        ));
    }
    let tables = catalog::list(store)?;
    let mut rows = 0;
    for table in &tables {
        catalog::scan_keys(store, &catalog::row_prefix(table), |s, key, bytes| {
            let row = catalog::decode(table, &bytes)?;
            let pk = catalog::pk(table, &row)?;
            if key != catalog::row_key(table, pk) {
                return Err(error("XX001", "row key/primary mismatch"));
            }
            for (i, c) in table.columns.iter().enumerate() {
                if let Some(f) = &c.foreign
                    && let Value::BigInt(parent) = row[i]
                {
                    let parent_table = tables
                        .iter()
                        .find(|t| t.name == f.table)
                        .ok_or_else(|| error("XX001", "foreign parent missing"))?;
                    if s.get(&catalog::row_key(parent_table, parent))?.is_none() {
                        return Err(error("23503", "import foreign key violation"));
                    }
                }
            }
            for index in &table.indices {
                if s.get(&catalog::index_key(table, index, &row)?)?.as_deref()
                    != Some(&pk.to_le_bytes())
                {
                    return Err(error("XX001", "secondary index missing/mismatch"));
                }
            }
            rows += 1;
            Ok(())
        })?;
        for index in &table.indices {
            catalog::scan_keys(
                store,
                &catalog::index_prefix(table, index),
                |s, key, bytes| {
                    let pk = i64::from_le_bytes(
                        bytes
                            .as_slice()
                            .try_into()
                            .map_err(|_| error("XX001", "index primary width"))?,
                    );
                    let bytes = s
                        .get(&catalog::row_key(table, pk))?
                        .ok_or_else(|| error("XX001", "orphan secondary index"))?;
                    if catalog::index_key(table, index, &catalog::decode(table, &bytes)?)? != key {
                        return Err(error("XX001", "secondary key mismatch"));
                    }
                    Ok(())
                },
            )?;
        }
    }
    Ok(rows)
}
pub(super) fn export(store: &mut dyn Store, w: &mut dyn Write) -> Result<QueryResult> {
    validate(store)?;
    let tables = order(catalog::list(store)?)?;
    write(w, HEADER)?;
    let mut count = 0;
    for table in &tables {
        let mut cols = Vec::new();
        for c in &table.columns {
            let mut s = format!(
                "{} {}",
                identifier(&c.name),
                match c.data_type {
                    DataType::Int => "INT",
                    DataType::BigInt => "BIGINT",
                    DataType::Text => "TEXT",
                    DataType::Boolean => "BOOLEAN",
                }
            );
            if c.primary {
                s.push_str(" PRIMARY KEY");
            } else {
                if !c.nullable {
                    s.push_str(" NOT NULL");
                }
                if c.unique {
                    s.push_str(" UNIQUE");
                }
            }
            if let Some(f) = &c.foreign {
                s.push_str(&format!(
                    " REFERENCES {} ({})",
                    identifier(&f.table),
                    identifier(&f.column)
                ));
            }
            cols.push(s);
        }
        write(
            w,
            &format!(
                "CREATE TABLE {} ({});\n",
                identifier(&table.name),
                cols.join(", ")
            ),
        )?;
    }
    for table in &tables {
        for i in &table.indices {
            if table.columns[i.column].unique && i.name == format!("__u_{}_{}", table.id, i.column)
            {
                continue;
            }
            write(
                w,
                &format!(
                    "CREATE {}INDEX {} ON {} ({});\n",
                    if i.unique { "UNIQUE " } else { "" },
                    identifier(&i.name),
                    identifier(&table.name),
                    identifier(&table.columns[i.column].name)
                ),
            )?;
        }
    }
    for table in &tables {
        let columns = table
            .columns
            .iter()
            .map(|c| identifier(&c.name))
            .collect::<Vec<_>>()
            .join(", ");
        catalog::scan_keys(store, &catalog::row_prefix(table), |_, _, bytes| {
            let row = catalog::decode(table, &bytes)?;
            write(
                w,
                &format!(
                    "INSERT INTO {} ({columns}) VALUES ({});\n",
                    identifier(&table.name),
                    row.iter().map(literal).collect::<Vec<_>>().join(", ")
                ),
            )?;
            count += 1;
            Ok(())
        })?;
    }
    write(w, FOOTER)?;
    w.flush().map_err(|e| Error::io(Operation::Write, e))?;
    Ok(QueryResult::command("EXPORT", count))
}
pub fn validate_statement(sql: &str) -> Result<()> {
    match parser::parse(sql)? {
        Statement::Create(..) | Statement::CreateIndex { .. } => Ok(()),
        Statement::Insert { values, .. }
            if values
                .iter()
                .flatten()
                .all(|e| matches!(e, Expr::Literal(_))) =>
        {
            Ok(())
        }
        _ => Err(error(
            "0A000",
            "import accepts only generated CREATE TABLE/INDEX and literal INSERT",
        )),
    }
}
fn progress(store: &mut dyn Store, job: &[u8; 32]) -> Result<(u64, bool)> {
    match store.get(PROGRESS)? {
        None => Ok((0, false)),
        Some(b) if b.len() == 49 && &b[..8] == b"KEDRIMP1" && &b[8..40] == job && b[48] <= 1 => {
            Ok((
                u64::from_le_bytes(b[40..48].try_into().unwrap()),
                b[48] == 1,
            ))
        }
        _ => Err(error("XX001", "import job/progress mismatch")),
    }
}
fn save_progress(store: &mut dyn Store, job: &[u8; 32], ordinal: u64, done: bool) -> Result<()> {
    let mut b = b"KEDRIMP1".to_vec();
    b.extend(job);
    b.extend(ordinal.to_le_bytes());
    b.push(u8::from(done));
    store.replace(PROGRESS, &b)
}
pub(super) fn import(
    store: &mut dyn Store,
    sql: &str,
    job: &[u8; 32],
    ordinal: u64,
) -> Result<QueryResult> {
    validate_statement(sql)?;
    let (last, done) = progress(store, job)?;
    if ordinal == 0 || ordinal > last.saturating_add(1) {
        return Err(error("XX001", "import ordinal gap"));
    }
    if ordinal <= last {
        return Ok(QueryResult::command("IMPORT SKIP", 0));
    }
    if done {
        return Err(error("XX001", "import already finished"));
    }
    if last == 0 && !store.range(None, None, 1)?.is_empty() {
        return Err(error("55000", "import requires an empty database"));
    }
    let result = match parser::parse(sql)? {
        Statement::Insert {
            table,
            columns,
            values,
        } => executor::insert_import(store, &table, &columns, &values)?,
        statement => executor::execute(store, statement, &[])?,
    };
    save_progress(store, job, ordinal, false)?;
    Ok(result)
}
pub(super) fn finish(store: &mut dyn Store, job: &[u8; 32], total: u64) -> Result<QueryResult> {
    let (last, _) = progress(store, job)?;
    if last != total {
        return Err(error("XX001", "incomplete import progress"));
    }
    let rows = validate(store)?;
    if total != 0 {
        save_progress(store, job, total, true)?;
    }
    Ok(QueryResult::command("IMPORT COMPLETE", rows))
}
