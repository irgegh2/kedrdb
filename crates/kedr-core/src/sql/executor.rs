// SPDX-License-Identifier: Apache-2.0
use super::{
    DataType, MAX_RESULT_BYTES, MAX_WORK_BYTES, QueryResult, ResultColumn, Store,
    catalog::{self, Index, Table},
    describe, error,
    expression::{self, Bound, Field, Kind},
    parser::{Column, Expr, Op, Projection, Select, Statement},
};
use crate::{Result, record::Value};
use std::collections::BTreeSet;

pub(super) fn execute(
    store: &mut dyn Store,
    statement: Statement,
    params: &[Value],
) -> Result<QueryResult> {
    match statement {
        Statement::Create(name, columns) => create(store, name, columns),
        Statement::Drop(name) => drop_table(store, &name),
        Statement::Add(name, column) => add(store, &name, column),
        Statement::CreateIndex {
            name,
            table,
            column,
            unique,
        } => create_index(store, name, &table, &column, unique),
        Statement::Insert {
            table,
            columns,
            values,
        } => insert(store, &table, &columns, &values, params),
        Statement::Select(select) => select_rows(store, &select, params, false, false),
        Statement::Explain(select) => select_rows(store, &select, params, true, false),
        Statement::Catalog(target) => describe::catalog_result(store, target.as_ref()),
        Statement::Update {
            table,
            assignments,
            filter,
        } => update(store, &table, &assignments, filter.as_ref(), params),
        Statement::Delete { table, filter } => delete(store, &table, filter.as_ref(), params),
        _ => Err(error("XX000", "transaction statement reached executor")),
    }
}
fn column_index(table: &Table, name: &str) -> Result<usize> {
    table
        .columns
        .iter()
        .position(|c| c.name == name)
        .ok_or_else(|| error("42703", format!("column {name} does not exist")))
}
pub(super) fn fields(table: &Table, alias: &str) -> Vec<Field> {
    table
        .columns
        .iter()
        .map(|c| Field {
            qualifier: alias.into(),
            name: c.name.clone(),
            data_type: c.data_type,
        })
        .collect()
}
fn create(store: &mut dyn Store, name: String, columns: Vec<Column>) -> Result<QueryResult> {
    if store.get(&catalog::table_key(&name))?.is_some() {
        return Err(error("42P07", "table already exists"));
    }
    let tables = catalog::list(store)?;
    if tables
        .iter()
        .any(|t| t.indices.iter().any(|i| i.name == name))
    {
        return Err(error("42P07", "relation name already exists"));
    }
    if tables.len() >= 128 {
        return Err(error("54000", "table limit exceeded"));
    }
    if columns.is_empty() || columns.len() > 256 {
        return Err(error("54000", "table requires 1..256 columns"));
    }
    let mut names = BTreeSet::new();
    for column in &columns {
        if !names.insert(&column.name) {
            return Err(error("42701", "duplicate column"));
        }
    }
    let primary = columns
        .iter()
        .position(|c| c.primary)
        .ok_or_else(|| error("0A000", "one BIGINT primary key required"))?;
    if columns.iter().filter(|c| c.primary).count() != 1
        || columns[primary].data_type != DataType::BigInt
    {
        return Err(error("0A000", "one BIGINT primary key required"));
    }
    let mut table = Table {
        id: catalog::allocate_id(store)?,
        name,
        columns,
        primary,
        indices: Vec::new(),
    };
    for (i, column) in table.columns.iter().enumerate() {
        if let Some(foreign) = &column.foreign {
            let parent = if foreign.table == table.name {
                table.clone()
            } else {
                catalog::load(store, &foreign.table)?
            };
            if column.data_type != DataType::BigInt
                || parent.columns[parent.primary].name != foreign.column
            {
                return Err(error(
                    "42830",
                    "foreign key must reference a BIGINT primary key",
                ));
            }
        }
        if column.unique && i != primary {
            table.indices.push(Index {
                id: table.indices.len() as u64 + 1,
                name: format!("__u_{}_{}", table.id, i),
                column: i,
                unique: true,
            });
        }
    }
    catalog::save(store, &table)?;
    Ok(QueryResult::command("CREATE TABLE", 0))
}
fn add(store: &mut dyn Store, name: &str, column: Column) -> Result<QueryResult> {
    let mut table = catalog::load(store, name)?;
    if !column.nullable || column.primary || column.unique || column.foreign.is_some() {
        return Err(error(
            "0A000",
            "ADD COLUMN requires nullable column without constraints or DEFAULT",
        ));
    }
    if table.columns.iter().any(|c| c.name == column.name) {
        return Err(error("42701", "column already exists"));
    }
    if table.columns.len() >= 256 {
        return Err(error("54000", "column limit exceeded"));
    }
    table.columns.push(column);
    catalog::save(store, &table)?;
    Ok(QueryResult::command("ALTER TABLE", 0))
}
fn drop_table(store: &mut dyn Store, name: &str) -> Result<QueryResult> {
    let table = catalog::load(store, name)?;
    for other in catalog::list(store)? {
        if other.name != name
            && other
                .columns
                .iter()
                .any(|c| c.foreign.as_ref().is_some_and(|f| f.table == name))
        {
            return Err(error(
                "2BP01",
                "referencing foreign key prevents DROP TABLE",
            ));
        }
    }
    catalog::scan_keys(store, &catalog::row_prefix(&table), |s, k, _| {
        s.delete(&k).map(|_| ())
    })?;
    for index in &table.indices {
        catalog::scan_keys(store, &catalog::index_prefix(&table, index), |s, k, _| {
            s.delete(&k).map(|_| ())
        })?;
    }
    store.delete(&catalog::table_key(name))?;
    Ok(QueryResult::command("DROP TABLE", 0))
}
fn create_index(
    store: &mut dyn Store,
    name: String,
    table_name: &str,
    column: &str,
    unique: bool,
) -> Result<QueryResult> {
    let mut table = catalog::load(store, table_name)?;
    let column = column_index(&table, column)?;
    if catalog::list(store)?
        .iter()
        .any(|t| t.name == name || t.indices.iter().any(|i| i.name == name))
    {
        return Err(error("42P07", "index name already exists"));
    }
    if table.indices.len() >= 256 {
        return Err(error("54000", "index limit exceeded"));
    }
    let index = Index {
        id: table.indices.iter().map(|i| i.id).max().unwrap_or(0) + 1,
        name,
        column,
        unique,
    };
    catalog::scan_keys(store, &catalog::row_prefix(&table), |s, _, v| {
        let row = catalog::decode(&table, &v)?;
        insert_index(s, &table, &index, &row, None)
    })?;
    table.indices.push(index);
    catalog::save(store, &table)?;
    Ok(QueryResult::command("CREATE INDEX", 0))
}
fn check_row(table: &Table, row: &[Value]) -> Result<()> {
    for (column, value) in table.columns.iter().zip(row) {
        if !column.nullable && matches!(value, Value::Null) {
            return Err(error(
                "23502",
                format!("column {} cannot be NULL", column.name),
            ));
        }
    }
    catalog::pk(table, row)?;
    catalog::encoded(row)?;
    Ok(())
}
fn insert_index(
    store: &mut dyn Store,
    table: &Table,
    index: &Index,
    row: &[Value],
    replacing: Option<i64>,
) -> Result<()> {
    let key = catalog::index_key(table, index, row)?;
    if index.unique && !matches!(row[index.column], Value::Null) {
        let prefix = &key[..key.len() - 8];
        let end = catalog::prefix_end(prefix);
        for (_, value) in store.range(Some(prefix), Some(&end), 2)? {
            if value.len() != 8 {
                return Err(error("XX001", "invalid secondary primary key"));
            }
            let primary = i64::from_le_bytes(value.try_into().expect("width"));
            if Some(primary) != replacing {
                return Err(error(
                    "23505",
                    format!("unique index {} conflict", index.name),
                ));
            }
        }
    }
    store
        .insert(&key, &catalog::pk(table, row)?.to_le_bytes())
        .map_err(|e| {
            if matches!(e, crate::Error::DuplicateKey) {
                error("23505", "duplicate secondary key")
            } else {
                e
            }
        })
}
fn insert_row(store: &mut dyn Store, table: &Table, row: &[Value]) -> Result<()> {
    check_row(table, row)?;
    store
        .insert(
            &catalog::row_key(table, catalog::pk(table, row)?),
            &catalog::encoded(row)?,
        )
        .map_err(|e| {
            if matches!(e, crate::Error::DuplicateKey) {
                error("23505", "duplicate primary key")
            } else {
                e
            }
        })?;
    for index in &table.indices {
        insert_index(store, table, index, row, None)?;
    }
    Ok(())
}
fn delete_row(store: &mut dyn Store, table: &Table, row: &[Value]) -> Result<()> {
    for index in &table.indices {
        if !store.delete(&catalog::index_key(table, index, row)?)? {
            return Err(error("XX001", "secondary entry missing"));
        }
    }
    if !store.delete(&catalog::row_key(table, catalog::pk(table, row)?))? {
        return Err(error("XX001", "row missing during delete"));
    }
    Ok(())
}
/// Foreign keys are checked against the final statement state, including bulk
/// self references. Errors abort the containing private-page transaction.
fn validate_foreign_keys(store: &mut dyn Store) -> Result<()> {
    let tables = catalog::list(store)?;
    for table in &tables {
        let foreign = table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(i, c)| c.foreign.as_ref().map(|f| (i, f)))
            .collect::<Vec<_>>();
        if foreign.is_empty() {
            continue;
        }
        catalog::scan_keys(store, &catalog::row_prefix(table), |s, _, bytes| {
            let row = catalog::decode(table, &bytes)?;
            for (i, f) in &foreign {
                if let Value::BigInt(primary) = row[*i] {
                    let parent = tables
                        .iter()
                        .find(|t| t.name == f.table)
                        .ok_or_else(|| error("XX001", "foreign parent missing"))?;
                    if s.get(&catalog::row_key(parent, primary))?.is_none() {
                        return Err(error("23503", "foreign key violation"));
                    }
                }
            }
            Ok(())
        })?;
    }
    Ok(())
}
fn validate_inserted_foreign_rows(
    store: &mut dyn Store,
    table: &Table,
    rows: &[Vec<Value>],
) -> Result<()> {
    let mut parents = Vec::new();
    for (i, column) in table.columns.iter().enumerate() {
        if let Some(foreign) = &column.foreign {
            parents.push((i, catalog::load(store, &foreign.table)?));
        }
    }
    for row in rows {
        for (i, parent) in &parents {
            if let Value::BigInt(primary) = row[*i]
                && store.get(&catalog::row_key(parent, primary))?.is_none()
            {
                return Err(error("23503", "foreign key violation"));
            }
        }
    }
    Ok(())
}
pub(super) fn insert_import(
    store: &mut dyn Store,
    table: &str,
    columns: &[String],
    values: &[Vec<Expr>],
) -> Result<QueryResult> {
    insert_mode(store, table, columns, values, &[], false)
}
fn insert(
    store: &mut dyn Store,
    name: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    params: &[Value],
) -> Result<QueryResult> {
    insert_mode(store, name, columns, values, params, true)
}
fn insert_mode(
    store: &mut dyn Store,
    name: &str,
    columns: &[String],
    values: &[Vec<Expr>],
    params: &[Value],
    check_foreign: bool,
) -> Result<QueryResult> {
    let table = catalog::load(store, name)?;
    let mut selected = BTreeSet::new();
    let mut indices = Vec::new();
    for name in columns {
        let i = column_index(&table, name)?;
        if !selected.insert(i) {
            return Err(error("42701", "duplicate INSERT column"));
        }
        indices.push(i);
    }
    // Bind every row before modifying storage. Empty tables never bypass types.
    let bound = values
        .iter()
        .map(|row| {
            if row.len() != indices.len() {
                return Err(error("42601", "INSERT value count differs from columns"));
            }
            row.iter()
                .zip(&indices)
                .map(|(e, i)| expression::assign(e, &[], params, table.columns[*i].data_type))
                .collect::<Result<Vec<_>>>()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut inserted = Vec::new();
    for row in bound {
        store.check()?;
        let mut result = vec![Value::Null; table.columns.len()];
        for (expr, i) in row.iter().zip(&indices) {
            result[*i] = catalog::coerce(expr.eval(&[])?, table.columns[*i].data_type)?;
        }
        insert_row(store, &table, &result)?;
        inserted.push(result);
    }
    if check_foreign {
        validate_inserted_foreign_rows(store, &table, &inserted)?;
    }
    Ok(QueryResult::command("INSERT", values.len() as u64))
}
fn targets(
    store: &mut dyn Store,
    table: &Table,
    predicate: Option<&Bound>,
) -> Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    let mut memory = 0;
    catalog::scan_keys(store, &catalog::row_prefix(table), |_, _, bytes| {
        let row = catalog::decode(table, &bytes)?;
        if predicate
            .map(|p| p.matches(&row))
            .transpose()?
            .unwrap_or(true)
        {
            memory += row.iter().map(expression::value_size).sum::<usize>()
                + std::mem::size_of::<Vec<Value>>();
            if memory > MAX_WORK_BYTES / 2 {
                return Err(error("54000", "mutation working set exceeds memory limit"));
            }
            rows.push(row);
        }
        Ok(())
    })?;
    Ok(rows)
}
fn update(
    store: &mut dyn Store,
    name: &str,
    assignments: &[(String, Expr)],
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<QueryResult> {
    let table = catalog::load(store, name)?;
    let fields = fields(&table, name);
    let predicate = expression::boolean(filter, &fields, params)?;
    let mut seen = BTreeSet::new();
    let assignments = assignments
        .iter()
        .map(|(name, expr)| {
            let i = column_index(&table, name)?;
            if !seen.insert(i) {
                return Err(error("42701", "column assigned twice"));
            }
            Ok((
                i,
                expression::assign(expr, &fields, params, table.columns[i].data_type)?,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let rows = targets(store, &table, predicate.as_ref())?;
    let count = rows.len() as u64;
    for old in rows {
        store.check()?;
        let mut row = old.clone();
        for (i, expr) in &assignments {
            row[*i] = catalog::coerce(expr.eval(&old)?, table.columns[*i].data_type)?;
        }
        check_row(&table, &row)?;
        delete_row(store, &table, &old)?;
        insert_row(store, &table, &row)?;
    }
    if seen.contains(&table.primary)
        || table
            .columns
            .iter()
            .enumerate()
            .any(|(i, c)| c.foreign.is_some() && seen.contains(&i))
    {
        validate_foreign_keys(store)?;
    }
    Ok(QueryResult::command("UPDATE", count))
}
fn delete(
    store: &mut dyn Store,
    name: &str,
    filter: Option<&Expr>,
    params: &[Value],
) -> Result<QueryResult> {
    let table = catalog::load(store, name)?;
    let predicate = expression::boolean(filter, &fields(&table, name), params)?;
    let rows = targets(store, &table, predicate.as_ref())?;
    let count = rows.len() as u64;
    for row in rows {
        store.check()?;
        delete_row(store, &table, &row)?;
    }
    validate_foreign_keys(store)?;
    Ok(QueryResult::command("DELETE", count))
}
#[derive(Clone)]
enum Access {
    Scan,
    Primary(i64),
    Index(Index, Vec<u8>),
}
fn equality(expr: &Bound, column: usize) -> Option<Value> {
    match &expr.kind {
        Kind::Binary(a, Op::And, b) => equality(a, column).or_else(|| equality(b, column)),
        Kind::Binary(a, Op::Eq, b) => match (&a.kind, &b.kind) {
            (Kind::Column(i), Kind::Literal(v)) | (Kind::Literal(v), Kind::Column(i))
                if *i == column =>
            {
                Some(v.clone())
            }
            _ => None,
        },
        Kind::IsNull(a, false) if matches!(a.kind,Kind::Column(i) if i==column) => {
            Some(Value::Null)
        }
        _ => None,
    }
}
fn choose(table: &Table, predicate: Option<&Bound>) -> Access {
    let Some(predicate) = predicate else {
        return Access::Scan;
    };
    if let Some(value) = equality(predicate, table.primary)
        && let Ok(Value::BigInt(n)) = catalog::coerce(value, DataType::BigInt)
    {
        return Access::Primary(n);
    }
    for index in &table.indices {
        if let Some(value) = equality(predicate, index.column)
            && let Ok(value) = catalog::coerce(value, table.columns[index.column].data_type)
        {
            let mut row = vec![Value::Null; table.columns.len()];
            row[table.primary] = Value::BigInt(0);
            row[index.column] = value;
            if let Ok(mut prefix) = catalog::index_key(table, index, &row) {
                prefix.truncate(prefix.len() - 8);
                return Access::Index(index.clone(), prefix);
            }
        }
    }
    Access::Scan
}
fn scan(
    store: &mut dyn Store,
    table: &Table,
    access: &Access,
    mut visit: impl FnMut(&mut dyn Store, Vec<Value>) -> Result<()>,
) -> Result<()> {
    match access {
        Access::Primary(primary) => {
            if let Some(bytes) = store.get(&catalog::row_key(table, *primary))? {
                visit(store, catalog::decode(table, &bytes)?)?;
            }
            Ok(())
        }
        Access::Scan => catalog::scan_keys(store, &catalog::row_prefix(table), |s, _, bytes| {
            visit(s, catalog::decode(table, &bytes)?)
        }),
        Access::Index(index, prefix) => catalog::scan_keys(store, prefix, |s, key, value| {
            if value.len() != 8 {
                return Err(error("XX001", "index value invalid"));
            }
            let primary = i64::from_le_bytes(value.try_into().expect("width"));
            let bytes = s
                .get(&catalog::row_key(table, primary))?
                .ok_or_else(|| error("XX001", "index references missing row"))?;
            let row = catalog::decode(table, &bytes)?;
            if catalog::index_key(table, index, &row)? != key {
                return Err(error("XX001", "index entry differs from row"));
            }
            visit(s, row)
        }),
    }
}
fn access_name(access: &Access) -> String {
    match access {
        Access::Scan => "FullScan".into(),
        Access::Primary(_) => "PrimaryKeyLookup".into(),
        Access::Index(i, _) => format!("IndexLookup {}", i.name),
    }
}
pub(super) fn select_rows(
    store: &mut dyn Store,
    select: &Select,
    params: &[Value],
    explain: bool,
    describe_only: bool,
) -> Result<QueryResult> {
    let left = select
        .source
        .as_ref()
        .map(|s| catalog::load(store, &s.table))
        .transpose()?;
    let right = select
        .join
        .as_ref()
        .map(|(s, _)| catalog::load(store, &s.table))
        .transpose()?;
    if right.is_some() && left.is_none() {
        return Err(error("42601", "JOIN requires FROM"));
    }
    let mut scope = left
        .as_ref()
        .zip(select.source.as_ref())
        .map(|(t, s)| fields(t, &s.alias))
        .unwrap_or_default();
    let left_width = scope.len();
    if let Some((table, (source, _))) = right.as_ref().zip(select.join.as_ref()) {
        if select
            .source
            .as_ref()
            .is_some_and(|s| s.alias == source.alias)
        {
            return Err(error("42712", "duplicate table alias"));
        }
        scope.extend(fields(table, &source.alias));
    }
    let predicate = expression::boolean(select.filter.as_ref(), &scope, params)?;
    let join = select
        .join
        .as_ref()
        .map(|(_, e)| expression::bind(e, &scope, params))
        .transpose()?;
    if let Some(join) = &join
        && !matches!(&join.kind,Kind::Binary(a,Op::Eq,b) if matches!((&a.kind,&b.kind),(Kind::Column(i),Kind::Column(j)) if (*i<left_width && *j>=left_width)||(*j<left_width && *i>=left_width)))
    {
        return Err(error(
            "0A000",
            "INNER JOIN requires equality of columns from the two tables",
        ));
    }
    let count = select
        .projection
        .iter()
        .any(|p| matches!(p, Projection::Count(_)));
    if count && select.projection.len() != 1 {
        return Err(error(
            "0A000",
            "COUNT(*) cannot be mixed with other projections",
        ));
    }
    let mut columns = Vec::new();
    let mut projection = Vec::new();
    for item in &select.projection {
        match item {
            Projection::All => {
                if count || scope.is_empty() {
                    return Err(error("42601", "wildcard requires table"));
                }
                for (i, f) in scope.iter().enumerate() {
                    columns.push(ResultColumn {
                        name: f.name.clone(),
                        data_type: f.data_type,
                    });
                    projection.push(Bound {
                        kind: Kind::Column(i),
                        data_type: Some(f.data_type),
                    });
                }
            }
            Projection::Count(alias) => columns.push(ResultColumn {
                name: alias.clone().unwrap_or_else(|| "count".into()),
                data_type: DataType::BigInt,
            }),
            Projection::Expression(e, alias) => {
                let b = expression::bind(e, &scope, params)?;
                let name = alias.clone().unwrap_or_else(|| match e {
                    Expr::Name(n) => n.last().expect("name").clone(),
                    _ => "?column?".into(),
                });
                columns.push(ResultColumn {
                    name,
                    data_type: b.data_type.unwrap_or(DataType::Text),
                });
                projection.push(b);
            }
        }
    }
    let order = select
        .order
        .as_ref()
        .map(|(e, d, n)| expression::bind(e, &scope, params).map(|b| (b, *d, *n)))
        .transpose()?;
    if count && order.is_some() {
        return Err(error(
            "0A000",
            "ORDER BY with COUNT is outside the first dialect",
        ));
    }
    let limit = if let Some(expr) = &select.limit {
        let b = expression::assign(expr, &[], params, DataType::BigInt)?;
        match b.eval(&[])? {
            Value::BigInt(n) if n >= 0 => Some(n as u64),
            Value::Null => None,
            _ => {
                return Err(error(
                    "2201W",
                    "LIMIT must be a nonnegative integer or NULL",
                ));
            }
        }
    } else {
        None
    };
    if describe_only {
        return Ok(QueryResult {
            columns: if explain {
                vec![ResultColumn {
                    name: "QUERY PLAN".into(),
                    data_type: DataType::Text,
                }]
            } else {
                columns
            },
            rows: Vec::new(),
            affected: 0,
            tag: String::new(),
        });
    }
    let access = left
        .as_ref()
        .map(|t| choose(t, predicate.as_ref()))
        .unwrap_or(Access::Scan);
    if explain {
        let plan = if right.is_some() {
            format!("{}; NestedLoop InnerJoin", access_name(&access))
        } else {
            access_name(&access)
        };
        return Ok(QueryResult {
            columns: vec![ResultColumn {
                name: "QUERY PLAN".into(),
                data_type: DataType::Text,
            }],
            rows: vec![vec![Value::Text(plan)]],
            affected: 1,
            tag: "EXPLAIN".into(),
        });
    }
    let mut entries: Vec<(Vec<Value>, Value)> = Vec::new();
    let mut total = 0u64;
    let mut memory = 0usize;
    let mut emit = |s: &mut dyn Store, row: &[Value]| -> Result<()> {
        s.check()?;
        if !predicate
            .as_ref()
            .map(|p| p.matches(row))
            .transpose()?
            .unwrap_or(true)
        {
            return Ok(());
        }
        total = total
            .checked_add(1)
            .ok_or_else(|| error("22003", "COUNT overflow"))?;
        if count || (order.is_none() && limit.is_some_and(|n| entries.len() as u64 >= n)) {
            return Ok(());
        }
        let projected = projection
            .iter()
            .map(|p| p.eval(row))
            .collect::<Result<Vec<_>>>()?;
        let sort = order
            .as_ref()
            .map(|(p, _, _)| p.eval(row))
            .transpose()?
            .unwrap_or(Value::Null);
        memory += projected.iter().map(expression::value_size).sum::<usize>()
            + expression::value_size(&sort)
            + std::mem::size_of::<Vec<Value>>();
        // Account for vector capacity growth by reserving at most half budget.
        if memory > MAX_RESULT_BYTES.min(MAX_WORK_BYTES) / 2 {
            return Err(error("54000", "query result/sort memory limit exceeded"));
        }
        entries.push((projected, sort));
        Ok(())
    };
    let mut outer = |s: &mut dyn Store, row: Vec<Value>| -> Result<()> {
        if let Some(table) = &right {
            scan(s, table, &Access::Scan, |s, right_row| {
                let mut joined = row.clone();
                joined.extend(right_row);
                if join.as_ref().expect("join").matches(&joined)? {
                    emit(s, &joined)?;
                }
                Ok(())
            })
        } else {
            emit(s, &row)
        }
    };
    if let Some(table) = &left {
        scan(store, table, &access, &mut outer)?;
    } else {
        outer(store, Vec::new())?;
    }
    if count {
        let n = i64::try_from(total).map_err(|_| error("22003", "COUNT overflow"))?;
        if limit != Some(0) {
            entries.push((vec![Value::BigInt(n)], Value::Null));
        }
    }
    if let Some((_, desc, nulls_first)) = &order {
        let mut sort_error = None;
        entries.sort_by(|(_, a), (_, b)| match (a, b) {
            (Value::Null, Value::Null) => std::cmp::Ordering::Equal,
            (Value::Null, _) => {
                if *nulls_first {
                    std::cmp::Ordering::Less
                } else {
                    std::cmp::Ordering::Greater
                }
            }
            (_, Value::Null) => {
                if *nulls_first {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Less
                }
            }
            _ => match expression::compare(a, b) {
                Ok(c) => {
                    if *desc {
                        c.reverse()
                    } else {
                        c
                    }
                }
                Err(e) => {
                    sort_error = Some(e);
                    std::cmp::Ordering::Equal
                }
            },
        });
        if let Some(e) = sort_error {
            return Err(e);
        }
        store.check()?;
    }
    if let Some(n) = limit {
        entries.truncate(usize::try_from(n).unwrap_or(usize::MAX));
    }
    let rows = entries.into_iter().map(|(row, _)| row).collect::<Vec<_>>();
    Ok(QueryResult {
        affected: rows.len() as u64,
        columns,
        rows,
        tag: "SELECT".into(),
    })
}
