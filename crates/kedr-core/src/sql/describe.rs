// SPDX-License-Identifier: Apache-2.0
use super::{
    DataType, Description, QueryResult, ResultColumn, Store, catalog, error, executor,
    expression::{self, Field},
    parser::{Expr, Op, Projection, Select, Statement},
};
use crate::{Result, record::Value};

fn dummy(kind: DataType) -> Value {
    match kind {
        DataType::Int => Value::Int(0),
        DataType::BigInt => Value::BigInt(0),
        DataType::Text => Value::Text(String::new()),
        DataType::Boolean => Value::Bool(false),
    }
}

struct Inference {
    types: Vec<Option<DataType>>,
    highest: usize,
}
impl Inference {
    fn expr(
        &mut self,
        expr: &Expr,
        fields: &[Field],
        expected: Option<DataType>,
    ) -> Result<Option<DataType>> {
        match expr {
            Expr::Parameter(index) => {
                self.highest = self.highest.max(index + 1);
                if self.types.len() <= *index {
                    self.types.resize(index + 1, None);
                }
                if self.types[*index].is_none() {
                    self.types[*index] = expected;
                }
                Ok(self.types[*index])
            }
            Expr::Literal(_) => Ok(expression::bind(expr, fields, &[])?.data_type),
            Expr::Name(_) => Ok(expression::bind(expr, fields, &[])?.data_type),
            Expr::IsNull(inner, _) => {
                self.expr(inner, fields, None)?;
                Ok(Some(DataType::Boolean))
            }
            Expr::Binary(left, op, right) => {
                let a = self.expr(
                    left,
                    fields,
                    if *op == Op::And {
                        Some(DataType::Boolean)
                    } else {
                        None
                    },
                )?;
                let b = self.expr(
                    right,
                    fields,
                    if *op == Op::And {
                        Some(DataType::Boolean)
                    } else {
                        a
                    },
                )?;
                let numeric = matches!(op, Op::Add | Op::Sub);
                let a = self.expr(left, fields, b.or(if numeric { expected } else { None }))?;
                self.expr(right, fields, a.or(if numeric { expected } else { None }))?;
                Ok(if numeric {
                    if a == Some(DataType::BigInt) || b == Some(DataType::BigInt) {
                        Some(DataType::BigInt)
                    } else {
                        a.or(b)
                    }
                } else {
                    Some(DataType::Boolean)
                })
            }
        }
    }
    fn select(&mut self, store: &mut dyn Store, select: &Select) -> Result<()> {
        let mut fields = Vec::new();
        if let Some(source) = &select.source {
            fields.extend(executor::fields(
                &catalog::load(store, &source.table)?,
                &source.alias,
            ));
        }
        if let Some((source, expr)) = &select.join {
            fields.extend(executor::fields(
                &catalog::load(store, &source.table)?,
                &source.alias,
            ));
            self.expr(expr, &fields, Some(DataType::Boolean))?;
        }
        if let Some(expr) = &select.filter {
            self.expr(expr, &fields, Some(DataType::Boolean))?;
        }
        if let Some((expr, _, _)) = &select.order {
            self.expr(expr, &fields, None)?;
        }
        if let Some(expr) = &select.limit {
            self.expr(expr, &[], Some(DataType::BigInt))?;
        }
        for item in &select.projection {
            if let Projection::Expression(expr, _) = item {
                self.expr(expr, &fields, None)?;
            }
        }
        Ok(())
    }
}

pub(super) fn describe(
    store: &mut dyn Store,
    statement: &Statement,
    types: Vec<Option<DataType>>,
) -> Result<Description> {
    let mut inference = Inference {
        highest: types.len(),
        types,
    };
    match statement {
        Statement::Select(select) | Statement::Explain(select) => {
            inference.select(store, select)?
        }
        Statement::Insert {
            table,
            columns,
            values,
        } => {
            let table = catalog::load(store, table)?;
            for row in values {
                if row.len() != columns.len() {
                    return Err(error("42601", "INSERT column/value count mismatch"));
                }
                for (name, expr) in columns.iter().zip(row) {
                    let kind = table
                        .columns
                        .iter()
                        .find(|column| column.name == *name)
                        .ok_or_else(|| error("42703", "unknown INSERT column"))?
                        .data_type;
                    inference.expr(expr, &[], Some(kind))?;
                }
            }
        }
        Statement::Update {
            table: name,
            assignments,
            filter,
        } => {
            let table = catalog::load(store, name)?;
            let fields = executor::fields(&table, name);
            for (name, expr) in assignments {
                let kind = table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or_else(|| error("42703", "unknown UPDATE column"))?
                    .data_type;
                inference.expr(expr, &fields, Some(kind))?;
            }
            if let Some(expr) = filter {
                inference.expr(expr, &fields, Some(DataType::Boolean))?;
            }
        }
        Statement::Delete {
            table: name,
            filter,
        } => {
            let table = catalog::load(store, name)?;
            if let Some(expr) = filter {
                inference.expr(
                    expr,
                    &executor::fields(&table, name),
                    Some(DataType::Boolean),
                )?;
            }
        }
        _ => {}
    }
    // A second pass propagates constraints learned in later projections/assignments.
    if let Statement::Select(select) | Statement::Explain(select) = statement {
        inference.select(store, select)?;
    }
    let parameters = inference
        .types
        .into_iter()
        .take(inference.highest)
        .map(|kind| kind.unwrap_or(DataType::Text))
        .collect::<Vec<_>>();
    let params = parameters.iter().copied().map(dummy).collect::<Vec<_>>();
    // Validate write expressions at Parse/Describe, without evaluating rows or changing data.
    match statement {
        Statement::Insert {
            table,
            columns,
            values,
        } => {
            let table = catalog::load(store, table)?;
            let mut seen = std::collections::HashSet::new();
            for name in columns {
                if !seen.insert(name) {
                    return Err(error("42701", "duplicate INSERT column"));
                }
            }
            for row in values {
                for (name, expr) in columns.iter().zip(row) {
                    let kind = table
                        .columns
                        .iter()
                        .find(|column| column.name == *name)
                        .ok_or_else(|| error("42703", "unknown INSERT column"))?
                        .data_type;
                    expression::assign(expr, &[], &params, kind)?;
                }
            }
        }
        Statement::Update {
            table: name,
            assignments,
            filter,
        } => {
            let table = catalog::load(store, name)?;
            let fields = executor::fields(&table, name);
            let mut seen = std::collections::HashSet::new();
            for (name, expr) in assignments {
                if !seen.insert(name) {
                    return Err(error("42701", "duplicate UPDATE column"));
                }
                let kind = table
                    .columns
                    .iter()
                    .find(|column| column.name == *name)
                    .ok_or_else(|| error("42703", "unknown UPDATE column"))?
                    .data_type;
                expression::assign(expr, &fields, &params, kind)?;
            }
            expression::boolean(filter.as_ref(), &fields, &params)?;
        }
        Statement::Delete {
            table: name,
            filter,
        } => {
            let table = catalog::load(store, name)?;
            expression::boolean(filter.as_ref(), &executor::fields(&table, name), &params)?;
        }
        _ => {}
    }
    let columns = match statement {
        Statement::Select(select) => {
            executor::select_rows(store, select, &params, false, true)?.columns
        }
        Statement::Explain(select) => {
            executor::select_rows(store, select, &params, true, true)?.columns
        }
        Statement::Catalog(target) => catalog_result(store, target.as_ref())?.columns,
        _ => Vec::new(),
    };
    Ok(Description {
        parameters,
        columns,
    })
}

pub(super) fn catalog_result(
    store: &mut dyn Store,
    target: Option<&(String, bool)>,
) -> Result<QueryResult> {
    let col = |name: &str, data_type| ResultColumn {
        name: name.into(),
        data_type,
    };
    let (columns, rows) = if let Some((name, indices)) = target {
        let table = catalog::load(store, name)?;
        if *indices {
            (
                vec![
                    col("name", DataType::Text),
                    col("column", DataType::Text),
                    col("unique", DataType::Boolean),
                ],
                table
                    .indices
                    .iter()
                    .map(|index| {
                        vec![
                            Value::Text(index.name.clone()),
                            Value::Text(table.columns[index.column].name.clone()),
                            Value::Bool(index.unique),
                        ]
                    })
                    .collect::<Vec<_>>(),
            )
        } else {
            (
                vec![
                    col("name", DataType::Text),
                    col("type", DataType::Text),
                    col("nullable", DataType::Boolean),
                    col("primary_key", DataType::Boolean),
                    col("unique", DataType::Boolean),
                    col("reference", DataType::Text),
                ],
                table
                    .columns
                    .iter()
                    .map(|column| {
                        vec![
                            Value::Text(column.name.clone()),
                            Value::Text(
                                match column.data_type {
                                    DataType::Int => "INT",
                                    DataType::BigInt => "BIGINT",
                                    DataType::Text => "TEXT",
                                    DataType::Boolean => "BOOLEAN",
                                }
                                .into(),
                            ),
                            Value::Bool(column.nullable),
                            Value::Bool(column.primary),
                            Value::Bool(column.unique),
                            column
                                .foreign
                                .as_ref()
                                .map(|reference| {
                                    Value::Text(format!("{}.{}", reference.table, reference.column))
                                })
                                .unwrap_or(Value::Null),
                        ]
                    })
                    .collect(),
            )
        }
    } else {
        (
            vec![col("name", DataType::Text)],
            catalog::list(store)?
                .into_iter()
                .map(|table| vec![Value::Text(table.name)])
                .collect(),
        )
    };
    Ok(QueryResult {
        affected: rows.len() as u64,
        columns,
        rows,
        tag: "SHOW".into(),
    })
}
