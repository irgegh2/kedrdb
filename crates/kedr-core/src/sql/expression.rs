// SPDX-License-Identifier: Apache-2.0
use super::{
    DataType,
    catalog::coerce,
    error,
    parser::{Expr, Op},
};
use crate::{Result, record::Value};
use std::cmp::Ordering;

#[derive(Clone)]
pub(super) struct Field {
    pub qualifier: String,
    pub name: String,
    pub data_type: DataType,
}
#[derive(Clone)]
pub(super) struct Bound {
    pub kind: Kind,
    pub data_type: Option<DataType>,
}
#[derive(Clone)]
pub(super) enum Kind {
    Literal(Value),
    Column(usize),
    Binary(Box<Bound>, Op, Box<Bound>),
    IsNull(Box<Bound>, bool),
}
pub(super) fn value_size(value: &Value) -> usize {
    std::mem::size_of::<Value>()
        + match value {
            Value::Text(s) => s.len(),
            _ => 0,
        }
}
fn numeric(t: Option<DataType>) -> bool {
    matches!(t, Some(DataType::Int | DataType::BigInt))
}
pub(super) fn bind(expr: &Expr, fields: &[Field], params: &[Value]) -> Result<Bound> {
    let bound = match expr {
        Expr::Literal(value) => {
            let value = if let Value::BigInt(n) = value {
                if let Ok(n) = i32::try_from(*n) {
                    Value::Int(n)
                } else {
                    value.clone()
                }
            } else {
                value.clone()
            };
            Bound {
                data_type: DataType::of(&value),
                kind: Kind::Literal(value),
            }
        }
        Expr::Parameter(n) => {
            let value = params
                .get(*n)
                .ok_or_else(|| error("42P02", "missing parameter"))?
                .clone();
            if matches!(&value,Value::Text(s) if s.contains('\0')) {
                return Err(error("22021", "TEXT parameter contains NUL"));
            }
            Bound {
                data_type: DataType::of(&value),
                kind: Kind::Literal(value),
            }
        }
        Expr::Name(names) => {
            let matches = fields
                .iter()
                .enumerate()
                .filter(|(_, f)| match names.as_slice() {
                    [name] => f.name == *name,
                    [qualifier, name] => f.name == *name && f.qualifier == *qualifier,
                    _ => false,
                })
                .map(|(i, _)| i)
                .collect::<Vec<_>>();
            if matches.len() > 1 {
                return Err(error("42702", "ambiguous column"));
            }
            let i = *matches
                .first()
                .ok_or_else(|| error("42703", format!("unknown column {}", names.join("."))))?;
            Bound {
                kind: Kind::Column(i),
                data_type: Some(fields[i].data_type),
            }
        }
        Expr::IsNull(inner, not) => Bound {
            kind: Kind::IsNull(Box::new(bind(inner, fields, params)?), *not),
            data_type: Some(DataType::Boolean),
        },
        Expr::Binary(a, op, b) => {
            let a = bind(a, fields, params)?;
            let b = bind(b, fields, params)?;
            let data_type = match op {
                Op::And => {
                    for t in [a.data_type, b.data_type] {
                        if t.is_some_and(|t| t != DataType::Boolean) {
                            return Err(error("42804", "AND requires BOOLEAN"));
                        }
                    }
                    Some(DataType::Boolean)
                }
                Op::Add | Op::Sub => {
                    if a.data_type.is_some() && !numeric(a.data_type)
                        || b.data_type.is_some() && !numeric(b.data_type)
                    {
                        return Err(error("42883", "arithmetic requires integers"));
                    }
                    if a.data_type == Some(DataType::BigInt)
                        || b.data_type == Some(DataType::BigInt)
                    {
                        Some(DataType::BigInt)
                    } else {
                        Some(DataType::Int)
                    }
                }
                _ => {
                    if a.data_type.is_some()
                        && b.data_type.is_some()
                        && a.data_type != b.data_type
                        && !(numeric(a.data_type) && numeric(b.data_type))
                    {
                        return Err(error("42883", "comparison has incompatible types"));
                    }
                    Some(DataType::Boolean)
                }
            };
            Bound {
                kind: Kind::Binary(Box::new(a), *op, Box::new(b)),
                data_type,
            }
        }
    };
    Ok(bound)
}
pub(super) fn assign(
    expr: &Expr,
    fields: &[Field],
    params: &[Value],
    target: DataType,
) -> Result<Bound> {
    let mut bound = bind(expr, fields, params)?;
    if let Kind::Literal(value) = bound.kind {
        bound.kind = Kind::Literal(coerce(value, target)?);
        bound.data_type = Some(target);
    } else if bound
        .data_type
        .is_some_and(|t| t != target && !(numeric(Some(t)) && numeric(Some(target))))
    {
        return Err(error("42804", "assignment has incompatible type"));
    }
    Ok(bound)
}
pub(super) fn boolean(
    expr: Option<&Expr>,
    fields: &[Field],
    params: &[Value],
) -> Result<Option<Bound>> {
    expr.map(|expr| {
        let b = bind(expr, fields, params)?;
        if b.data_type.is_some_and(|t| t != DataType::Boolean) {
            return Err(error("42804", "predicate must be BOOLEAN"));
        }
        Ok(b)
    })
    .transpose()
}
fn integer(value: &Value) -> Option<i64> {
    match value {
        Value::Int(n) => Some(i64::from(*n)),
        Value::BigInt(n) => Some(*n),
        _ => None,
    }
}
pub(super) fn compare(a: &Value, b: &Value) -> Result<Ordering> {
    match (a, b) {
        (Value::Text(a), Value::Text(b)) => Ok(a.as_bytes().cmp(b.as_bytes())),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        _ => integer(a)
            .zip(integer(b))
            .map(|(a, b)| a.cmp(&b))
            .ok_or_else(|| error("42883", "values cannot be compared")),
    }
}
impl Bound {
    pub fn eval(&self, row: &[Value]) -> Result<Value> {
        match &self.kind {
            Kind::Literal(v) => Ok(v.clone()),
            Kind::Column(i) => row
                .get(*i)
                .cloned()
                .ok_or_else(|| error("XX001", "bound row width invalid")),
            Kind::IsNull(inner, not) => {
                Ok(Value::Bool(matches!(inner.eval(row)?, Value::Null) != *not))
            }
            Kind::Binary(a, op, b) => {
                let a = a.eval(row)?;
                let b = b.eval(row)?;
                if *op == Op::And {
                    return Ok(match (a, b) {
                        (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
                        (Value::Null, _) | (_, Value::Null) => Value::Null,
                        (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                        _ => return Err(error("42804", "invalid AND values")),
                    });
                }
                if matches!(a, Value::Null) || matches!(b, Value::Null) {
                    return Ok(Value::Null);
                }
                if matches!(op, Op::Add | Op::Sub) {
                    let a = integer(&a).ok_or_else(|| error("42883", "integer required"))?;
                    let b = integer(&b).ok_or_else(|| error("42883", "integer required"))?;
                    let n = if *op == Op::Add {
                        a.checked_add(b)
                    } else {
                        a.checked_sub(b)
                    }
                    .ok_or_else(|| error("22003", "BIGINT arithmetic overflow"))?;
                    return coerce(Value::BigInt(n), self.data_type.expect("arithmetic type"));
                }
                let c = compare(&a, &b)?;
                Ok(Value::Bool(match op {
                    Op::Eq => c.is_eq(),
                    Op::Ne => !c.is_eq(),
                    Op::Lt => c.is_lt(),
                    Op::Le => !c.is_gt(),
                    Op::Gt => c.is_gt(),
                    Op::Ge => !c.is_lt(),
                    _ => unreachable!(),
                }))
            }
        }
    }
    pub fn matches(&self, row: &[Value]) -> Result<bool> {
        Ok(matches!(self.eval(row)?, Value::Bool(true)))
    }
}
