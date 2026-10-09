// SPDX-License-Identifier: Apache-2.0
use super::{Store, error, parser::Column};
use crate::{
    Result,
    record::{Cursor, Value, decode_row, encode_row, primary_key, secondary_key},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    Int,
    BigInt,
    Text,
    Boolean,
}
impl DataType {
    pub(super) fn of(value: &Value) -> Option<Self> {
        match value {
            Value::Null => None,
            Value::Int(_) => Some(Self::Int),
            Value::BigInt(_) => Some(Self::BigInt),
            Value::Text(_) => Some(Self::Text),
            Value::Bool(_) => Some(Self::Boolean),
        }
    }
}
#[derive(Clone, Debug)]
pub(super) struct Index {
    pub id: u64,
    pub name: String,
    pub column: usize,
    pub unique: bool,
}
#[derive(Clone, Debug)]
pub(super) struct Table {
    pub id: u64,
    pub name: String,
    pub columns: Vec<Column>,
    pub primary: usize,
    pub indices: Vec<Index>,
}
const ROOT: &[u8] = &[1];
pub(super) fn table_key(name: &str) -> Vec<u8> {
    let mut key = vec![0x10];
    key.extend_from_slice(name.as_bytes());
    key
}
pub(super) fn row_prefix(table: &Table) -> Vec<u8> {
    let mut key = vec![0x20];
    key.extend_from_slice(&table.id.to_be_bytes());
    key
}
pub(super) fn row_key(table: &Table, primary: i64) -> Vec<u8> {
    let mut key = row_prefix(table);
    key.extend_from_slice(&primary_key(primary));
    key
}
pub(super) fn index_prefix(table: &Table, index: &Index) -> Vec<u8> {
    let mut key = vec![0x30];
    key.extend_from_slice(&table.id.to_be_bytes());
    key.extend_from_slice(&index.id.to_be_bytes());
    key
}
pub(super) fn index_key(table: &Table, index: &Index, row: &[Value]) -> Result<Vec<u8>> {
    let mut key = index_prefix(table, index);
    key.extend_from_slice(&secondary_key(&row[index.column], pk(table, row)?).map_err(
        |e| match e {
            crate::Error::InvalidRecord { .. } | crate::Error::RecordTooLarge { .. } => {
                error("54000", "secondary index value exceeds key limit")
            }
            other => other,
        },
    )?);
    if key.len() > crate::record::MAX_KEY_SIZE {
        return Err(error("54000", "secondary index key exceeds 512 bytes"));
    }
    Ok(key)
}
pub(super) fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.last_mut() {
        if *last < 255 {
            *last += 1;
            break;
        }
        end.pop();
    }
    end
}
pub(super) fn pk(table: &Table, row: &[Value]) -> Result<i64> {
    match row.get(table.primary) {
        Some(Value::BigInt(v)) => Ok(*v),
        _ => Err(error("23502", "primary key must be non-NULL BIGINT")),
    }
}
pub(super) fn coerce(value: Value, data_type: DataType) -> Result<Value> {
    match (value, data_type) {
        (Value::Null, _) => Ok(Value::Null),
        (Value::BigInt(n), DataType::Int) => i32::try_from(n)
            .map(Value::Int)
            .map_err(|_| error("22003", "INT overflow")),
        (Value::Int(n), DataType::BigInt) => Ok(Value::BigInt(i64::from(n))),
        (Value::Text(s), DataType::Text) if s.contains('\0') => {
            Err(error("22021", "TEXT cannot contain NUL"))
        }
        (value, data_type) if DataType::of(&value) == Some(data_type) => Ok(value),
        _ => Err(error("42804", "value has incompatible type")),
    }
}
pub(super) fn decode(table: &Table, bytes: &[u8]) -> Result<Vec<Value>> {
    let mut row = decode_row(bytes)?;
    if row.len() > table.columns.len() {
        return Err(error("XX001", "row wider than catalog"));
    }
    row.resize(table.columns.len(), Value::Null);
    for (value, column) in row.iter().zip(&table.columns) {
        if (matches!(value, Value::Null) && !column.nullable)
            || DataType::of(value).is_some_and(|t| t != column.data_type)
        {
            return Err(error("XX001", "row type differs from catalog"));
        }
    }
    Ok(row)
}
pub(super) fn load(store: &mut dyn Store, name: &str) -> Result<Table> {
    let bytes = store
        .get(&table_key(name))?
        .ok_or_else(|| error("42P01", format!("table {name} does not exist")))?;
    let table = deserialize(&bytes)?;
    if table.name != name {
        return Err(error("XX001", "catalog key/name mismatch"));
    }
    Ok(table)
}
pub(super) fn list(store: &mut dyn Store) -> Result<Vec<Table>> {
    let rows = store.range(Some(&[0x10]), Some(&[0x11]), 129)?;
    if rows.len() > 128 {
        return Err(error("54000", "catalog limited to 128 tables"));
    }
    rows.into_iter()
        .map(|(key, value)| {
            let table = deserialize(&value)?;
            if key != table_key(&table.name) {
                return Err(error("XX001", "catalog key/name mismatch"));
            }
            Ok(table)
        })
        .collect()
}
pub(super) fn save(store: &mut dyn Store, table: &Table) -> Result<()> {
    store.replace(&table_key(&table.name), &serialize(table)?)
}
pub(super) fn allocate_id(store: &mut dyn Store) -> Result<u64> {
    let next = if let Some(bytes) = store.get(ROOT)? {
        if bytes.len() != 16 || &bytes[..8] != b"KEDRSQL1" {
            return Err(error("XX001", "invalid SQL catalog root"));
        }
        u64::from_le_bytes(bytes[8..].try_into().expect("width"))
    } else {
        if !store.range(None, None, 1)?.is_empty() {
            return Err(error(
                "0A000",
                "raw key/value store cannot be converted into a SQL database",
            ));
        }
        1
    };
    let following = next
        .checked_add(1)
        .filter(|_| next != 0)
        .ok_or_else(|| error("54000", "table id exhausted"))?;
    let mut bytes = b"KEDRSQL1".to_vec();
    bytes.extend_from_slice(&following.to_le_bytes());
    store.replace(ROOT, &bytes)?;
    Ok(next)
}
fn put_string(bytes: &mut Vec<u8>, text: &str) -> Result<()> {
    if text.is_empty() || text.len() > 63 || text.contains('\0') {
        return Err(error("XX001", "invalid catalog identifier"));
    }
    bytes.push(text.len() as u8);
    bytes.extend_from_slice(text.as_bytes());
    Ok(())
}
fn get_string(cursor: &mut Cursor<'_>) -> Result<String> {
    let length = usize::from(cursor.byte()?);
    let s = std::str::from_utf8(cursor.take(length)?)
        .map_err(|_| error("XX001", "catalog UTF-8 invalid"))?;
    if length == 0 || length > 63 || s.contains('\0') {
        return Err(error("XX001", "catalog identifier invalid"));
    }
    Ok(s.into())
}
fn serialize(table: &Table) -> Result<Vec<u8>> {
    let mut bytes = b"KEDRSCH1".to_vec();
    bytes.extend_from_slice(&table.id.to_le_bytes());
    put_string(&mut bytes, &table.name)?;
    bytes.extend_from_slice(&(table.columns.len() as u16).to_le_bytes());
    bytes.extend_from_slice(&(table.primary as u16).to_le_bytes());
    for column in &table.columns {
        put_string(&mut bytes, &column.name)?;
        bytes.push(match column.data_type {
            DataType::Int => 1,
            DataType::BigInt => 2,
            DataType::Text => 3,
            DataType::Boolean => 4,
        });
        bytes.push(u8::from(column.nullable));
        bytes.push(u8::from(column.primary));
        bytes.push(u8::from(column.unique));
        if let Some(foreign) = &column.foreign {
            bytes.push(1);
            put_string(&mut bytes, &foreign.table)?;
            put_string(&mut bytes, &foreign.column)?;
        } else {
            bytes.push(0);
        }
    }
    bytes.extend_from_slice(&(table.indices.len() as u16).to_le_bytes());
    for index in &table.indices {
        bytes.extend_from_slice(&index.id.to_le_bytes());
        put_string(&mut bytes, &index.name)?;
        bytes.extend_from_slice(&(index.column as u16).to_le_bytes());
        bytes.push(u8::from(index.unique));
    }
    if bytes.len() > 8192 {
        return Err(error("54000", "catalog entry exceeds 8 KiB"));
    }
    Ok(bytes)
}
fn boolean(cursor: &mut Cursor<'_>) -> Result<bool> {
    match cursor.byte()? {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(error("XX001", "invalid catalog flag")),
    }
}
fn deserialize(bytes: &[u8]) -> Result<Table> {
    let mut c = Cursor::new(bytes);
    if bytes.len() > 8192 || c.take(8)? != b"KEDRSCH1" {
        return Err(error("XX001", "catalog format unsupported"));
    }
    let id = c.u64()?;
    let name = get_string(&mut c)?;
    let count = usize::from(c.u16()?);
    let primary = usize::from(c.u16()?);
    if id == 0 || count == 0 || count > 256 || primary >= count {
        return Err(error("XX001", "catalog bounds invalid"));
    }
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let name = get_string(&mut c)?;
        let data_type = match c.byte()? {
            1 => DataType::Int,
            2 => DataType::BigInt,
            3 => DataType::Text,
            4 => DataType::Boolean,
            _ => return Err(error("XX001", "unknown catalog type")),
        };
        let nullable = boolean(&mut c)?;
        let primary = boolean(&mut c)?;
        let unique = boolean(&mut c)?;
        let foreign = if boolean(&mut c)? {
            Some(super::parser::Foreign {
                table: get_string(&mut c)?,
                column: get_string(&mut c)?,
            })
        } else {
            None
        };
        if columns.iter().any(|col: &Column| col.name == name) {
            return Err(error("XX001", "duplicate catalog column"));
        }
        columns.push(Column {
            name,
            data_type,
            nullable,
            primary,
            unique,
            foreign,
        });
    }
    if columns[primary].data_type != DataType::BigInt
        || columns[primary].nullable
        || !columns[primary].primary
        || columns.iter().filter(|c| c.primary).count() != 1
    {
        return Err(error("XX001", "catalog primary key invalid"));
    }
    let count = usize::from(c.u16()?);
    if count > 256 {
        return Err(error("XX001", "too many catalog indices"));
    }
    let mut indices = Vec::with_capacity(count);
    for _ in 0..count {
        let id = c.u64()?;
        let name = get_string(&mut c)?;
        let column = usize::from(c.u16()?);
        let unique = boolean(&mut c)?;
        if id == 0
            || column >= columns.len()
            || indices.iter().any(|i: &Index| i.id == id || i.name == name)
        {
            return Err(error("XX001", "catalog index invalid"));
        }
        indices.push(Index {
            id,
            name,
            column,
            unique,
        });
    }
    if !c.done() {
        return Err(error("XX001", "trailing catalog bytes"));
    }
    Ok(Table {
        id,
        name,
        columns,
        primary,
        indices,
    })
}
pub(super) fn scan_keys(
    store: &mut dyn Store,
    prefix: &[u8],
    mut visit: impl FnMut(&mut dyn Store, Vec<u8>, Vec<u8>) -> Result<()>,
) -> Result<()> {
    let end = prefix_end(prefix);
    let mut start = prefix.to_vec();
    loop {
        let rows = store.range(Some(&start), Some(&end), 64)?;
        if rows.is_empty() {
            return Ok(());
        }
        start = rows.last().expect("nonempty").0.clone();
        start.push(0);
        for (key, value) in rows {
            store.check()?;
            visit(store, key, value)?;
        }
    }
}
pub(super) fn encoded(row: &[Value]) -> Result<Vec<u8>> {
    encode_row(row).map_err(|e| match e {
        crate::Error::InvalidRecord { .. } | crate::Error::RecordTooLarge { .. } => {
            error("54000", "encoded SQL row exceeds 8 KiB")
        }
        other => other,
    })
}
