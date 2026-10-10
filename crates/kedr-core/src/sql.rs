// SPDX-License-Identifier: Apache-2.0
//! Bounded relational SQL over the same private-page transaction machinery.
mod catalog;
mod describe;
pub mod dump;
mod executor;
mod expression;
mod keywords;
mod parser;

use crate::{
    Error, Result,
    engine::{ReadView, StorageTransaction},
    record::Value,
    storage::StorageIo,
    transaction::{Mode, Session},
    tree::KeyValue,
};
pub use catalog::DataType;

pub const MAX_SQL_BYTES: usize = 65_536;
pub const MAX_RESULT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_WORK_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResultColumn {
    pub name: String,
    pub data_type: DataType,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryResult {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Value>>,
    pub affected: u64,
    pub tag: String,
}
#[derive(Debug, Clone)]
pub struct Description {
    pub parameters: Vec<DataType>,
    pub columns: Vec<ResultColumn>,
}
impl QueryResult {
    pub(crate) fn command(tag: &str, affected: u64) -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            affected,
            tag: tag.into(),
        }
    }
}
pub struct SqlSession {
    session: Session,
}
impl SqlSession {
    /// One stable read snapshot; streaming avoids materializing the database.
    pub fn export_sql(
        &mut self,
        mut writer: impl std::io::Write + Send + 'static,
    ) -> Result<QueryResult> {
        self.session
            .execute_sql(true, move |store| dump::export(store, &mut writer))
    }
    /// Maintenance only: caller must quarantine the target until finish_import succeeds.
    pub fn import_statement(
        &mut self,
        sql: &str,
        job: [u8; 32],
        ordinal: u64,
    ) -> Result<QueryResult> {
        let sql = sql.to_owned();
        self.session
            .execute_sql(false, move |store| dump::import(store, &sql, &job, ordinal))
    }
    pub fn finish_import(&mut self, job: [u8; 32], total: u64) -> Result<QueryResult> {
        self.session
            .execute_sql(false, move |store| dump::finish(store, &job, total))
    }
    pub fn new(session: Session) -> Self {
        Self { session }
    }
    pub fn cancel_handle(&self) -> crate::transaction::CancelHandle {
        self.session.cancel_handle()
    }
    /// Bind names/types without executing writes or scanning table rows.
    pub fn describe(&mut self, sql: &str, types: &[Option<DataType>]) -> Result<Description> {
        let statement = match parser::parse(sql) {
            Ok(statement) => statement,
            Err(error) => {
                self.session.fail(error)?;
                unreachable!()
            }
        };
        if types.len() > 256 {
            self.session.fail(error("54000", "parameter count limit"))?;
        }
        // Transaction control must remain preparable after an error, so clients
        // using Extended Query can still ROLLBACK (or COMMIT an aborted block).
        if matches!(
            statement,
            parser::Statement::Begin(_) | parser::Statement::Commit | parser::Statement::Rollback
        ) {
            return Ok(Description {
                parameters: types
                    .iter()
                    .map(|kind| kind.unwrap_or(DataType::Text))
                    .collect(),
                columns: Vec::new(),
            });
        }
        let types = types.to_vec();
        self.session
            .describe_sql(move |store| describe::describe(store, &statement, types))
    }
    pub fn fail(&mut self, error: Error) -> Result<()> {
        self.session.fail(error)
    }
    /// Exactly one statement; values are supplied separately from SQL text.
    pub fn execute(&mut self, sql: &str, parameters: &[Value]) -> Result<QueryResult> {
        let statement = match parser::parse(sql) {
            Ok(statement) => statement,
            Err(error) => {
                self.session.fail(error)?;
                unreachable!()
            }
        };
        if parameters.len() > 256
            || parameters.iter().map(expression::value_size).sum::<usize>() > MAX_WORK_BYTES
        {
            self.session
                .fail(error("54000", "parameter memory limit exceeded"))?;
        }
        match statement {
            parser::Statement::Begin(read_only) => {
                self.session.begin(if read_only {
                    Mode::ReadOnly
                } else {
                    Mode::ReadWrite
                })?;
                Ok(QueryResult::command("BEGIN", 0))
            }
            parser::Statement::Commit => {
                self.session.commit()?;
                Ok(QueryResult::command("COMMIT", 0))
            }
            parser::Statement::Rollback => {
                self.session.rollback()?;
                Ok(QueryResult::command("ROLLBACK", 0))
            }
            statement => {
                let params = parameters.to_vec();
                self.session
                    .execute_sql(statement.read_only(), move |store| {
                        executor::execute(store, statement, &params)
                    })
            }
        }
    }
}
pub(crate) fn error(state: &'static str, message: impl Into<String>) -> Error {
    Error::Sql {
        state,
        message: message.into(),
    }
}
pub(crate) trait Store {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn replace(&mut self, key: &[u8], value: &[u8]) -> Result<()>;
    fn delete(&mut self, key: &[u8]) -> Result<bool>;
    fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<KeyValue>>;
    fn check(&mut self) -> Result<()>;
}
impl<D: StorageIo, W: StorageIo> Store for StorageTransaction<'_, D, W> {
    fn check(&mut self) -> Result<()> {
        self.check_interrupt()
    }
    fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(k)
    }
    fn insert(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.insert(k, v)
    }
    fn replace(&mut self, k: &[u8], v: &[u8]) -> Result<()> {
        self.replace(k, v)
    }
    fn delete(&mut self, k: &[u8]) -> Result<bool> {
        self.delete(k)
    }
    fn range(&mut self, a: Option<&[u8]>, b: Option<&[u8]>, n: usize) -> Result<Vec<KeyValue>> {
        self.range(a, b, n)
    }
}
impl<I: StorageIo> Store for ReadView<I> {
    fn check(&mut self) -> Result<()> {
        self.check_interrupt()
    }
    fn get(&mut self, k: &[u8]) -> Result<Option<Vec<u8>>> {
        self.get(k)
    }
    fn insert(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        Err(Error::ReadOnlyTransaction)
    }
    fn replace(&mut self, _: &[u8], _: &[u8]) -> Result<()> {
        Err(Error::ReadOnlyTransaction)
    }
    fn delete(&mut self, _: &[u8]) -> Result<bool> {
        Err(Error::ReadOnlyTransaction)
    }
    fn range(&mut self, a: Option<&[u8]>, b: Option<&[u8]>, n: usize) -> Result<Vec<KeyValue>> {
        self.range(a, b, n)
    }
}
