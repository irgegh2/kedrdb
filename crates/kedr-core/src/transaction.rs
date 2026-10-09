// SPDX-License-Identifier: Apache-2.0
//! Bounded local sessions. Each session worker owns its transaction on its stack;
//! no borrowed transaction is stored in a self-referential struct.
mod lock;
use crate::{
    Error, Result,
    database::FileDatabase,
    engine::{ReadView, StorageTransaction},
    sql::{QueryResult, Store},
    storage::FileStorage,
    tree::KeyValue,
};
use lock::Gate;
pub use lock::LockMetrics;
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Mode {
    ReadOnly,
    ReadWrite,
}
#[derive(Clone, Copy)]
pub struct SessionLimits {
    pub max_sessions: usize,
    pub lock_wait: Duration,
    pub transaction_lifetime: Duration,
    pub idle_in_transaction: Duration,
}
impl Default for SessionLimits {
    fn default() -> Self {
        Self {
            max_sessions: 32,
            lock_wait: Duration::from_secs(5),
            transaction_lifetime: Duration::from_secs(30),
            idle_in_transaction: Duration::from_secs(5),
        }
    }
}
struct Inner {
    database: Mutex<FileDatabase>,
    gate: Arc<Gate>,
    limits: SessionLimits,
    sessions: AtomicUsize,
}
#[derive(Clone)]
pub struct SharedDatabase(Arc<Inner>);
impl SharedDatabase {
    pub fn new(database: FileDatabase, limits: SessionLimits) -> Result<Self> {
        if limits.max_sessions == 0
            || limits.max_sessions > 64
            || [
                limits.lock_wait,
                limits.transaction_lifetime,
                limits.idle_in_transaction,
            ]
            .iter()
            .any(|duration| duration.is_zero() || *duration > Duration::from_secs(3600))
        {
            return Err(Error::InvalidArgument {
                message: "session limit must be 1..64 and timeouts in (0, 1 hour]",
            });
        }
        Ok(Self(Arc::new(Inner {
            database: Mutex::new(database),
            gate: Arc::default(),
            limits,
            sessions: AtomicUsize::new(0),
        })))
    }
    pub fn connect(&self) -> Result<Session> {
        self.0
            .sessions
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < self.0.limits.max_sessions).then_some(count + 1)
            })
            .map_err(|count| Error::LimitExceeded {
                limit: self.0.limits.max_sessions,
                requested: count as u64 + 1,
            })?;
        let (sender, receiver) = mpsc::sync_channel(1);
        let cancelled = Arc::new(AtomicBool::new(false));
        let inner = self.0.clone();
        let token = cancelled.clone();
        let worker = thread::Builder::new()
            .name("kedr-session".into())
            .spawn(move || {
                struct Admission(Arc<Inner>);
                impl Drop for Admission {
                    fn drop(&mut self) {
                        self.0.sessions.fetch_sub(1, Ordering::AcqRel);
                    }
                }
                let _admission = Admission(inner.clone());
                worker_loop(&inner, &receiver, &token);
            })
            .map_err(|source| {
                self.0.sessions.fetch_sub(1, Ordering::AcqRel);
                Error::io(crate::Operation::Open, source)
            })?;
        Ok(Session {
            sender: Some(sender),
            worker: Some(worker),
            cancelled,
            in_transaction: false,
        })
    }
    pub fn lock_metrics(&self) -> LockMetrics {
        self.0.gate.metrics()
    }
}
#[derive(Clone)]
pub struct CancelHandle(Arc<AtomicBool>);
impl CancelHandle {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }
}
pub struct Session {
    sender: Option<SyncSender<Request>>,
    worker: Option<JoinHandle<()>>,
    cancelled: Arc<AtomicBool>,
    in_transaction: bool,
}
impl Drop for Session {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
enum Command {
    Begin(Mode),
    Commit,
    Rollback,
    Get(Vec<u8>),
    Insert(Vec<u8>, Vec<u8>),
    Replace(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    Range(Option<Vec<u8>>, Option<Vec<u8>>, usize),
    Fail(Error),
    Sql(bool, SqlOperation),
}
type SqlOperation = Box<dyn FnOnce(&mut dyn Store) -> Result<QueryResult> + Send>;
enum Response {
    Done,
    Value(Option<Vec<u8>>),
    Deleted(bool),
    Rows(Vec<KeyValue>),
    Sql(QueryResult),
}
struct Request {
    command: Command,
    reply: SyncSender<Result<Response>>,
}
impl Session {
    pub(crate) fn execute_sql(
        &mut self,
        read_only: bool,
        operation: impl FnOnce(&mut dyn Store) -> Result<QueryResult> + Send + 'static,
    ) -> Result<QueryResult> {
        match self.request(Command::Sql(read_only, Box::new(operation)))? {
            Response::Sql(result) => Ok(result),
            _ => unreachable!("response contract"),
        }
    }
    fn validate_key(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() || key.len() > crate::record::MAX_KEY_SIZE {
            return self.fail(Error::InvalidArgument {
                message: "key length must be 1..512",
            });
        }
        Ok(())
    }
    fn validate_value(&mut self, value: &[u8]) -> Result<()> {
        if value.len() > crate::page::MAX_RECORD_SIZE {
            return self.fail(Error::RecordTooLarge {
                length: value.len(),
                limit: crate::page::MAX_RECORD_SIZE,
            });
        }
        Ok(())
    }
    pub fn cancel_handle(&self) -> CancelHandle {
        CancelHandle(self.cancelled.clone())
    }
    fn request(&mut self, command: Command) -> Result<Response> {
        if !self.in_transaction {
            self.cancelled.store(false, Ordering::Release);
        }
        let (reply, receiver) = mpsc::sync_channel(1);
        self.sender
            .as_ref()
            .ok_or(Error::Cancelled)?
            .send(Request { command, reply })
            .map_err(|_| Error::Cancelled)?;
        receiver.recv().map_err(|_| Error::Cancelled)?
    }
    pub fn begin(&mut self, mode: Mode) -> Result<()> {
        self.request(Command::Begin(mode))?;
        self.in_transaction = true;
        Ok(())
    }
    pub fn commit(&mut self) -> Result<()> {
        self.request(Command::Commit)?;
        self.in_transaction = false;
        Ok(())
    }
    pub fn rollback(&mut self) -> Result<()> {
        self.request(Command::Rollback)?;
        self.in_transaction = false;
        self.cancelled.store(false, Ordering::Release);
        Ok(())
    }
    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.validate_key(key)?;
        match self.request(Command::Get(key.to_vec()))? {
            Response::Value(value) => Ok(value),
            _ => unreachable!("response contract"),
        }
    }
    pub fn insert(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        self.validate_value(value)?;
        self.request(Command::Insert(key.to_vec(), value.to_vec()))?;
        Ok(())
    }
    pub fn replace(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.validate_key(key)?;
        self.validate_value(value)?;
        self.request(Command::Replace(key.to_vec(), value.to_vec()))?;
        Ok(())
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.validate_key(key)?;
        match self.request(Command::Delete(key.to_vec()))? {
            Response::Deleted(value) => Ok(value),
            _ => unreachable!("response contract"),
        }
    }
    pub fn range(
        &mut self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<KeyValue>> {
        if let Some(key) = start {
            self.validate_key(key)?;
        }
        if let Some(key) = end {
            self.validate_key(key)?;
        }
        match self.request(Command::Range(
            start.map(<[u8]>::to_vec),
            end.map(<[u8]>::to_vec),
            limit,
        ))? {
            Response::Rows(value) => Ok(value),
            _ => unreachable!("response contract"),
        }
    }
    /// Parser/type errors must abort an explicit transaction just like I/O errors.
    pub fn fail(&mut self, error: Error) -> Result<()> {
        self.request(Command::Fail(error))?;
        Ok(())
    }
}
fn reply(request: Request, result: Result<Response>) {
    let _ = request.reply.send(result);
}
fn read_command(view: &mut ReadView<FileStorage>, command: Command) -> Result<Response> {
    match command {
        Command::Sql(true, operation) => operation(view).map(Response::Sql),
        Command::Sql(false, _) => Err(Error::ReadOnlyTransaction),
        Command::Get(key) => view.get(&key).map(Response::Value),
        Command::Range(start, end, limit) => view
            .range(start.as_deref(), end.as_deref(), limit)
            .map(Response::Rows),
        Command::Insert(..) | Command::Replace(..) | Command::Delete(..) => {
            Err(Error::ReadOnlyTransaction)
        }
        Command::Fail(error) => Err(error),
        _ => Err(Error::InvalidTransactionState),
    }
}
fn write_command(
    tx: &mut StorageTransaction<'_, FileStorage, FileStorage>,
    command: Command,
) -> Result<Response> {
    match command {
        Command::Sql(_, operation) => operation(tx).map(Response::Sql),
        Command::Get(key) => tx.get(&key).map(Response::Value),
        Command::Range(start, end, limit) => tx
            .range(start.as_deref(), end.as_deref(), limit)
            .map(Response::Rows),
        Command::Insert(key, value) => tx.insert(&key, &value).map(|()| Response::Done),
        Command::Replace(key, value) => tx.replace(&key, &value).map(|()| Response::Done),
        Command::Delete(key) => tx.delete(&key).map(Response::Deleted),
        Command::Fail(error) => {
            tx.abort();
            Err(error)
        }
        _ => {
            tx.abort();
            Err(Error::InvalidTransactionState)
        }
    }
}
// Cancellation/lifetime checks also occur between commands. Idle transactions
// are rolled back by their worker even when the caller sends nothing more.
fn receive(
    receiver: &Receiver<Request>,
    cancelled: &AtomicBool,
    deadline: Instant,
    idle: Instant,
) -> Result<Option<Request>> {
    loop {
        if cancelled.load(Ordering::Acquire) {
            return Err(Error::Cancelled);
        }
        let until = deadline.min(idle);
        if Instant::now() >= until {
            return Err(Error::TransactionTimeout);
        }
        match receiver.recv_timeout(
            until
                .saturating_duration_since(Instant::now())
                .min(Duration::from_millis(10)),
        ) {
            Ok(request) => return Ok(Some(request)),
            Err(RecvTimeoutError::Disconnected) => return Ok(None),
            Err(RecvTimeoutError::Timeout) => {}
        }
    }
}
struct Finish {
    commit: bool,
    reply: Option<SyncSender<Result<Response>>>,
    error: Option<Error>,
    aborted: bool,
}
fn write_loop(
    tx: &mut StorageTransaction<'_, FileStorage, FileStorage>,
    receiver: &Receiver<Request>,
    cancelled: &AtomicBool,
    deadline: Instant,
    idle_timeout: Duration,
) -> Finish {
    let mut idle = Instant::now() + idle_timeout;
    let mut aborted = false;
    loop {
        let request = match receive(receiver, cancelled, deadline, idle) {
            Ok(Some(request)) => request,
            Ok(None) => {
                return Finish {
                    commit: false,
                    reply: None,
                    error: None,
                    aborted: false,
                };
            }
            Err(error) => {
                return Finish {
                    commit: false,
                    reply: None,
                    error: Some(error),
                    aborted: true,
                };
            }
        };
        match request.command {
            Command::Rollback => {
                return Finish {
                    commit: false,
                    reply: Some(request.reply),
                    error: None,
                    aborted: false,
                };
            }
            Command::Commit => {
                return Finish {
                    commit: !aborted,
                    reply: Some(request.reply),
                    error: aborted.then_some(Error::TransactionAborted),
                    aborted,
                };
            }
            command => {
                let result = if aborted {
                    Err(Error::TransactionAborted)
                } else {
                    write_command(tx, command)
                };
                if result.is_err() {
                    aborted = true;
                    tx.abort();
                }
                let interrupted =
                    matches!(&result, Err(Error::Cancelled | Error::TransactionTimeout));
                let _ = request.reply.send(result);
                if interrupted {
                    return Finish {
                        commit: false,
                        reply: None,
                        error: None,
                        aborted: true,
                    };
                }
                idle = Instant::now() + idle_timeout;
            }
        }
    }
}
fn explicit_write(
    inner: &Inner,
    receiver: &Receiver<Request>,
    cancelled: &Arc<AtomicBool>,
    begin: Request,
    permit: lock::Permit,
) -> (bool, Option<Error>) {
    let mut database = inner.database.lock().unwrap();
    let deadline = Instant::now() + inner.limits.transaction_lifetime;
    let mut finish = None;
    let mut begin_reply = Some(begin.reply);
    let result = database.transaction(|tx| {
        tx.set_interrupt(cancelled.clone(), deadline);
        if begin_reply
            .take()
            .expect("BEGIN reply")
            .send(Ok(Response::Done))
            .is_err()
        {
            return Err(Error::Cancelled);
        }
        let outcome = write_loop(
            tx,
            receiver,
            cancelled,
            deadline,
            inner.limits.idle_in_transaction,
        );
        let commit = outcome.commit;
        finish = Some(outcome);
        if commit {
            Ok(())
        } else {
            Err(Error::TransactionAborted)
        }
    });
    drop(database);
    drop(permit);
    if let Some(sender) = begin_reply {
        let _ = sender.send(result.map(|()| Response::Done));
        return (false, None);
    }
    let Some(mut finish) = finish else {
        return (false, None);
    };
    let error = finish.error.take();
    let failed_commit = finish.commit && result.is_err();
    if let Some(sender) = finish.reply {
        let response = if let Some(error) = error {
            Err(error)
        } else if finish.commit {
            result.map(|()| Response::Done)
        } else {
            Ok(Response::Done)
        };
        let _ = sender.send(response);
        (finish.aborted || failed_commit, None)
    } else {
        (finish.aborted, error)
    }
}
fn explicit_read(
    inner: &Inner,
    receiver: &Receiver<Request>,
    cancelled: &Arc<AtomicBool>,
    begin: Request,
    permit: lock::Permit,
) -> (bool, Option<Error>) {
    let mut view = match open_read(inner) {
        Ok(view) => view,
        Err(error) => {
            drop(permit);
            reply(begin, Err(error));
            return (false, None);
        }
    };
    let deadline = Instant::now() + inner.limits.transaction_lifetime;
    view.set_interrupt(cancelled.clone(), deadline);
    if begin.reply.send(Ok(Response::Done)).is_err() {
        return (false, None);
    }
    let mut idle = Instant::now() + inner.limits.idle_in_transaction;
    let mut aborted = false;
    loop {
        let request = match receive(receiver, cancelled, deadline, idle) {
            Ok(Some(request)) => request,
            Ok(None) => return (false, None),
            Err(error) => return (true, Some(error)),
        };
        match request.command {
            Command::Rollback => {
                drop(view);
                drop(permit);
                let _ = request.reply.send(Ok(Response::Done));
                return (false, None);
            }
            Command::Commit => {
                drop(view);
                drop(permit);
                let _ = request.reply.send(if aborted {
                    Err(Error::TransactionAborted)
                } else {
                    Ok(Response::Done)
                });
                return (aborted, None);
            }
            command => {
                let result = if aborted {
                    Err(Error::TransactionAborted)
                } else {
                    read_command(&mut view, command)
                };
                aborted |= result.is_err();
                let interrupted =
                    matches!(&result, Err(Error::Cancelled | Error::TransactionTimeout));
                let _ = request.reply.send(result);
                if interrupted {
                    return (true, None);
                }
                idle = Instant::now() + inner.limits.idle_in_transaction;
            }
        }
    }
}
fn open_read(inner: &Inner) -> Result<ReadView<FileStorage>> {
    inner
        .database
        .lock()
        .map_err(|_| Error::RecoveryRequired)?
        .read_view()
}
fn worker_loop(inner: &Inner, receiver: &Receiver<Request>, cancelled: &Arc<AtomicBool>) {
    let mut aborted = false;
    let mut deferred = None;
    while let Ok(request) = receiver.recv() {
        if matches!(request.command, Command::Rollback) {
            aborted = false;
            deferred = None;
            reply(request, Ok(Response::Done));
            continue;
        }
        if aborted {
            reply(
                request,
                Err(deferred.take().unwrap_or(Error::TransactionAborted)),
            );
            continue;
        }
        if matches!(request.command, Command::Commit) {
            reply(request, Err(Error::InvalidTransactionState));
            continue;
        }
        if let Command::Fail(error) = request.command {
            let _ = request.reply.send(Err(error));
            continue;
        }
        let mode = match request.command {
            Command::Begin(mode) => mode,
            Command::Get(..) | Command::Range(..) => Mode::ReadOnly,
            Command::Sql(true, _) => Mode::ReadOnly,
            _ => Mode::ReadWrite,
        };
        let permit =
            match inner
                .gate
                .acquire(mode, Instant::now() + inner.limits.lock_wait, cancelled)
            {
                Ok(permit) => permit,
                Err(error) => {
                    reply(request, Err(error));
                    continue;
                }
            };
        if matches!(request.command, Command::Begin(_)) {
            (aborted, deferred) = match mode {
                Mode::ReadOnly => explicit_read(inner, receiver, cancelled, request, permit),
                Mode::ReadWrite => explicit_write(inner, receiver, cancelled, request, permit),
            };
        } else {
            let deadline = Instant::now() + inner.limits.transaction_lifetime;
            let Request {
                command,
                reply: sender,
            } = request;
            let result = match mode {
                Mode::ReadOnly => open_read(inner).and_then(|mut view| {
                    view.set_interrupt(cancelled.clone(), deadline);
                    read_command(&mut view, command)
                }),
                Mode::ReadWrite => inner.database.lock().unwrap().transaction(|tx| {
                    tx.set_interrupt(cancelled.clone(), deadline);
                    write_command(tx, command)
                }),
            };
            drop(permit);
            let _ = sender.send(result);
        }
    }
}
