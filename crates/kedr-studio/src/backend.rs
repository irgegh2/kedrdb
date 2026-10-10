// SPDX-License-Identifier: Apache-2.0
//! UI transport boundary. The preview adapter never opens a database or socket.
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
    mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub const MAX_SQL_BYTES: usize = 64 * 1024;
pub const PRODUCT_SQL: &str =
    "SELECT id, name, stock, active\nFROM products\nORDER BY id LIMIT 100;";
pub const USER_SQL: &str = "SELECT id, name, email\nFROM users\nORDER BY id LIMIT 100;";

#[derive(Clone)]
pub struct Column {
    pub name: String,
    pub kind: String,
    pub detail: String,
}

#[derive(Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indices: Vec<String>,
}

#[derive(Clone)]
pub enum Cell {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

impl Cell {
    pub fn display(&self) -> String {
        match self {
            Self::Null => "NULL".to_owned(),
            Self::Integer(value) => value.to_string(),
            Self::Text(value) => value.clone(),
            Self::Boolean(value) => value.to_string(),
        }
    }
}

pub struct ResultSet {
    pub columns: Vec<(String, String)>,
    pub rows: Vec<Vec<Cell>>,
    pub affected: u64,
    pub truncated: bool,
}

#[derive(Clone)]
pub struct Profile {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: zeroize::Zeroizing<String>,
    pub ca_file: Option<String>,
}

pub enum Command {
    Connect(Profile),
    Catalog,
    Query(String),
}

pub enum Payload {
    Connected(Vec<Table>),
    Catalog(Vec<Table>),
    Rows(ResultSet),
}

pub struct Reply {
    pub id: u64,
    pub elapsed: Duration,
    pub result: Result<Payload, String>,
    pub status: String,
}
#[derive(Default)]
pub struct ConnectionHealth {
    pub generation: AtomicU64,
    pub connected: AtomicBool,
}

struct Job {
    id: u64,
    command: Command,
    cancel: Arc<AtomicBool>,
}

/// Future transports must honor cancellation/deadlines and return bounded data.
pub trait Backend: Send + 'static {
    fn perform(&mut self, command: Command, cancel: &AtomicBool) -> Result<Payload, String>;
    fn status(&self) -> String {
        "Тест интерфейса · сервер не подключён".into()
    }
    fn health(&self) -> Option<Arc<ConnectionHealth>> {
        None
    }
}

pub struct Preview;

impl Backend for Preview {
    fn perform(&mut self, command: Command, cancel: &AtomicBool) -> Result<Payload, String> {
        // A short cancellable delay makes the background state visible in the prototype.
        for _ in 0..30 {
            if cancel.load(Ordering::Acquire) {
                return Err("Операция отменена.".to_owned());
            }
            thread::sleep(Duration::from_millis(2));
        }
        match command {
            Command::Connect(_) => Err("Тестовый адаптер не подключается к серверу.".into()),
            Command::Catalog => Ok(Payload::Catalog(preview_catalog())),
            Command::Query(sql) => {
                if sql.len() > MAX_SQL_BYTES {
                    return Err("Запрос превышает лимит 64 КиБ.".to_owned());
                }
                // Exact canned examples only: this is intentionally not an SQL interpreter.
                let normalized = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
                if normalized(&sql) == normalized(PRODUCT_SQL) {
                    Ok(Payload::Rows(product_rows()))
                } else if normalized(&sql) == normalized(USER_SQL) {
                    Ok(Payload::Rows(user_rows()))
                } else {
                    Err("В предпросмотре доступны только запросы-примеры из дерева объектов. Произвольный SQL и запись появятся после подключения сервера.".to_owned())
                }
            }
        }
    }
}

pub struct Worker {
    health: Option<Arc<ConnectionHealth>>,
    commands: Option<SyncSender<Job>>,
    replies: Option<Receiver<Reply>>,
    active_cancel: Option<Arc<AtomicBool>>,
    thread: Option<JoinHandle<()>>,
    next_id: u64,
}

impl Worker {
    pub fn new(mut backend: impl Backend) -> Result<Self, String> {
        let health = backend.health();
        let (commands, jobs) = mpsc::sync_channel::<Job>(1);
        let (out, replies) = mpsc::sync_channel(1);
        let thread = thread::Builder::new()
            .name("kedr-studio-transport".to_owned())
            .spawn(move || {
                while let Ok(job) = jobs.recv() {
                    let start = Instant::now();
                    let result = backend.perform(job.command, &job.cancel);
                    if out
                        .send(Reply {
                            id: job.id,
                            elapsed: start.elapsed(),
                            result,
                            status: backend.status(),
                        })
                        .is_err()
                    {
                        break;
                    }
                }
            })
            .map_err(|error| format!("Не удалось запустить фоновые операции: {error}"))?;
        Ok(Self {
            health,
            commands: Some(commands),
            replies: Some(replies),
            active_cancel: None,
            thread: Some(thread),
            next_id: 1,
        })
    }
    pub fn connection_alive(&self) -> Option<bool> {
        self.health
            .as_ref()
            .map(|health| health.connected.load(Ordering::Acquire))
    }

    pub fn submit(&mut self, command: Command) -> Result<u64, String> {
        if let Command::Query(sql) = &command
            && sql.len() > MAX_SQL_BYTES
        {
            return Err("Запрос превышает лимит 64 КиБ.".to_owned());
        }
        // Exactly one outstanding job, including a reply waiting for the UI.
        if self.active_cancel.is_some() {
            return Err("Дождитесь завершения текущей операции.".to_owned());
        }
        let id = self.next_id;
        let next = id.checked_add(1).ok_or("Исчерпан счетчик операций.")?;
        let cancel = Arc::new(AtomicBool::new(false));
        match self
            .commands
            .as_ref()
            .ok_or("Фоновое соединение закрыто.")?
            .try_send(Job {
                id,
                command,
                cancel: Arc::clone(&cancel),
            }) {
            Ok(()) => {
                self.next_id = next;
                self.active_cancel = Some(cancel);
                Ok(id)
            }
            Err(TrySendError::Full(_)) => Err("Очередь операций занята.".to_owned()),
            Err(TrySendError::Disconnected(_)) => Err("Фоновое соединение закрыто.".to_owned()),
        }
    }

    pub fn poll(&mut self) -> Result<Option<Reply>, String> {
        match self
            .replies
            .as_ref()
            .ok_or("Фоновое соединение закрыто.")?
            .try_recv()
        {
            Ok(reply) => {
                self.active_cancel = None;
                Ok(Some(reply))
            }
            Err(TryRecvError::Empty) => Ok(None),
            Err(TryRecvError::Disconnected) => {
                self.active_cancel = None;
                Err("Фоновое соединение закрыто.".to_owned())
            }
        }
    }

    pub fn cancel(&self) {
        if let Some(cancel) = &self.active_cancel {
            cancel.store(true, Ordering::Release);
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.cancel();
        // Drop response receiver before joining, unblocking a full response queue.
        self.commands.take();
        self.replies.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

pub fn preview_catalog() -> Vec<Table> {
    let col = |name: &str, kind: &str, detail: &str| Column {
        name: name.to_owned(),
        kind: kind.to_owned(),
        detail: detail.to_owned(),
    };
    vec![
        Table {
            name: "products".to_owned(),
            columns: vec![
                col("id", "BIGINT", "PRIMARY KEY"),
                col("name", "TEXT", "NOT NULL"),
                col("stock", "INT", "NOT NULL"),
                col("active", "BOOLEAN", "NULL разрешен"),
            ],
            indices: vec!["products_pkey (id)".to_owned()],
        },
        Table {
            name: "users".to_owned(),
            columns: vec![
                col("id", "BIGINT", "PRIMARY KEY"),
                col("name", "TEXT", "NOT NULL"),
                col("email", "TEXT", "UNIQUE"),
            ],
            indices: vec![
                "users_pkey (id)".to_owned(),
                "users_email (email)".to_owned(),
            ],
        },
    ]
}

pub fn product_rows() -> ResultSet {
    let products = [
        "Кедровый блокнот",
        "Лампа для стола",
        "Кружка",
        "Подставка для ноутбука",
        "Кабель USB-C",
        "Рюкзак",
        "Набор карандашей",
        "Термос",
        "Ежедневник",
        "Сумка",
    ];
    ResultSet {
        affected: 10,
        truncated: false,
        columns: [
            ("id", "BIGINT"),
            ("name", "TEXT"),
            ("stock", "INT"),
            ("active", "BOOLEAN"),
        ]
        .into_iter()
        .map(|(name, kind)| (name.to_owned(), kind.to_owned()))
        .collect(),
        rows: products
            .into_iter()
            .enumerate()
            .map(|(i, name)| {
                vec![
                    Cell::Integer(i as i64 + 1),
                    Cell::Text(name.to_owned()),
                    Cell::Integer(10 + i as i64 * 3),
                    if i == 3 {
                        Cell::Null
                    } else {
                        Cell::Boolean(i != 5)
                    },
                ]
            })
            .collect(),
    }
}

fn user_rows() -> ResultSet {
    ResultSet {
        affected: 2,
        truncated: false,
        columns: [("id", "BIGINT"), ("name", "TEXT"), ("email", "TEXT")]
            .into_iter()
            .map(|(name, kind)| (name.to_owned(), kind.to_owned()))
            .collect(),
        rows: vec![
            vec![
                Cell::Integer(1),
                Cell::Text("Анна".to_owned()),
                Cell::Text("anna@example.test".to_owned()),
            ],
            vec![Cell::Integer(2), Cell::Text("Иван".to_owned()), Cell::Null],
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn receive(worker: &mut Worker) -> Reply {
        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if let Some(reply) = worker.poll().unwrap() {
                return reply;
            }
            assert!(Instant::now() < deadline, "worker did not respond");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn backpressure_and_cancel_allow_subsequent_operation() {
        let mut worker = Worker::new(Preview).unwrap();
        let id = worker
            .submit(Command::Query(PRODUCT_SQL.to_owned()))
            .unwrap();
        assert!(worker.submit(Command::Catalog).is_err());
        worker.cancel();
        let reply = receive(&mut worker);
        assert_eq!(reply.id, id);
        assert!(reply.result.is_err());
        worker.submit(Command::Catalog).unwrap();
        assert!(matches!(
            receive(&mut worker).result,
            Ok(Payload::Catalog(_))
        ));
    }

    #[test]
    fn preview_cannot_acknowledge_database_writes() {
        let mut worker = Worker::new(Preview).unwrap();
        worker
            .submit(Command::Query("DROP TABLE products;".to_owned()))
            .unwrap();
        assert!(receive(&mut worker).result.is_err());
        assert!(
            worker
                .submit(Command::Query("я".repeat(MAX_SQL_BYTES)))
                .is_err()
        );
        worker
            .submit(Command::Query(PRODUCT_SQL.to_owned()))
            .unwrap();
        assert!(matches!(receive(&mut worker).result, Ok(Payload::Rows(_))));
    }

    #[test]
    fn closing_with_an_unread_reply_does_not_deadlock() {
        let mut worker = Worker::new(Preview).unwrap();
        worker.submit(Command::Catalog).unwrap();
        thread::sleep(Duration::from_millis(100));
        let start = Instant::now();
        drop(worker);
        assert!(start.elapsed() < Duration::from_secs(1));
    }
}
