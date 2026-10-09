// SPDX-License-Identifier: Apache-2.0
//! The same real PostgreSQL driver used by integration tests. No file-level access.
use crate::backend::{
    Backend, Cell, Column, Command, ConnectionHealth, Payload, Profile, ResultSet, Table,
};
use futures_util::{StreamExt, pin_mut};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio_postgres::{Client, NoTls, types::Type};
use tokio_postgres_rustls::MakeRustlsConnect;

pub struct Network {
    runtime: Arc<tokio::runtime::Runtime>,
    client: Option<Client>,
    connection: Option<tokio::task::JoinHandle<()>>,
    tls: Option<MakeRustlsConnect>,
    label: String,
    status: String,
    health: Arc<ConnectionHealth>,
}
impl Network {
    pub fn new() -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map_err(|e| e.to_string())?;
        Ok(Self {
            runtime: Arc::new(runtime),
            client: None,
            connection: None,
            tls: None,
            label: String::new(),
            status: "Нет подключения".into(),
            health: Arc::default(),
        })
    }
    fn disconnect(&mut self) {
        self.health.generation.fetch_add(1, Ordering::AcqRel);
        self.health.connected.store(false, Ordering::Release);
        self.client.take();
        if let Some(connection) = self.connection.take() {
            connection.abort();
        }
        self.status = "Нет подключения".into();
    }
    async fn connect(&mut self, profile: Profile) -> Result<Payload, String> {
        self.disconnect();
        let health = self.health.clone();
        let generation = health.generation.load(Ordering::Acquire);
        if profile.host.is_empty()
            || profile.host.len() > 253
            || profile.user.len() > 63
            || profile.password.len() > 1024
        {
            return Err("Проверьте адрес, пользователя и длину пароля.".into());
        }
        let mut config = tokio_postgres::Config::new();
        config
            .host(&profile.host)
            .port(profile.port)
            .user(&profile.user)
            .password(profile.password.as_bytes())
            .dbname(&profile.database)
            .connect_timeout(Duration::from_secs(5));
        self.label = format!("{}:{} / {}", profile.host, profile.port, profile.database);
        if let Some(path) = profile.ca_file {
            let metadata = std::fs::metadata(&path).map_err(|e| format!("Сертификат CA: {e}"))?;
            if !metadata.is_file() || metadata.len() > 1024 * 1024 {
                return Err("Файл CA должен быть обычным файлом до 1 МиБ.".into());
            }
            let mut roots = rustls::RootCertStore::empty();
            let mut reader =
                std::io::BufReader::new(std::fs::File::open(&path).map_err(|e| e.to_string())?);
            let certs = rustls_pemfile::certs(&mut reader);
            for cert in certs {
                roots
                    .add(cert.map_err(|e| e.to_string())?)
                    .map_err(|e| e.to_string())?;
            }
            if roots.is_empty() {
                return Err("Файл CA не содержит сертификатов.".into());
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_root_certificates(roots)
            .with_no_client_auth();
            let tls = MakeRustlsConnect::new(tls);
            config.ssl_mode(tokio_postgres::config::SslMode::Require);
            let (client, connection) = config.connect(tls.clone()).await.map_err(error)?;
            self.health.connected.store(true, Ordering::Release);
            self.connection = Some(tokio::spawn(async move {
                let _ = connection.await;
                if health.generation.load(Ordering::Acquire) == generation {
                    health.connected.store(false, Ordering::Release);
                }
            }));
            self.client = Some(client);
            self.tls = Some(tls);
        } else {
            let loopback = profile
                .host
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
            if !loopback {
                return Err(
                    "Без TLS разрешен только числовой loopback-адрес: 127.0.0.1 или ::1.".into(),
                );
            }
            config.ssl_mode(tokio_postgres::config::SslMode::Disable);
            let (client, connection) = config.connect(NoTls).await.map_err(error)?;
            self.health.connected.store(true, Ordering::Release);
            self.connection = Some(tokio::spawn(async move {
                let _ = connection.await;
                if health.generation.load(Ordering::Acquire) == generation {
                    health.connected.store(false, Ordering::Release);
                }
            }));
            self.client = Some(client);
            self.tls = None;
        }
        // Own, versioned metadata endpoint: PostgreSQL pg_catalog is not impersonated.
        self.update_status().await?;
        let tables = self.catalog().await?;
        if self.client.as_ref().is_some_and(Client::is_closed) {
            return Err("Соединение закрыто во время подключения.".into());
        }
        Ok(Payload::Connected(tables))
    }
    async fn update_status(&mut self) -> Result<(), String> {
        let client = self.client.as_ref().ok_or("Нет подключения.")?;
        let rows = client
            .simple_query("SHOW KEDR STATUS")
            .await
            .map_err(error)?;
        let row = rows
            .iter()
            .find_map(|row| {
                if let tokio_postgres::SimpleQueryMessage::Row(row) = row {
                    Some(row)
                } else {
                    None
                }
            })
            .ok_or("Сервер не вернул сведения о совместимости.")?;
        if row.get(0) != Some("1") {
            return Err("Эта версия API KedrDB не поддерживается приложением.".into());
        }
        let status = match row.get(2) {
            Some("I") => "готов",
            Some("T") => "транзакция открыта",
            Some("E") => "нужен ROLLBACK",
            _ => return Err("Неизвестное состояние сервера.".into()),
        };
        self.status = format!(
            "{} · {} · {}",
            self.label,
            if self.tls.is_some() {
                "TLS"
            } else {
                "локально"
            },
            status
        );
        Ok(())
    }
    async fn catalog(&self) -> Result<Vec<Table>, String> {
        let client = self.client.as_ref().ok_or("Нет подключения.")?;
        let rows = client.query("SHOW TABLES", &[]).await.map_err(error)?;
        if rows.len() > 128 {
            return Err("Превышен лимит каталога: 128 таблиц.".into());
        }
        let mut tables = Vec::new();
        for row in rows {
            let name: String = row.try_get(0).map_err(error)?;
            let mut columns = Vec::new();
            let rows = client
                .query(&format!("SHOW COLUMNS FROM {}", identifier(&name)), &[])
                .await
                .map_err(error)?;
            if rows.len() > 256 {
                return Err("Превышен лимит столбцов.".into());
            }
            let mut indices = Vec::new();
            for row in rows {
                let name: String = row.try_get(0).map_err(error)?;
                let kind: String = row.try_get(1).map_err(error)?;
                let nullable: bool = row.try_get(2).map_err(error)?;
                let primary: bool = row.try_get(3).map_err(error)?;
                let unique: bool = row.try_get(4).map_err(error)?;
                let foreign: Option<String> = row.try_get(5).map_err(error)?;
                let mut detail = Vec::new();
                if primary {
                    detail.push("PRIMARY KEY".into());
                    indices.push(format!("PRIMARY KEY ({name})"));
                } else if unique {
                    detail.push("UNIQUE".into());
                }
                if !nullable && !primary {
                    detail.push("NOT NULL".into());
                }
                if let Some(reference) = foreign {
                    detail.push(format!("REFERENCES {reference}"));
                }
                columns.push(Column {
                    name,
                    kind,
                    detail: detail.join(" · "),
                });
            }
            for row in client
                .query(&format!("SHOW INDEXES FROM {}", identifier(&name)), &[])
                .await
                .map_err(error)?
            {
                let name: String = row.try_get(0).map_err(error)?;
                let column: String = row.try_get(1).map_err(error)?;
                let unique: bool = row.try_get(2).map_err(error)?;
                indices.push(format!(
                    "{} ({}){}",
                    name,
                    column,
                    if unique { " UNIQUE" } else { "" }
                ));
            }
            tables.push(Table {
                name,
                columns,
                indices,
            });
        }
        Ok(tables)
    }
    async fn query(&self, sql: String) -> Result<ResultSet, String> {
        let client = self.client.as_ref().ok_or("Нет подключения.")?;
        let statement = client.prepare(&sql).await.map_err(error)?;
        if !statement.params().is_empty() {
            return Err("В редакторе используйте значения в SQL. Параметры $1 доступны через драйвер приложения.".into());
        }
        let columns = statement
            .columns()
            .iter()
            .map(|col| (col.name().to_owned(), type_name(col.type_()).to_owned()))
            .collect::<Vec<_>>();
        let stream = client
            .query_raw(
                &statement,
                std::iter::empty::<&(dyn tokio_postgres::types::ToSql + Sync)>(),
            )
            .await
            .map_err(error)?;
        pin_mut!(stream);
        let mut rows = Vec::new();
        let mut bytes = 0;
        let mut truncated = false;
        while let Some(row) = stream.next().await {
            let row = row.map_err(error)?;
            if rows.len() >= 1000 || truncated {
                truncated = true;
                continue;
            }
            let mut cells = Vec::new();
            for (index, column) in row.columns().iter().enumerate() {
                let cell = match *column.type_() {
                    Type::INT8 => row
                        .try_get::<_, Option<i64>>(index)
                        .map_err(error)?
                        .map(Cell::Integer),
                    Type::INT4 => row
                        .try_get::<_, Option<i32>>(index)
                        .map_err(error)?
                        .map(|n| Cell::Integer(i64::from(n))),
                    Type::TEXT => row
                        .try_get::<_, Option<String>>(index)
                        .map_err(error)?
                        .map(Cell::Text),
                    Type::BOOL => row
                        .try_get::<_, Option<bool>>(index)
                        .map_err(error)?
                        .map(Cell::Boolean),
                    _ => return Err("Сервер вернул неподдерживаемый тип результата.".into()),
                }
                .unwrap_or(Cell::Null);
                bytes += match &cell {
                    Cell::Text(s) => s.len() + 32,
                    _ => 32,
                };
                cells.push(cell);
            }
            if bytes > 2 * 1024 * 1024 {
                truncated = true;
            } else {
                rows.push(cells);
            }
        }
        Ok(ResultSet {
            columns,
            rows,
            affected: stream.rows_affected().unwrap_or(0),
            truncated,
        })
    }
    async fn operation(&mut self, command: Command) -> Result<Payload, String> {
        let connecting = matches!(command, Command::Connect(_));
        let result = match command {
            Command::Connect(profile) => self.connect(profile).await,
            Command::Catalog => self.catalog().await.map(Payload::Catalog),
            Command::Query(sql) => self.query(sql).await.map(Payload::Rows),
        };
        if connecting && result.is_err() {
            self.disconnect();
            return result;
        }
        if self.client.as_ref().is_some_and(Client::is_closed) {
            self.disconnect();
        } else if self.client.is_some()
            && let Err(error) = self.update_status().await
        {
            self.disconnect();
            return Err(error);
        }
        result
    }
}
impl Backend for Network {
    fn perform(&mut self, command: Command, cancel: &AtomicBool) -> Result<Payload, String> {
        let connecting = matches!(command, Command::Connect(_));
        let runtime = self.runtime.clone();
        let token = if connecting {
            None
        } else {
            self.client.as_ref().map(Client::cancel_token)
        };
        let tls = self.tls.clone();
        let result = runtime.block_on(async {
            let operation = self.operation(command); pin_mut!(operation);
            let deadline = tokio::time::sleep(Duration::from_secs(if connecting { 8 } else { 30 })); pin_mut!(deadline);
            loop { tokio::select! {
                biased;
                result = &mut operation => return result,
                _ = &mut deadline => break,
                _ = tokio::time::sleep(Duration::from_millis(20)) => { if cancel.load(Ordering::Acquire) { break; } },
            } }
            if let Some(token) = token {
                let _ = tokio::time::timeout(Duration::from_secs(2), async { if let Some(tls) = tls { token.cancel_query(tls).await } else { token.cancel_query(NoTls).await } }).await;
                return match tokio::time::timeout(Duration::from_secs(6), &mut operation).await { Ok(result) => result, Err(_) => Err("Связь прервана после отмены. Результат записи мог остаться неопределенным; запрос не повторялся.".into()) };
            }
            Err("Подключение отменено или истекло время ожидания.".into())
        });
        if result.as_ref().is_err_and(|message| {
            message.starts_with("Связь прервана") || message.starts_with("Подключение отменено")
        }) {
            self.disconnect();
        }
        result
    }
    fn status(&self) -> String {
        self.status.clone()
    }
    fn health(&self) -> Option<Arc<ConnectionHealth>> {
        Some(self.health.clone())
    }
}
impl Drop for Network {
    fn drop(&mut self) {
        self.disconnect();
    }
}
pub fn identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
fn type_name(kind: &Type) -> &'static str {
    match *kind {
        Type::INT4 => "INT",
        Type::INT8 => "BIGINT",
        Type::TEXT => "TEXT",
        Type::BOOL => "BOOLEAN",
        _ => "?",
    }
}
fn error(error: tokio_postgres::Error) -> String {
    if let Some(db) = error.as_db_error() {
        format!("{}: {}", db.code().code(), db.message())
    } else {
        format!(
            "Соединение: {error}. При потере связи исход последней записи может быть неизвестен; автоматического повтора нет."
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{Reply, Worker};
    use kedr_core::{database::FileDatabase, page::PAGE_SIZE};
    use kedr_server::{Config, Server, credentials::Credential};
    use std::time::Instant;
    fn receive(worker: &mut Worker) -> Reply {
        let start = Instant::now();
        loop {
            if let Some(reply) = worker.poll().unwrap() {
                return reply;
            }
            assert!(start.elapsed() < Duration::from_secs(12));
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn query(worker: &mut Worker, sql: &str) -> Reply {
        worker.submit(Command::Query(sql.into())).unwrap();
        receive(worker)
    }
    fn profile(port: u16, password: &str) -> Profile {
        Profile {
            host: "127.0.0.1".into(),
            port,
            database: "kedrdb".into(),
            user: "admin".into(),
            password: zeroize::Zeroizing::new(password.into()),
            ca_file: None,
        }
    }
    #[test]
    fn real_worker_crud_catalog_abort_cancel_limits_disconnect_and_reconnect() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let root = std::env::temp_dir().join(format!(
            "kedr-studio-real-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&root).unwrap();
        drop(FileDatabase::create(root.join("data"), [61; 16], 4 * PAGE_SIZE).unwrap());
        Credential::new("admin", "test-password-123")
            .unwrap()
            .write_new(&root.join("auth"))
            .unwrap();
        let config = || Config {
            data: root.join("data"),
            auth: root.join("auth"),
            listen: "127.0.0.1:0".parse().unwrap(),
            tls_cert: None,
            tls_key: None,
        };
        let server = runtime.block_on(Server::bind(config())).unwrap();
        let port = server.local_addr().unwrap().port();
        let (shutdown, rx) = tokio::sync::watch::channel(false);
        let task = runtime.spawn(server.run(rx));
        let mut worker = Worker::new(Network::new().unwrap()).unwrap();
        worker
            .submit(Command::Connect(profile(port, "wrong-password")))
            .unwrap();
        assert!(receive(&mut worker).result.is_err());
        worker
            .submit(Command::Connect(profile(port, "test-password-123")))
            .unwrap();
        assert!(matches!(
            receive(&mut worker).result,
            Ok(Payload::Connected(_))
        ));
        assert!(
            query(
                &mut worker,
                "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, active BOOLEAN)"
            )
            .result
            .is_ok()
        );
        worker.submit(Command::Catalog).unwrap();
        let Ok(Payload::Catalog(tables)) = receive(&mut worker).result else {
            panic!("catalog")
        };
        assert_eq!(tables[0].name, "items");
        assert_eq!(tables[0].columns.len(), 3);
        assert_eq!(tables[0].columns[0].detail, "PRIMARY KEY");
        assert!(
            query(
                &mut worker,
                "INSERT INTO items (id,name,active) VALUES (1,'Кедр',true),(2,NULL,NULL)"
            )
            .result
            .is_ok()
        );
        assert!(
            query(&mut worker, "BEGIN")
                .status
                .contains("транзакция открыта")
        );
        assert!(
            query(&mut worker, "UPDATE items SET name='Откат' WHERE id=1")
                .result
                .is_ok()
        );
        assert!(query(&mut worker, "ROLLBACK").status.contains("готов"));
        let Ok(Payload::Rows(result)) =
            query(&mut worker, "SELECT name,active FROM items ORDER BY id").result
        else {
            panic!("rows")
        };
        assert_eq!(result.rows[0][0].display(), "Кедр");
        assert!(matches!(result.rows[1][0], Cell::Null));
        assert!(matches!(result.rows[1][1], Cell::Null));
        query(&mut worker, "BEGIN");
        let failed = query(&mut worker, "SELECT unknown FROM items");
        assert!(failed.result.err().unwrap().contains("42703"));
        assert!(failed.status.contains("нужен ROLLBACK"));
        assert!(query(&mut worker, "ROLLBACK").result.is_ok());
        let (holder, connection) = runtime
            .block_on(
                tokio_postgres::Config::new()
                    .host("127.0.0.1")
                    .port(port)
                    .user("admin")
                    .password("test-password-123")
                    .dbname("kedrdb")
                    .connect(NoTls),
            )
            .unwrap();
        runtime.spawn(async move {
            let _ = connection.await;
        });
        runtime.block_on(holder.batch_execute("BEGIN")).unwrap();
        worker
            .submit(Command::Query("DELETE FROM items WHERE id=1".into()))
            .unwrap();
        std::thread::sleep(Duration::from_millis(100));
        assert!(worker.submit(Command::Catalog).is_err());
        worker.cancel();
        let cancelled = receive(&mut worker);
        assert!(cancelled.result.err().unwrap().contains("57014"));
        runtime.block_on(holder.batch_execute("ROLLBACK")).unwrap();
        drop(holder);
        for batch in 0..22 {
            let values = (0..50)
                .map(|index| format!("({},'строка',true)", batch * 50 + index + 3))
                .collect::<Vec<_>>()
                .join(",");
            let reply = query(
                &mut worker,
                &format!("INSERT INTO items (id,name,active) VALUES {values}"),
            );
            if let Err(error) = reply.result {
                panic!("insert batch {batch}: {error}; status {}", reply.status);
            }
        }
        let Ok(Payload::Rows(result)) =
            query(&mut worker, "SELECT * FROM items ORDER BY id").result
        else {
            panic!("limited rows")
        };
        assert_eq!(result.rows.len(), 1000);
        assert!(result.truncated);
        assert_eq!(result.affected, 1102);
        assert!(
            query(&mut worker, "DELETE FROM items WHERE id=2")
                .result
                .is_ok()
        );
        shutdown.send(true).unwrap();
        runtime.block_on(task).unwrap().unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while worker.connection_alive() == Some(true) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(2));
        }
        assert_eq!(worker.connection_alive(), Some(false));
        let lost = query(&mut worker, "SELECT id FROM items");
        assert!(lost.result.is_err());
        assert_eq!(lost.status, "Нет подключения");
        let server = runtime.block_on(Server::bind(config())).unwrap();
        let port = server.local_addr().unwrap().port();
        let (shutdown, rx) = tokio::sync::watch::channel(false);
        let task = runtime.spawn(server.run(rx));
        worker
            .submit(Command::Connect(profile(port, "test-password-123")))
            .unwrap();
        assert!(matches!(
            receive(&mut worker).result,
            Ok(Payload::Connected(_))
        ));
        assert!(
            query(&mut worker, "SELECT COUNT(*) FROM items")
                .result
                .is_ok()
        );
        drop(worker);
        shutdown.send(true).unwrap();
        runtime.block_on(task).unwrap().unwrap();
        std::fs::remove_dir_all(root).unwrap();
    }
}
