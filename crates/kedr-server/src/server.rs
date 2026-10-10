// SPDX-License-Identifier: Apache-2.0
use crate::{
    credentials::{self, Credential},
    metrics::{self, Metrics},
    wire::{self, Cursor, Fault, Result},
};
use kedr_core::{
    database::FileDatabase,
    record::Value,
    sql::{DataType, Description, QueryResult, ResultColumn, SqlSession},
    transaction::{CancelHandle, SessionLimits, SharedDatabase},
};
use rsasl::{mechname::Mechname, prelude::State};
use std::{
    collections::HashMap,
    io::{self, BufReader},
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Semaphore, watch},
    task::JoinSet,
};
use tokio_rustls::TlsAcceptor;

pub struct Config {
    pub data: PathBuf,
    pub auth: PathBuf,
    pub listen: SocketAddr,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}
trait Socket: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Socket for T {}
type Stream = Box<dyn Socket>;
type Registry = Arc<Mutex<HashMap<i32, (i32, CancelHandle)>>>;
struct Shared {
    database: SharedDatabase,
    credential: Credential,
    tls: Option<TlsAcceptor>,
    require_tls: bool,
    registry: Registry,
    metrics: Arc<Metrics>,
    data: PathBuf,
}
pub struct Server {
    listener: TcpListener,
    shared: Arc<Shared>,
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
fn tls(config: &Config) -> io::Result<Option<TlsAcceptor>> {
    match (&config.tls_cert, &config.tls_key) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => {
            if std::fs::metadata(cert)?.len() > 1024 * 1024
                || std::fs::metadata(key)?.len() > 1024 * 1024
            {
                return Err(invalid("TLS file size limit"));
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if std::fs::metadata(key)?.permissions().mode() & 0o077 != 0 {
                    return Err(invalid("TLS key must have mode 0600"));
                }
            }
            let certs = rustls_pemfile::certs(&mut BufReader::new(std::fs::File::open(cert)?))
                .collect::<io::Result<Vec<_>>>()?;
            let key = rustls_pemfile::private_key(&mut BufReader::new(std::fs::File::open(key)?))?
                .ok_or_else(|| invalid("missing TLS private key"))?;
            let provider = Arc::new(rustls::crypto::ring::default_provider());
            let config = rustls::ServerConfig::builder_with_provider(provider)
                .with_safe_default_protocol_versions()
                .map_err(|error| invalid(error.to_string()))?
                .with_no_client_auth()
                .with_single_cert(certs, key)
                .map_err(|error| invalid(error.to_string()))?;
            Ok(Some(TlsAcceptor::from(Arc::new(config))))
        }
        _ => Err(invalid("TLS requires both certificate and key")),
    }
}
impl Server {
    /// Opens and locks the existing data directory. Remote listeners require TLS.
    pub async fn bind(config: Config) -> io::Result<Self> {
        let tls = tls(&config)?;
        if !config.listen.ip().is_loopback() && tls.is_none() {
            return Err(invalid("non-loopback listener requires TLS"));
        }
        let credential = Credential::read(&config.auth)?;
        Self::bind_credential(config, credential, tls).await
    }
    /// Managed local server: ephemeral loopback port and in-memory SCRAM verifier.
    /// Authentication is still mandatory; no plaintext password is persisted.
    pub async fn bind_local(data: PathBuf, credential: Credential) -> io::Result<Self> {
        Self::bind_credential(
            Config {
                data,
                auth: PathBuf::new(),
                listen: "127.0.0.1:0".parse().unwrap(),
                tls_cert: None,
                tls_key: None,
            },
            credential,
            None,
        )
        .await
    }
    async fn bind_credential(
        config: Config,
        credential: Credential,
        tls: Option<TlsAcceptor>,
    ) -> io::Result<Self> {
        let database = FileDatabase::open(&config.data, 16 * 1024 * 1024)
            .map_err(|error| invalid(error.to_string()))?;
        let database = SharedDatabase::new(
            database,
            SessionLimits {
                max_sessions: 32,
                transaction_lifetime: Duration::from_secs(300),
                idle_in_transaction: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .map_err(|error| invalid(error.to_string()))?;
        Ok(Self {
            listener: TcpListener::bind(config.listen).await?,
            shared: Arc::new(Shared {
                database,
                credential,
                tls,
                require_tls: !config.listen.ip().is_loopback(),
                registry: Arc::default(),
                metrics: Arc::default(),
                data: config.data,
            }),
        })
    }
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }
    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> io::Result<()> {
        if *shutdown.borrow() {
            return Ok(());
        }
        let (stop, stopped) = watch::channel(false);
        let mut task_failed = false;
        let slots = Arc::new(Semaphore::new(64));
        let mut tasks = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                Some(result) = tasks.join_next(), if !tasks.is_empty() => { if result.is_err() { task_failed = true; break; } },
                accepted = self.listener.accept() => {
                    let (socket, _) = accepted?;
                    self.shared.metrics.connections.fetch_add(1, Ordering::Relaxed);
                    match slots.clone().try_acquire_owned() {
                        Ok(permit) => { let shared = self.shared.clone(); let shutdown = stopped.clone(); tasks.spawn(async move {
                            let _permit = permit;
                            // Credentials, packets and SQL are never logged.
                            if let Err(error) = connection(socket, shared, shutdown).await { metrics::event(serde_json::json!({"event":"connection_error","sqlstate":error.state})); }
                        }); },
                        Err(_) => {self.shared.metrics.rejected.fetch_add(1,Ordering::Relaxed);drop(socket);},
                    }
                }
            }
        }
        let _ = stop.send(true);
        if let Ok(registry) = self.shared.registry.lock() {
            for (_, cancel) in registry.values() {
                cancel.cancel();
            }
        }
        // Connection tasks observe shutdown. Await their SQL workers and rollback before releasing the file lock.
        while let Some(result) = tasks.join_next().await {
            if result.is_err() {
                task_failed = true;
            }
        }
        if task_failed {
            Err(io::Error::other("connection task panicked"))
        } else {
            Ok(())
        }
    }
}
struct Registration {
    registry: Registry,
    pid: i32,
    cancel: CancelHandle,
}
impl Drop for Registration {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Ok(mut entries) = self.registry.lock() {
            entries.remove(&self.pid);
        }
    }
}
async fn connection(
    socket: TcpStream,
    shared: Arc<Shared>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    socket.set_nodelay(true)?;
    let startup: Result<Option<(Stream, SqlSession)>> = tokio::select! {
        _ = shutdown.changed() => return Ok(()),
        startup = tokio::time::timeout(Duration::from_secs(8), authenticate(socket, &shared)) => startup.map_err(|_| Fault::new("08006", "authentication timeout")).and_then(|r|r),
    };
    let startup = startup.inspect_err(|_| {
        shared
            .metrics
            .authentication_errors
            .fetch_add(1, Ordering::Relaxed);
    })?;
    let Some((mut socket, session)) = startup else {
        return Ok(());
    };
    let cancel = session.cancel_handle();
    let (pid, secret) = {
        let mut registry = shared
            .registry
            .lock()
            .map_err(|_| Fault::new("58000", "session registry unavailable"))?;
        let mut pid = i32::from_be_bytes(credentials::random()?) & i32::MAX;
        while pid == 0 || registry.contains_key(&pid) {
            pid = i32::from_be_bytes(credentials::random()?) & i32::MAX;
        }
        let secret = i32::from_be_bytes(credentials::random()?);
        registry.insert(pid, (secret, cancel.clone()));
        (pid, secret)
    };
    let _registration = Registration {
        registry: shared.registry.clone(),
        pid,
        cancel: cancel.clone(),
    };
    let mut key = pid.to_be_bytes().to_vec();
    key.extend_from_slice(&secret.to_be_bytes());
    wire::send(&mut socket, b'K', &key).await?;
    for (name, value) in [
        ("server_version", "16.0-KedrDB-0.1.0"),
        ("server_encoding", "UTF8"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, YMD"),
        ("integer_datetimes", "on"),
        ("standard_conforming_strings", "on"),
        ("TimeZone", "UTC"),
        ("kedr_api_version", "1"),
        ("kedr_features", "sql,catalog,cancel,scram,metrics"),
    ] {
        let mut bytes = Vec::new();
        wire::cstring(&mut bytes, name);
        wire::cstring(&mut bytes, value);
        wire::send(&mut socket, b'S', &bytes).await?;
    }
    wire::send(&mut socket, b'Z', b"I").await?;
    let (mut reader, mut writer) = tokio::io::split(socket);
    let (sender, mut incoming) = tokio::sync::mpsc::channel(1);
    let reader_cancel = cancel.clone();
    let mut reader_shutdown = shutdown.clone();
    let reader_task = tokio::spawn(async move {
        loop {
            let frame = tokio::select! { _ = reader_shutdown.changed() => break, frame = wire::frame(&mut reader) => frame };
            let finished = frame.is_err();
            if finished {
                reader_cancel.cancel();
            }
            if sender.send(frame).await.is_err() || finished {
                break;
            }
        }
        reader_cancel.cancel();
    });
    let mut state = ConnectionState::new(session, shared.clone());
    let result = async {
        loop {
            let frame = tokio::select! { _ = shutdown.changed() => break, frame = incoming.recv() => frame };
            let Some(frame) = frame else { break; };
            let (tag, bytes) = frame?;
            if tag == b'X' { if !bytes.is_empty() { return Err(wire::protocol()); } break; }
            if state.skip && tag != b'S' { continue; }
            match state.message(tag, &bytes, &mut writer).await {
                Ok(()) => {},
                Err(error) => {
                    shared.metrics.protocol_errors.fetch_add(1,Ordering::Relaxed);
                    let _ = state.run({ let code = error.state; let message = error.message.clone(); move |session| session.fail(kedr_core::Error::Sql { state: code, message }) }).await;
                    if state.status == b'T' { state.status = b'E'; }
                    wire::error(&mut writer, &error, false).await?;
                    if tag == b'Q' { wire::send(&mut writer, b'Z', &[state.status]).await?; }
                    else if tag == b'S' { return Err(error); }
                    else { state.skip = true; }
                }
            }
        }
        Ok(())
    }.await;
    cancel.cancel();
    reader_task.abort();
    let _ = reader_task.await;
    // Drop the session off the async executor: joining its worker may wait for filesystem I/O.
    let session = state.session;
    let _ = tokio::task::spawn_blocking(move || drop(session)).await;
    result
}
async fn authenticate(socket: TcpStream, shared: &Shared) -> Result<Option<(Stream, SqlSession)>> {
    let mut socket: Stream = Box::new(socket);
    let mut encrypted = false;
    let mut packet = wire::startup(&mut socket).await?;
    let mut code = i32::from_be_bytes(packet[..4].try_into().map_err(|_| wire::protocol())?);
    if code == 80877104 {
        if packet.len() != 4 {
            return Err(wire::protocol());
        }
        socket.write_all(b"N").await?;
        packet = wire::startup(&mut socket).await?;
        code = i32::from_be_bytes(packet[..4].try_into().map_err(|_| wire::protocol())?);
    }
    if code == 80877103 {
        if packet.len() != 4 {
            return Err(wire::protocol());
        }
        if let Some(tls) = &shared.tls {
            socket.write_all(b"S").await?;
            socket = Box::new(tls.accept(socket).await?);
            encrypted = true;
        } else {
            socket.write_all(b"N").await?;
        }
        packet = wire::startup(&mut socket).await?;
        code = i32::from_be_bytes(packet[..4].try_into().map_err(|_| wire::protocol())?);
    }
    if code == 80877102 {
        if shared.require_tls && !encrypted {
            return Err(Fault::new("28000", "remote cancellation requires TLS"));
        }
        if packet.len() != 12 {
            return Err(wire::protocol());
        }
        let mut cursor = Cursor::new(&packet[4..]);
        let pid = cursor.i32()?;
        let secret = cursor.i32()?;
        if let Ok(entries) = shared.registry.lock()
            && let Some((expected, cancel)) = entries.get(&pid)
            && *expected == secret
        {
            cancel.cancel();
        }
        return Ok(None);
    }
    let authentication = async {
        if code != 196608 {
            return Err(Fault::new(
                "0A000",
                "KedrDB supports PostgreSQL protocol 3.0",
            ));
        }
        if shared.require_tls && !encrypted {
            return Err(Fault::new("28000", "remote connections require TLS"));
        }
        let mut cursor = Cursor::new(&packet[4..]);
        let mut user = None;
        let mut database = None;
        let mut seen = HashMap::new();
        for _ in 0..32 {
            let key = cursor.string(63)?;
            if key.is_empty() {
                cursor.end()?;
                break;
            }
            let value = cursor.string(1024)?;
            if seen.insert(key.clone(), ()).is_some() {
                return Err(wire::protocol());
            }
            match key.as_str() {
                "user" => user = Some(value),
                "database" => database = Some(value),
                "client_encoding"
                    if !value.eq_ignore_ascii_case("UTF8")
                        && !value.eq_ignore_ascii_case("UTF-8") =>
                {
                    return Err(Fault::new("0A000", "only UTF8 is supported"));
                }
                "options" if !value.is_empty() => {
                    return Err(Fault::new("0A000", "startup options are not supported"));
                }
                key if key.starts_with("_pq_.") => {
                    return Err(Fault::new("0A000", "protocol extension is not supported"));
                }
                _ => {}
            }
        }
        cursor.end()?;
        if user.as_deref() != Some(&shared.credential.username) {
            return Err(Fault::new("28P01", "authentication failed"));
        }
        if database.as_deref().unwrap_or("kedrdb") != "kedrdb" {
            return Err(Fault::new(
                "3D000",
                "this server exposes the database kedrdb",
            ));
        }
        let mechanism = Mechname::parse(b"SCRAM-SHA-256").map_err(|_| wire::protocol())?;
        let server = shared.credential.server()?;
        let mut session = server
            .start_suggested(mechanism)
            .map_err(|_| wire::protocol())?;
        let mut bytes = 10i32.to_be_bytes().to_vec();
        bytes.extend_from_slice(b"SCRAM-SHA-256\0\0");
        wire::send(&mut socket, b'R', &bytes).await?;
        let (tag, first) = wire::frame_limited(&mut socket, 8192).await?;
        if tag != b'p' {
            return Err(wire::protocol());
        }
        let mut cursor = Cursor::new(&first);
        if cursor.string(63)? != "SCRAM-SHA-256" {
            return Err(wire::protocol());
        }
        let length = cursor.i32()?;
        if !(0..=8192).contains(&length) {
            return Err(wire::protocol());
        }
        let first = cursor.take(length as usize)?;
        cursor.end()?;
        let failed = || Fault::new("28P01", "authentication failed");
        let mut challenge = Vec::new();
        session
            .step(Some(first), &mut challenge)
            .map_err(|_| failed())?;
        let mut bytes = 11i32.to_be_bytes().to_vec();
        bytes.extend(challenge);
        wire::send(&mut socket, b'R', &bytes).await?;
        let (tag, proof) = wire::frame_limited(&mut socket, 8192).await?;
        if tag != b'p' {
            return Err(wire::protocol());
        }
        let mut final_message = Vec::new();
        let state = session
            .step(Some(&proof), &mut final_message)
            .map_err(|_| failed())?;
        if !matches!(state, State::Finished(_)) || session.validation() != Some(true) {
            return Err(failed());
        }
        let mut bytes = 12i32.to_be_bytes().to_vec();
        bytes.extend(final_message);
        wire::send(&mut socket, b'R', &bytes).await?;
        let session = SqlSession::new(shared.database.connect().map_err(Fault::from)?);
        wire::send(&mut socket, b'R', &0i32.to_be_bytes()).await?;
        Ok(session)
    }
    .await;
    match authentication {
        Ok(session) => Ok(Some((socket, session))),
        Err(fault) => {
            let _ = wire::error(&mut socket, &fault, true).await;
            Err(fault)
        }
    }
}
#[derive(Clone)]
struct Statement {
    sql: String,
    description: Description,
    parameter_oids: Vec<i32>,
}
struct Portal {
    statement: Statement,
    parameters: Vec<Value>,
    formats: Vec<i16>,
    result: Option<QueryResult>,
    position: usize,
}
struct ConnectionState {
    session: Arc<Mutex<SqlSession>>,
    statements: HashMap<String, Statement>,
    portals: HashMap<String, Portal>,
    status: u8,
    implicit: bool,
    skip: bool,
    shared: Arc<Shared>,
}
impl ConnectionState {
    fn new(session: SqlSession, shared: Arc<Shared>) -> Self {
        Self {
            session: Arc::new(Mutex::new(session)),
            statements: HashMap::new(),
            portals: HashMap::new(),
            status: b'I',
            implicit: false,
            skip: false,
            shared,
        }
    }
    async fn run<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut SqlSession) -> kedr_core::Result<T> + Send + 'static,
    ) -> Result<T> {
        let session = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session = session
                .lock()
                .map_err(|_| Fault::new("58000", "SQL session unavailable"))?;
            operation(&mut session).map_err(Fault::from)
        })
        .await
        .map_err(|_| Fault::new("58000", "SQL worker failed"))?
    }
    async fn execute(
        &mut self,
        sql: String,
        parameters: Vec<Value>,
        extended: bool,
    ) -> Result<QueryResult> {
        let began = std::time::Instant::now();
        let result = self.execute_inner(sql, parameters, extended).await;
        self.shared
            .metrics
            .query(began, result.as_ref().err().map(|e| e.state));
        result
    }
    async fn execute_inner(
        &mut self,
        sql: String,
        parameters: Vec<Value>,
        extended: bool,
    ) -> Result<QueryResult> {
        if metrics_query(&sql) {
            if !parameters.is_empty() {
                return Err(wire::protocol());
            }
            let mut value = self.shared.metrics.snapshot();
            let lock = self.shared.database.lock_metrics();
            let io = kedr_core::metrics::storage_metrics();
            value["locks"] = serde_json::json!({"readers":lock.readers,"writer":lock.writer,"waiting":lock.waiting,"grants":lock.grants,"total_wait_ns":lock.total_wait_nanos,"max_wait_ns":lock.max_wait_nanos});
            value["active_sessions"] = self.shared.database.active_sessions().into();
            value["storage"] = serde_json::json!({"read_bytes":io.read_bytes,"data_write_bytes":io.data_write_bytes,"wal_write_bytes":io.wal_write_bytes,"fsync_calls":io.fsync_calls,"fsync_ns":io.fsync_nanos,"io_errors":io.io_errors,"checkpoints":io.checkpoints,"checkpoint_ns":io.checkpoint_nanos,"checkpoint_errors":io.checkpoint_errors});
            let root = self.shared.data.clone();
            value["system"] = tokio::task::spawn_blocking(move || metrics::system(&root))
                .await
                .map_err(|_| Fault::new("58000", "metric sampler failed"))?;
            return Ok(QueryResult {
                columns: metrics_columns(),
                rows: vec![vec![Value::Text(value.to_string())]],
                affected: 1,
                tag: "SHOW".into(),
            });
        }
        if status_query(&sql) {
            if !parameters.is_empty() {
                return Err(wire::protocol());
            }
            return Ok(QueryResult {
                columns: status_columns(),
                rows: vec![vec![
                    Value::Text("1".into()),
                    Value::Text(env!("CARGO_PKG_VERSION").into()),
                    Value::Text((self.status as char).to_string()),
                ]],
                affected: 1,
                tag: "SHOW".into(),
            });
        }
        let word = sql
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_end_matches(';')
            .to_ascii_uppercase();
        let control = matches!(word.as_str(), "BEGIN" | "COMMIT" | "ROLLBACK");
        if self.status == b'E' && word == "COMMIT" {
            self.run(|session| session.execute("ROLLBACK", &[])).await?;
            self.status = b'I';
            self.implicit = false;
            return Ok(QueryResult {
                columns: Vec::new(),
                rows: Vec::new(),
                affected: 0,
                tag: "ROLLBACK".into(),
            });
        }
        if extended && !control && self.status == b'I' && !self.implicit {
            self.run(|session| session.execute("BEGIN", &[])).await?;
            self.implicit = true;
        }
        let result = self
            .run(move |session| session.execute(&sql, &parameters))
            .await?;
        match result.tag.as_str() {
            "BEGIN" => self.status = b'T',
            "COMMIT" | "ROLLBACK" => {
                self.status = b'I';
                self.implicit = false;
                self.portals.clear();
            }
            _ => {}
        }
        Ok(result)
    }
    async fn message(
        &mut self,
        tag: u8,
        bytes: &[u8],
        writer: &mut (impl AsyncWrite + Unpin),
    ) -> Result<()> {
        let mut cursor = Cursor::new(bytes);
        match tag {
            b'Q' => {
                let sql = cursor.string(65_536)?;
                cursor.end()?;
                if sql.trim().trim_end_matches(';').is_empty() {
                    wire::send(writer, b'I', &[]).await?;
                } else {
                    let result = self.execute(sql, Vec::new(), false).await?;
                    if !result.columns.is_empty() {
                        wire::send(writer, b'T', &wire::description(&result.columns, &[])).await?;
                    }
                    for row in &result.rows {
                        wire::send(writer, b'D', &wire::row(row, &[])).await?;
                    }
                    complete(writer, &result).await?;
                }
                self.statements.remove("");
                self.portals.remove("");
                wire::send(writer, b'Z', &[self.status]).await?;
            }
            b'P' => {
                let name = cursor.string(63)?;
                let sql = cursor.string(65_536)?;
                let count = cursor.count()?;
                let mut types = Vec::new();
                let mut parameter_oids = Vec::new();
                for _ in 0..count {
                    let oid = cursor.i32()?;
                    types.push(wire::kind(oid)?);
                    parameter_oids.push(oid);
                }
                cursor.end()?;
                if !name.is_empty() && self.statements.contains_key(&name) {
                    return Err(Fault::new("42P05", "prepared statement already exists"));
                }
                if self.statements.len() >= 16 && !self.statements.contains_key(&name) {
                    return Err(Fault::new("54000", "prepared statement limit is 16"));
                }
                let description = if status_query(&sql) || metrics_query(&sql) {
                    if !types.is_empty() {
                        return Err(wire::protocol());
                    }
                    Description {
                        parameters: Vec::new(),
                        columns: if metrics_query(&sql) {
                            metrics_columns()
                        } else {
                            status_columns()
                        },
                    }
                } else {
                    self.run({
                        let sql = sql.clone();
                        move |session| session.describe(&sql, &types)
                    })
                    .await?
                };
                if name.is_empty() {
                    self.portals.remove("");
                }
                parameter_oids.resize(description.parameters.len(), 0);
                for (oid, kind) in parameter_oids.iter_mut().zip(&description.parameters) {
                    if *oid == 0 {
                        *oid = wire::oid(*kind);
                    }
                }
                self.statements.insert(
                    name,
                    Statement {
                        sql,
                        description,
                        parameter_oids,
                    },
                );
                wire::send(writer, b'1', &[]).await?;
            }
            b'B' => {
                let name = cursor.string(63)?;
                let source = cursor.string(63)?;
                let statement = self
                    .statements
                    .get(&source)
                    .cloned()
                    .ok_or_else(|| Fault::new("26000", "prepared statement does not exist"))?;
                let format_count = cursor.count()?;
                let mut formats = Vec::new();
                for _ in 0..format_count {
                    formats.push(cursor.i16()?);
                }
                let count = cursor.count()?;
                if count != statement.description.parameters.len() {
                    return Err(wire::protocol());
                }
                let formats = expand_formats(formats, count)?;
                let mut parameters = Vec::new();
                for (index, kind) in statement.description.parameters.iter().enumerate() {
                    let length = cursor.i32()?;
                    parameters.push(if length == -1 {
                        Value::Null
                    } else if length < 0 {
                        return Err(wire::protocol());
                    } else {
                        let bytes = cursor.take(length as usize)?;
                        if statement.parameter_oids[index] == 21 {
                            let value = if formats[index] == 1 {
                                i16::from_be_bytes(bytes.try_into().map_err(|_| wire::protocol())?)
                            } else {
                                std::str::from_utf8(bytes)
                                    .map_err(|_| wire::protocol())?
                                    .parse::<i16>()
                                    .map_err(|_| {
                                        Fault::new("22P02", "invalid smallint parameter")
                                    })?
                            };
                            Value::Int(i32::from(value))
                        } else {
                            wire::decode(bytes, *kind, formats[index] == 1)?
                        }
                    });
                }
                let count = cursor.count()?;
                let mut formats = Vec::new();
                for _ in 0..count {
                    formats.push(cursor.i16()?);
                }
                cursor.end()?;
                let formats = expand_formats(formats, statement.description.columns.len())?;
                if !name.is_empty() && self.portals.contains_key(&name) {
                    return Err(Fault::new("42P03", "portal already exists"));
                }
                if self.portals.len() >= 16 && !self.portals.contains_key(&name) {
                    return Err(Fault::new("54000", "portal limit is 16"));
                }
                let retained = self
                    .portals
                    .iter()
                    .filter(|(key, _)| *key != &name)
                    .map(|(_, portal)| values_size(&portal.parameters))
                    .sum::<usize>();
                if retained + values_size(&parameters) > 1024 * 1024 {
                    return Err(Fault::new("54000", "portal parameter memory limit"));
                }
                self.portals.insert(
                    name,
                    Portal {
                        statement,
                        parameters,
                        formats,
                        result: None,
                        position: 0,
                    },
                );
                wire::send(writer, b'2', &[]).await?;
            }
            b'D' => {
                let kind = cursor.byte()?;
                let name = cursor.string(63)?;
                cursor.end()?;
                let (columns, formats) = match kind {
                    b'S' => {
                        let statement = self.statements.get(&name).ok_or_else(|| {
                            Fault::new("26000", "prepared statement does not exist")
                        })?;
                        let mut types = (statement.description.parameters.len() as i16)
                            .to_be_bytes()
                            .to_vec();
                        for oid in &statement.parameter_oids {
                            types.extend_from_slice(&oid.to_be_bytes());
                        }
                        wire::send(writer, b't', &types).await?;
                        (&statement.description.columns, &[][..])
                    }
                    b'P' => {
                        let portal = self
                            .portals
                            .get(&name)
                            .ok_or_else(|| Fault::new("34000", "portal does not exist"))?;
                        (
                            &portal.statement.description.columns,
                            portal.formats.as_slice(),
                        )
                    }
                    _ => return Err(wire::protocol()),
                };
                if columns.is_empty() {
                    wire::send(writer, b'n', &[]).await?;
                } else {
                    wire::send(writer, b'T', &wire::description(columns, formats)).await?;
                }
            }
            b'E' => {
                let name = cursor.string(63)?;
                let limit = cursor.i32()?;
                cursor.end()?;
                if limit < 0 {
                    return Err(wire::protocol());
                }
                let mut portal = self
                    .portals
                    .remove(&name)
                    .ok_or_else(|| Fault::new("34000", "portal does not exist"))?;
                if portal.result.is_none() {
                    let result = self
                        .execute(
                            portal.statement.sql.clone(),
                            portal.parameters.clone(),
                            true,
                        )
                        .await?;
                    if result.columns != portal.statement.description.columns {
                        return Err(Fault::new(
                            "0A000",
                            "prepared result changed; prepare again",
                        ));
                    }
                    let retained = self
                        .portals
                        .values()
                        .filter_map(|portal| portal.result.as_ref())
                        .map(|result| {
                            result
                                .rows
                                .iter()
                                .map(|row| values_size(row))
                                .sum::<usize>()
                        })
                        .sum::<usize>();
                    if retained
                        + result
                            .rows
                            .iter()
                            .map(|row| values_size(row))
                            .sum::<usize>()
                        > 8 * 1024 * 1024
                    {
                        return Err(Fault::new("54000", "portal result memory limit"));
                    }
                    portal.result = Some(result);
                }
                let result = portal.result.as_ref().ok_or_else(wire::protocol)?;
                let end = if limit == 0 {
                    result.rows.len()
                } else {
                    (portal.position + limit as usize).min(result.rows.len())
                };
                for row in &result.rows[portal.position..end] {
                    wire::send(writer, b'D', &wire::row(row, &portal.formats)).await?;
                }
                portal.position = end;
                if end < result.rows.len() {
                    wire::send(writer, b's', &[]).await?;
                } else {
                    complete(writer, result).await?;
                }
                self.portals.insert(name, portal);
            }
            b'C' => {
                let kind = cursor.byte()?;
                let name = cursor.string(63)?;
                cursor.end()?;
                match kind {
                    b'S' => {
                        self.statements.remove(&name);
                    }
                    b'P' => {
                        self.portals.remove(&name);
                    }
                    _ => return Err(wire::protocol()),
                }
                wire::send(writer, b'3', &[]).await?;
            }
            b'H' => {
                cursor.end()?;
                writer.flush().await?;
            }
            b'S' => {
                cursor.end()?;
                if self.implicit {
                    let command = if self.skip { "ROLLBACK" } else { "COMMIT" };
                    self.run(move |session| session.execute(command, &[]))
                        .await?;
                    self.implicit = false;
                    self.portals.clear();
                }
                self.skip = false;
                wire::send(writer, b'Z', &[self.status]).await?;
            }
            _ => return Err(Fault::new("0A000", "unsupported frontend message")),
        }
        Ok(())
    }
}
fn metrics_query(sql: &str) -> bool {
    sql.trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("SHOW KEDR METRICS")
}
fn metrics_columns() -> Vec<ResultColumn> {
    vec![ResultColumn {
        name: "metrics".into(),
        data_type: DataType::Text,
    }]
}
fn status_query(sql: &str) -> bool {
    sql.trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("SHOW KEDR STATUS")
}
fn status_columns() -> Vec<ResultColumn> {
    ["api_version", "server_version", "transaction_status"]
        .into_iter()
        .map(|name| ResultColumn {
            name: name.into(),
            data_type: DataType::Text,
        })
        .collect()
}
fn values_size(values: &[Value]) -> usize {
    values
        .iter()
        .map(|value| match value {
            Value::Text(value) => value.len() + 32,
            _ => 32,
        })
        .sum()
}
fn expand_formats(formats: Vec<i16>, count: usize) -> Result<Vec<i16>> {
    if formats.iter().any(|format| !matches!(format, 0 | 1)) {
        return Err(wire::protocol());
    }
    match formats.len() {
        0 => Ok(vec![0; count]),
        1 => Ok(vec![formats[0]; count]),
        length if length == count => Ok(formats),
        _ => Err(wire::protocol()),
    }
}
async fn complete(writer: &mut (impl AsyncWrite + Unpin), result: &QueryResult) -> Result<()> {
    let tag = match result.tag.as_str() {
        "INSERT" => format!("INSERT 0 {}", result.affected),
        "SELECT" | "UPDATE" | "DELETE" => format!("{} {}", result.tag, result.affected),
        _ => result.tag.clone(),
    };
    let mut bytes = Vec::new();
    wire::cstring(&mut bytes, &tag);
    wire::send(writer, b'C', &bytes).await
}
