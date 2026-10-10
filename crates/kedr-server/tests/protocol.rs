// SPDX-License-Identifier: Apache-2.0
use kedr_core::{database::FileDatabase, page::PAGE_SIZE};
use kedr_server::{Config, Server, credentials::Credential};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{io::AsyncWriteExt, net::TcpStream, sync::watch};
use tokio_postgres::{Client, NoTls};
static NEXT: AtomicU64 = AtomicU64::new(1);
struct Fixture {
    root: PathBuf,
    addr: std::net::SocketAddr,
    shutdown: watch::Sender<bool>,
    server: tokio::task::JoinHandle<std::io::Result<()>>,
}
impl Fixture {
    async fn new() -> Self {
        let root = std::env::temp_dir().join(format!(
            "kedr-server-test-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        drop(FileDatabase::create(root.join("data"), [41; 16], 4 * PAGE_SIZE).unwrap());
        Credential::new("admin", "test-password-123")
            .unwrap()
            .write_new(&root.join("auth"))
            .unwrap();
        let server = Server::bind(Config {
            data: root.join("data"),
            auth: root.join("auth"),
            listen: "127.0.0.1:0".parse().unwrap(),
            tls_cert: None,
            tls_key: None,
        })
        .await
        .unwrap();
        let addr = server.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        Self {
            root,
            addr,
            shutdown,
            server: tokio::spawn(server.run(receiver)),
        }
    }
    async fn client(&self) -> Client {
        let (client, connection) = tokio_postgres::Config::new()
            .host("127.0.0.1")
            .port(self.addr.port())
            .user("admin")
            .password("test-password-123")
            .dbname("kedrdb")
            .connect(NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }
    async fn finish(self) {
        self.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(8), self.server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        drop(FileDatabase::open(self.root.join("data"), 4 * PAGE_SIZE).unwrap());
        std::fs::remove_dir_all(self.root).unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn driver_binary_crud_nulls_transactions_and_reconnect() {
    let fixture = Fixture::new().await;
    let client = fixture.client().await;
    client
        .batch_execute(
            "CREATE TABLE items (id BIGINT PRIMARY KEY, name TEXT, stock INT, active BOOLEAN)",
        )
        .await
        .unwrap();
    let insert = client
        .prepare("INSERT INTO items (id,name,stock,active) VALUES ($1,$2,$3,$4)")
        .await
        .unwrap();
    assert_eq!(
        insert.params(),
        &[
            tokio_postgres::types::Type::INT8,
            tokio_postgres::types::Type::TEXT,
            tokio_postgres::types::Type::INT4,
            tokio_postgres::types::Type::BOOL
        ]
    );
    assert_eq!(
        client
            .execute(&insert, &[&1i64, &"Кедр '✓", &4i32, &Some(true)])
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        client
            .execute(
                &insert,
                &[
                    &2i64,
                    &Option::<String>::None,
                    &Option::<i32>::None,
                    &Option::<bool>::None
                ]
            )
            .await
            .unwrap(),
        1
    );
    let row = client
        .query_one(
            "SELECT id,name,stock,active FROM items WHERE id=$1",
            &[&1i64],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 1);
    assert_eq!(row.get::<_, String>(1), "Кедр '✓");
    assert_eq!(row.get::<_, i32>(2), 4);
    assert!(row.get::<_, bool>(3));
    let row = client
        .query_one("SELECT name,stock,active FROM items WHERE id=$1", &[&2i64])
        .await
        .unwrap();
    assert_eq!(row.get::<_, Option<String>>(0), None);
    assert_eq!(row.get::<_, Option<i32>>(1), None);
    assert_eq!(row.get::<_, Option<bool>>(2), None);
    assert_eq!(
        client
            .execute("UPDATE items SET stock=$1 WHERE id=$2", &[&8i32, &1i64])
            .await
            .unwrap(),
        1
    );
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute(&insert, &[&3i64, &"rollback", &0i32, &false])
        .await
        .unwrap();
    let error = client
        .execute(&insert, &[&1i64, &"duplicate", &0i32, &false])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "23505");
    assert_eq!(
        client
            .query("SELECT id FROM items", &[])
            .await
            .unwrap_err()
            .code()
            .unwrap()
            .code(),
        "25P02"
    );
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT COUNT(*) FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    client.batch_execute("BEGIN READ ONLY").await.unwrap();
    assert!(
        client
            .execute("DELETE FROM items WHERE id=$1", &[&2i64])
            .await
            .is_err()
    );
    client.batch_execute("COMMIT").await.unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute("DELETE FROM items WHERE id=$1", &[&2i64])
        .await
        .unwrap();
    client.batch_execute("COMMIT").await.unwrap();
    let catalog = client.query("SHOW TABLES", &[]).await.unwrap();
    assert_eq!(catalog[0].get::<_, String>(0), "items");
    assert_eq!(
        client
            .query("SHOW COLUMNS FROM items", &[])
            .await
            .unwrap()
            .len(),
        4
    );
    drop(client);
    let second = fixture.client().await;
    assert_eq!(
        second
            .query_one("SELECT COUNT(*) FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    drop(second);
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bad_credentials_packets_disconnect_and_authenticated_cancel() {
    let fixture = Fixture::new().await;
    for (user, password) in [("admin", "wrong-password"), ("other", "test-password-123")] {
        assert!(
            tokio_postgres::Config::new()
                .host("127.0.0.1")
                .port(fixture.addr.port())
                .user(user)
                .password(password)
                .dbname("kedrdb")
                .connect(NoTls)
                .await
                .is_err()
        );
    }
    for bytes in [
        0u32.to_be_bytes().to_vec(),
        u32::MAX.to_be_bytes().to_vec(),
        vec![0, 0, 0, 12, 0, 3, 0, 0, b'u', 0],
    ] {
        let mut stream = TcpStream::connect(fixture.addr).await.unwrap();
        stream.write_all(&bytes).await.unwrap();
        stream.shutdown().await.unwrap();
    }
    let holder = fixture.client().await;
    holder
        .batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    holder.batch_execute("BEGIN").await.unwrap();
    holder
        .execute("INSERT INTO items (id) VALUES ($1)", &[&1i64])
        .await
        .unwrap();
    let waiting = fixture.client().await;
    let token = waiting.cancel_token();
    let task = tokio::spawn(async move {
        waiting
            .batch_execute("INSERT INTO items (id) VALUES (2)")
            .await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    token.cancel_query(NoTls).await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");
    drop(holder);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let next = fixture.client().await;
    assert_eq!(
        next.query_one("SELECT COUNT(*) FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(next);
    fixture.finish().await;
}

async fn send(stream: &mut TcpStream, tag: u8, payload: &[u8]) {
    stream.write_u8(tag).await.unwrap();
    stream.write_u32((payload.len() + 4) as u32).await.unwrap();
    stream.write_all(payload).await.unwrap();
}
async fn read(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(7), async {
        let tag = stream.read_u8().await.unwrap();
        let length = stream.read_u32().await.unwrap();
        assert!((4..=1024 * 1024).contains(&length));
        let mut bytes = vec![0; length as usize - 4];
        stream.read_exact(&mut bytes).await.unwrap();
        (tag, bytes)
    })
    .await
    .unwrap()
}
async fn raw_client(fixture: &Fixture) -> (TcpStream, i32, i32) {
    use rsasl::{
        mechname::Mechname,
        prelude::{SASLClient, SASLConfig},
    };
    let mut socket = TcpStream::connect(fixture.addr).await.unwrap();
    let mut startup = 196608i32.to_be_bytes().to_vec();
    startup.extend_from_slice(b"user\0admin\0database\0kedrdb\0\0");
    socket.write_u32((startup.len() + 4) as u32).await.unwrap();
    socket.write_all(&startup).await.unwrap();
    let (tag, bytes) = read(&mut socket).await;
    assert_eq!(tag, b'R');
    assert_eq!(&bytes[..4], &10i32.to_be_bytes());
    let client = SASLClient::new(
        SASLConfig::with_credentials(None, "admin".into(), "test-password-123".into()).unwrap(),
    );
    let mut client = client
        .start_suggested(&[Mechname::parse(b"SCRAM-SHA-256").unwrap()])
        .unwrap();
    let mut first = Vec::new();
    client.step(None, &mut first).unwrap();
    let mut initial = b"SCRAM-SHA-256\0".to_vec();
    initial.extend_from_slice(&(first.len() as i32).to_be_bytes());
    initial.extend(first);
    send(&mut socket, b'p', &initial).await;
    let (tag, challenge) = read(&mut socket).await;
    assert_eq!(tag, b'R');
    assert_eq!(&challenge[..4], &11i32.to_be_bytes());
    let mut proof = Vec::new();
    client.step(Some(&challenge[4..]), &mut proof).unwrap();
    send(&mut socket, b'p', &proof).await;
    let (tag, final_message) = read(&mut socket).await;
    assert_eq!(tag, b'R');
    assert_eq!(&final_message[..4], &12i32.to_be_bytes());
    client
        .step(Some(&final_message[4..]), &mut Vec::new())
        .unwrap();
    let mut pid = 0;
    let mut secret = 0;
    loop {
        let (tag, bytes) = read(&mut socket).await;
        match tag {
            b'K' => {
                pid = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                secret = i32::from_be_bytes(bytes[4..].try_into().unwrap());
            }
            b'Z' => {
                assert_eq!(bytes, b"I");
                break;
            }
            _ => {}
        }
    }
    (socket, pid, secret)
}
async fn simple(stream: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut bytes = sql.as_bytes().to_vec();
    bytes.push(0);
    send(stream, b'Q', &bytes).await;
    let mut messages = Vec::new();
    loop {
        let message = read(stream).await;
        let done = message.0 == b'Z';
        messages.push(message);
        if done {
            return messages;
        }
    }
}
async fn extended(stream: &mut TcpStream, sql: &str, portal: &str) {
    let mut parse = vec![0];
    parse.extend_from_slice(sql.as_bytes());
    parse.extend_from_slice(&[0, 0, 0]);
    send(stream, b'P', &parse).await;
    let mut bind = portal.as_bytes().to_vec();
    bind.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0, 0]);
    send(stream, b'B', &bind).await;
    let mut execute = portal.as_bytes().to_vec();
    execute.push(0);
    execute.extend_from_slice(&0i32.to_be_bytes());
    send(stream, b'E', &execute).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_prepared_read_sync_shares_reader_lock_and_retires_portals() {
    let fixture = Fixture::new().await;
    let reader = fixture.client().await;
    reader
        .batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    reader
        .batch_execute("INSERT INTO items (id) VALUES (1)")
        .await
        .unwrap();
    let (mut socket, _, _) = raw_client(&fixture).await;
    let mut parse = vec![0];
    parse.extend_from_slice(b"SELECT id FROM items\0\0\0");
    send(&mut socket, b'P', &parse).await;
    let mut bind = b"one\0\0".to_vec();
    bind.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    send(&mut socket, b'B', &bind).await;
    send(&mut socket, b'H', &[]).await;
    assert_eq!(read(&mut socket).await.0, b'1');
    assert_eq!(read(&mut socket).await.0, b'2');
    reader.batch_execute("BEGIN READ ONLY").await.unwrap();
    // Both complete frames arrive together. This read must not request the
    // exclusive writer gate held off by the other connection's read snapshot.
    let mut packet = b"E".to_vec();
    packet.extend_from_slice(&12u32.to_be_bytes());
    packet.extend_from_slice(b"one\0");
    packet.extend_from_slice(&0i32.to_be_bytes());
    packet.extend_from_slice(b"S\0\0\0\x04");
    socket.write_all(&packet).await.unwrap();
    let messages = tokio::time::timeout(Duration::from_secs(2), async {
        let mut messages = Vec::new();
        loop {
            let message = read(&mut socket).await;
            let done = message.0 == b'Z';
            messages.push(message);
            if done {
                return messages;
            }
        }
    })
    .await
    .expect("single read blocked behind an existing reader");
    assert_eq!(messages.iter().map(|m| m.0).collect::<Vec<_>>(), b"DCZ");
    assert_eq!(messages.last().unwrap().1, b"I");
    reader.batch_execute("ROLLBACK").await.unwrap();
    let mut execute = b"one\0".to_vec();
    execute.extend_from_slice(&0i32.to_be_bytes());
    send(&mut socket, b'E', &execute).await;
    send(&mut socket, b'S', &[]).await;
    assert_eq!(read(&mut socket).await.0, b'E');
    assert_eq!(read(&mut socket).await.0, b'Z');
    drop(socket);
    drop(reader);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_then_write_pipeline_is_one_transaction_until_sync() {
    let fixture = Fixture::new().await;
    let (mut socket, _, _) = raw_client(&fixture).await;
    simple(&mut socket, "CREATE TABLE items (id BIGINT PRIMARY KEY)").await;
    extended(&mut socket, "SELECT COUNT(*) FROM items", "read").await;
    // Flush is not a commit boundary, even when the preceding query is read-only.
    send(&mut socket, b'H', &[]).await;
    for expected in b"12DC" {
        assert_eq!(read(&mut socket).await.0, *expected);
    }
    extended(&mut socket, "INSERT INTO items (id) VALUES (1)", "write").await;
    let mut invalid = vec![0];
    invalid.extend_from_slice(b"SELECT missing FROM items\0\0\0");
    send(&mut socket, b'P', &invalid).await;
    send(&mut socket, b'S', &[]).await;
    let mut tags = Vec::new();
    loop {
        let (tag, bytes) = read(&mut socket).await;
        tags.push(tag);
        if tag == b'Z' {
            assert_eq!(bytes, b"I");
            break;
        }
    }
    assert_eq!(tags, b"12CEZ");
    let messages = simple(&mut socket, "SELECT COUNT(*) FROM items").await;
    assert_eq!(
        messages.iter().find(|m| m.0 == b'D').unwrap().1.last(),
        Some(&b'0')
    );
    drop(socket);
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_pipeline_error_rollback_and_portal_suspension() {
    let fixture = Fixture::new().await;
    let (mut socket, _, _) = raw_client(&fixture).await;
    assert!(
        !simple(&mut socket, "CREATE TABLE items (id BIGINT PRIMARY KEY)")
            .await
            .iter()
            .any(|message| message.0 == b'E')
    );
    extended(&mut socket, "INSERT INTO items (id) VALUES (1)", "").await;
    let mut invalid = vec![0];
    invalid.extend_from_slice(b"SELECT unknown FROM items\0\0\0");
    send(&mut socket, b'P', &invalid).await;
    // This valid-looking write must be discarded until Sync after the Parse error.
    extended(&mut socket, "INSERT INTO items (id) VALUES (2)", "").await;
    send(&mut socket, b'S', &[]).await;
    let mut tags = Vec::new();
    loop {
        let (tag, bytes) = read(&mut socket).await;
        tags.push(tag);
        if tag == b'Z' {
            assert_eq!(bytes, b"I");
            break;
        }
    }
    assert_eq!(tags, b"12CEZ");
    let messages = simple(&mut socket, "SELECT COUNT(*) FROM items").await;
    let row = &messages.iter().find(|message| message.0 == b'D').unwrap().1;
    assert_eq!(row.last(), Some(&b'0'));
    extended(&mut socket, "INSERT INTO items (id) VALUES (1),(2)", "").await;
    send(&mut socket, b'S', &[]).await;
    loop {
        if read(&mut socket).await.0 == b'Z' {
            break;
        }
    }
    let mut parse = vec![0];
    parse.extend_from_slice(b"SELECT id FROM items ORDER BY id\0\0\0");
    send(&mut socket, b'P', &parse).await;
    let mut bind = b"page\0\0".to_vec();
    bind.extend_from_slice(&[0, 0, 0, 0, 0, 0]);
    send(&mut socket, b'B', &bind).await;
    let mut execute = b"page\0".to_vec();
    execute.extend_from_slice(&1i32.to_be_bytes());
    send(&mut socket, b'E', &execute).await;
    assert_eq!(read(&mut socket).await.0, b'1');
    assert_eq!(read(&mut socket).await.0, b'2');
    assert_eq!(read(&mut socket).await.0, b'D');
    assert_eq!(read(&mut socket).await.0, b's');
    send(&mut socket, b'E', &execute).await;
    assert_eq!(read(&mut socket).await.0, b'D');
    assert_eq!(read(&mut socket).await.0, b'C');
    send(&mut socket, b'S', &[]).await;
    assert_eq!(read(&mut socket).await.0, b'Z');
    drop(socket);
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_authenticated_messages_recover_and_partial_frame_times_out() {
    use tokio::io::AsyncReadExt;
    let fixture = Fixture::new().await;
    let (mut socket, _, _) = raw_client(&fixture).await;
    let corpus = [
        (b'P', vec![0]),
        (b'P', b"\0SELECT 1\0\xff\xff".to_vec()),
        (b'B', b"\0\0\xff\xff".to_vec()),
        (b'D', b"Z\0".to_vec()),
        (b'E', b"\0\xff\xff\xff\xff".to_vec()),
        (b'C', b"S\xff\0".to_vec()),
        (b'H', vec![1]),
        (b'Y', Vec::new()),
    ];
    for (tag, bytes) in corpus {
        send(&mut socket, tag, &bytes).await;
        assert_eq!(read(&mut socket).await.0, b'E');
        send(&mut socket, b'S', &[]).await;
        assert_eq!(read(&mut socket).await.0, b'Z');
        assert!(
            !simple(&mut socket, "SELECT 1")
                .await
                .iter()
                .any(|message| message.0 == b'E')
        );
    }
    send(&mut socket, b'Q', b"SELECT 1\0extra").await;
    assert_eq!(read(&mut socket).await.0, b'E');
    assert_eq!(read(&mut socket).await.0, b'Z');
    // Header announces a payload but never supplies it. Deadline is absolute, not per byte.
    socket.write_all(b"Q\0\0\0\x08").await.unwrap();
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(7), socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    fixture.finish().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wrong_cancel_key_does_not_cancel_another_session() {
    let fixture = Fixture::new().await;
    let holder = fixture.client().await;
    holder
        .batch_execute("CREATE TABLE items (id BIGINT PRIMARY KEY)")
        .await
        .unwrap();
    holder.batch_execute("BEGIN").await.unwrap();
    let (mut socket, pid, secret) = raw_client(&fixture).await;
    send(&mut socket, b'Q', b"INSERT INTO items (id) VALUES (1)\0").await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    for (key, should_cancel) in [(secret.wrapping_add(1), false), (secret, true)] {
        let mut cancel = TcpStream::connect(fixture.addr).await.unwrap();
        let mut bytes = 16i32.to_be_bytes().to_vec();
        bytes.extend_from_slice(&80877102i32.to_be_bytes());
        bytes.extend_from_slice(&pid.to_be_bytes());
        bytes.extend_from_slice(&key.to_be_bytes());
        cancel.write_all(&bytes).await.unwrap();
        drop(cancel);
        if !should_cancel {
            assert!(
                tokio::time::timeout(Duration::from_millis(100), read(&mut socket))
                    .await
                    .is_err()
            );
        }
    }
    let (tag, error) = read(&mut socket).await;
    assert_eq!(tag, b'E');
    assert!(error.windows(5).any(|bytes| bytes == b"57014"));
    assert_eq!(read(&mut socket).await.0, b'Z');
    holder.batch_execute("ROLLBACK").await.unwrap();
    drop(holder);
    drop(socket);
    fixture.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_work_with_binary_and_simple_protocol_without_transaction_lock() {
    let fixture = Fixture::new().await;
    let writer = fixture.client().await;
    let observer = fixture.client().await;
    writer
        .batch_execute("CREATE TABLE metrics_test (id BIGINT PRIMARY KEY, name TEXT)")
        .await
        .unwrap();
    writer.batch_execute("BEGIN").await.unwrap();
    writer
        .batch_execute(
            "INSERT INTO metrics_test (id,name) VALUES (1,'secret SQL must not enter metrics')",
        )
        .await
        .unwrap();
    let row = tokio::time::timeout(
        Duration::from_secs(3),
        observer.query_one("SHOW KEDR METRICS", &[]),
    )
    .await
    .unwrap()
    .unwrap();
    let text: String = row.get(0);
    assert!(!text.contains("secret"));
    assert!(!text.contains("metrics_test"));
    let metrics: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(metrics["locks"]["writer"].as_bool().unwrap());
    assert!(metrics["storage"]["wal_write_bytes"].as_u64().unwrap() > 0);
    assert!(metrics["storage"]["fsync_calls"].as_u64().unwrap() > 0);
    assert!(metrics["active_sessions"].as_u64().unwrap() >= 2);
    assert!(metrics["queries"].as_u64().unwrap() >= 3);
    writer.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        observer
            .batch_execute("SELECT no_such_column FROM metrics_test")
            .await
            .is_err()
    );
    let row = observer.query_one("SHOW KEDR METRICS", &[]).await.unwrap();
    let metrics: serde_json::Value =
        serde_json::from_str(row.get::<_, String>(0).as_str()).unwrap();
    assert!(metrics["query_errors"].as_u64().unwrap() > 0);
    assert!(
        !observer
            .simple_query("SHOW KEDR METRICS")
            .await
            .unwrap()
            .is_empty()
    );
    drop(writer);
    drop(observer);
    fixture.finish().await;
}
