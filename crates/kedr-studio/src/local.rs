// SPDX-License-Identifier: Apache-2.0
//! Managed local KedrDB. Data survives; the per-launch SCRAM secret stays in RAM.
use crate::backend::Profile;
use kedr_core::database::FileDatabase;
use kedr_server::{Server, credentials::Credential};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};
use tokio::sync::watch;

pub fn default_root() -> Result<PathBuf, String> {
    let home = std::env::var_os("HOME")
        .filter(|s| !s.is_empty())
        .ok_or("Не удалось найти домашний каталог для локальной базы.")?;
    #[cfg(target_os = "macos")]
    let root = PathBuf::from(home).join("Library/Application Support/KedrDB");
    #[cfg(not(target_os = "macos"))]
    let root = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
        .unwrap_or_else(|| PathBuf::from(home).join(".local/share"))
        .join("kedrdb");
    Ok(root)
}
fn failure(e: impl std::fmt::Display) -> String {
    format!(
        "Не удалось запустить локальную KedrDB: {e}. Данные сохранены; проверьте каталог базы или подключитесь к другому серверу."
    )
}
fn random<const N: usize>() -> io::Result<zeroize::Zeroizing<[u8; N]>> {
    let mut value = zeroize::Zeroizing::new([0; N]);
    File::open("/dev/urandom")?.read_exact(value.as_mut())?;
    Ok(value)
}
fn owner(root: &Path) -> Result<File, String> {
    // Never change permissions of an existing directory or follow its symlink.
    match fs::symlink_metadata(root) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(root)
                .map_err(failure)?;
        }
        Err(e) => return Err(failure(e)),
        Ok(_) => {}
    }
    let metadata = fs::symlink_metadata(root).map_err(failure)?;
    let uid = fs::metadata(std::env::var_os("HOME").ok_or("HOME не определен")?)
        .map_err(failure)?
        .uid();
    if !metadata.is_dir() || metadata.uid() != uid || metadata.permissions().mode() & 0o077 != 0 {
        return Err(
            "Каталог локальной KedrDB должен принадлежать вам, иметь права 0700 и не быть ссылкой."
                .into(),
        );
    }
    let path = root.join(".studio.lock");
    match fs::symlink_metadata(&path) {
        Ok(m) if !m.is_file() => return Err("Неверный файл владения локальной Studio.".into()),
        Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(failure(e)),
        _ => {}
    }
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .open(&path)
        .map_err(failure)?;
    let a = fs::symlink_metadata(path).map_err(failure)?;
    let b = lock.metadata().map_err(failure)?;
    if !a.is_file()
        || a.dev() != b.dev()
        || a.ino() != b.ino()
        || b.uid() != uid
        || b.permissions().mode() & 0o077 != 0
    {
        return Err("Небезопасный файл владения локальной Studio.".into());
    }
    lock.try_lock().map_err(|_| "Эта локальная база уже открыта другой Studio. Закройте другое окно или подключитесь к отдельному серверу.".to_string())?;
    Ok(lock)
}
pub struct Local {
    pub receiver: mpsc::Receiver<Result<Profile, String>>,
    stop: watch::Sender<bool>,
    thread: Option<thread::JoinHandle<()>>,
}
impl Local {
    pub fn start(root: PathBuf, notify: impl Fn() + Send + 'static) -> Result<Self, String> {
        let (sender, receiver) = mpsc::sync_channel(1);
        let (stop, shutdown) = watch::channel(false);
        let thread = thread::Builder::new()
            .name("kedr-studio-local".into())
            .spawn(move || {
                let result = (|| -> Result<(), String> {
                    let runtime = tokio::runtime::Builder::new_multi_thread()
                        .worker_threads(2)
                        .enable_all()
                        .build()
                        .map_err(failure)?;
                    let _owner = owner(&root)?;
                    let data = root.join("data");
                    match fs::symlink_metadata(&data) {
                        Err(e) if e.kind() == io::ErrorKind::NotFound => {
                            let id = *random::<16>().map_err(failure)?;
                            drop(
                                FileDatabase::create(&data, id, 16 * 1024 * 1024)
                                    .map_err(failure)?,
                            );
                        }
                        Ok(m) if !m.is_dir() => {
                            return Err(
                                "Каталог локальной базы не должен быть ссылкой или файлом.".into(),
                            );
                        }
                        Err(e) => return Err(failure(e)),
                        _ => {}
                    }
                    let secret = random::<32>().map_err(failure)?;
                    let password = zeroize::Zeroizing::new(
                        secret
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>(),
                    );
                    let credential = Credential::new("studio", &password).map_err(failure)?;
                    runtime.block_on(async {
                        let server = Server::bind_local(data, credential)
                            .await
                            .map_err(failure)?;
                        let profile = Profile {
                            host: "127.0.0.1".into(),
                            port: server.local_addr().map_err(failure)?.port(),
                            database: "kedrdb".into(),
                            user: "studio".into(),
                            password,
                            ca_file: None,
                        };
                        if sender.send(Ok(profile)).is_err() {
                            return Ok(());
                        }
                        notify();
                        server.run(shutdown).await.map_err(failure)
                    })
                })();
                if let Err(error) = result {
                    let _ = sender.try_send(Err(error));
                    notify();
                }
            })
            .map_err(failure)?;
        Ok(Self {
            receiver,
            stop,
            thread: Some(thread),
        })
    }
}
impl Drop for Local {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        backend::{Backend, Command, Payload},
        network::Network,
    };
    use std::sync::atomic::AtomicBool;
    #[test]
    fn managed_server_authenticates_reopens_rows_and_never_resets_busy_or_bad_data() {
        let root = std::env::temp_dir().join(format!("kedr-local-{}", std::process::id()));
        assert!(!root.exists());
        let first = Local::start(root.clone(), || {}).unwrap();
        let profile = first
            .receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
            .unwrap();
        let second = Local::start(root.clone(), || {}).unwrap();
        assert!(
            second
                .receiver
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap()
                .is_err()
        );
        drop(second);
        let cancel = AtomicBool::new(false);
        let mut client = Network::new().unwrap();
        let mut wrong = profile.clone();
        wrong.password = zeroize::Zeroizing::new("wrong-password".into());
        assert!(client.perform(Command::Connect(wrong), &cancel).is_err());
        client.perform(Command::Connect(profile), &cancel).unwrap();
        client
            .perform(
                Command::Query("CREATE TABLE persisted (id BIGINT PRIMARY KEY, name TEXT);".into()),
                &cancel,
            )
            .unwrap();
        client
            .perform(
                Command::Query("INSERT INTO persisted (id,name) VALUES (1,'сохранено');".into()),
                &cancel,
            )
            .unwrap();
        drop(client);
        drop(first);
        assert!(FileDatabase::open(root.join("data"), 65536).is_ok());
        let reopened = Local::start(root.clone(), || {}).unwrap();
        let profile = reopened
            .receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap()
            .unwrap();
        let mut client = Network::new().unwrap();
        client.perform(Command::Connect(profile), &cancel).unwrap();
        assert!(
            matches!(client.perform(Command::Query("SELECT name FROM persisted;".into()), &cancel).unwrap(),Payload::Rows(r) if r.rows.len()==1 && r.rows[0][0].display()=="сохранено")
        );
        drop(client);
        drop(reopened);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 2); // lock/data, no saved credential
        fs::write(root.join("data/.import-incomplete"), b"pending").unwrap();
        let incomplete = Local::start(root.clone(), || {}).unwrap();
        assert!(
            incomplete
                .receiver
                .recv_timeout(std::time::Duration::from_secs(10))
                .unwrap()
                .is_err()
        );
        drop(incomplete);
        assert!(root.join("data/.import-incomplete").exists());
        fs::remove_dir_all(root).unwrap();
    }
}
