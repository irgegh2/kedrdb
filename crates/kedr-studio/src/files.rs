// SPDX-License-Identifier: Apache-2.0
//! Bounded UTF-8 SQL documents. Atomic replacement preserves the previous file
//! on write/sync failure. Opening a document never executes its SQL.
use crate::backend::MAX_SQL_BYTES;
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
static NEXT: AtomicU64 = AtomicU64::new(0);
fn extension(path: &Path) -> Result<(), String> {
    if path
        .extension()
        .is_some_and(|e| e.eq_ignore_ascii_case("sql"))
    {
        Ok(())
    } else {
        Err("Выберите файл с расширением .sql.".into())
    }
}
pub fn read(path: &Path) -> Result<String, String> {
    extension(path)?;
    let metadata =
        fs::symlink_metadata(path).map_err(|e| format!("Не удалось открыть SQL-файл: {e}"))?;
    if !metadata.is_file() {
        return Err("SQL-файл должен быть обычным файлом, не ссылкой.".into());
    }
    let mut bytes = Vec::new();
    File::open(path)
        .and_then(|f| f.take((MAX_SQL_BYTES + 1) as u64).read_to_end(&mut bytes))
        .map_err(|e| format!("Не удалось прочитать SQL-файл: {e}"))?;
    if bytes.len() > MAX_SQL_BYTES {
        return Err("SQL-файл превышает лимит 64 КиБ. Откройте меньший документ.".into());
    }
    let text = String::from_utf8(bytes)
        .map_err(|_| "SQL-файл должен быть в кодировке UTF-8.".to_string())?;
    Ok(text.strip_prefix('\u{feff}').unwrap_or(&text).to_string())
}
pub fn save(path: &Path, text: &str) -> Result<(), String> {
    extension(path)?;
    if text.len() > MAX_SQL_BYTES {
        return Err("SQL-документ превышает лимит 64 КиБ.".into());
    }
    match fs::symlink_metadata(path) {
        Ok(m) if !m.is_file() => {
            return Err("Нельзя заменить каталог или ссылку SQL-файлом.".into());
        }
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
            return Err(format!("Не удалось проверить SQL-файл: {e}"));
        }
        _ => {}
    }
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let temp = parent.join(format!(
        ".kedr-sql-{}-{}.tmp",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    struct Pending(PathBuf);
    impl Drop for Pending {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }
    let _pending = Pending(temp.clone());
    let operation = || -> std::io::Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let mut f = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temp)?;
        f.write_all(text.as_bytes())?;
        f.sync_all()?;
        // Never follow a replaced symlink. Recheck before atomic pathname replacement.
        if fs::symlink_metadata(path).is_ok_and(|m| !m.is_file()) {
            return Err(std::io::Error::other(
                "target changed to a non-regular file",
            ));
        }
        fs::rename(&temp, path)?;
        // This editor promises atomic document replacement, not database commit
        // durability. Do not open the chosen file's parent directory: macOS file
        // picker grants access to the document, not permission to browse its folder.
        // Database backup/import has its own mandatory directory-sync protocol.
        Ok(())
    };
    operation().map_err(|e| {
        format!("Не удалось сохранить SQL-файл: {e}. Проверьте путь, права и свободное место.")
    })
}
pub enum Reply {
    Opened(PathBuf, String),
    Saved(PathBuf, String),
}
pub struct Task {
    pub receiver: std::sync::mpsc::Receiver<Result<Option<Reply>, String>>,
}
impl Task {
    pub fn open(ctx: eframe::egui::Context) -> Result<Self, String> {
        // Create the platform dialog on the UI thread; await and read off it.
        // Validate extension after selection: macOS type associations can reject
        // an otherwise valid .sql document in a filtered NSOpenPanel.
        let dialog = rfd::AsyncFileDialog::new()
            .set_title("Открыть SQL (.sql)")
            .pick_file();
        Self::spawn(ctx, async move {
            match dialog.await {
                Some(file) => {
                    let path = file.path().to_path_buf();
                    read(&path).map(|text| Some(Reply::Opened(path, text)))
                }
                None => Ok(None),
            }
        })
    }
    pub fn save(ctx: eframe::egui::Context, text: String, name: String) -> Result<Self, String> {
        let dialog = rfd::AsyncFileDialog::new()
            .set_title("Сохранить SQL")
            .add_filter("SQL", &["sql"])
            .set_file_name(name)
            .save_file();
        Self::spawn(ctx, async move {
            match dialog.await {
                Some(file) => {
                    let mut path = file.path().to_path_buf();
                    if path.extension().is_none() {
                        path.set_extension("sql");
                    }
                    save(&path, &text).map(|()| Some(Reply::Saved(path, text)))
                }
                None => Ok(None),
            }
        })
    }
    fn spawn(
        ctx: eframe::egui::Context,
        future: impl std::future::Future<Output = Result<Option<Reply>, String>> + Send + 'static,
    ) -> Result<Self, String> {
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name("kedr-studio-file".into())
            .spawn(move || {
                let result = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .map_err(|e| format!("Файловая операция недоступна: {e}"))
                    .and_then(|runtime| runtime.block_on(future));
                let _ = sender.send(result);
                ctx.request_repaint();
            })
            .map_err(|e| format!("Не удалось открыть диалог: {e}"))?;
        Ok(Self { receiver })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn file_roundtrip_is_bounded_utf8_and_preserves_prior_file_on_error() {
        let root = std::env::temp_dir().join(format!(
            "kedr-studio-files-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&root).unwrap();
        let path = root.join("shop.sql");
        let sql = "-- Кедр 🌲\nSELECT 'кавычки '' NULL;';\n";
        save(&path, sql).unwrap();
        assert_eq!(read(&path).unwrap(), sql);
        assert!(save(&path, &"x".repeat(MAX_SQL_BYTES + 1)).is_err());
        assert_eq!(read(&path).unwrap(), sql);
        fs::write(&path, [0xff]).unwrap();
        assert!(read(&path).is_err());
        fs::write(&path, vec![b'x'; MAX_SQL_BYTES + 1]).unwrap();
        assert!(read(&path).is_err());
        fs::write(&path, "\u{feff}SELECT NULL;\n").unwrap();
        assert_eq!(read(&path).unwrap(), "SELECT NULL;\n");
        let link = root.join("link.sql");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(save(&link, "DROP TABLE x").is_err());
        assert!(read(&link).is_err());
        assert!(read(&root.join("missing.sql")).is_err());
        assert!(save(&root.join("missing").join("file.sql"), sql).is_err());
        assert!(!fs::read_dir(&root).unwrap().any(|e| {
            e.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".kedr-sql")
        }));
        fs::remove_dir_all(root).unwrap();
    }
}
