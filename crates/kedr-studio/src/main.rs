// SPDX-License-Identifier: Apache-2.0
mod app;
mod backend;
mod files;
mod local;
mod network;

fn main() -> eframe::Result {
    let args = std::env::args().collect::<Vec<_>>();
    let preview_results = args.iter().any(|arg| arg == "--preview-results");
    let managed = !preview_results
        && !args
            .iter()
            .any(|arg| arg == "--no-local" || arg == "--connect-test");
    let local_root = args
        .windows(2)
        .find(|a| a[0] == "--local-data")
        .map(|a| Ok(std::path::PathBuf::from(&a[1])))
        .unwrap_or_else(local::default_root);
    #[cfg(feature = "screenshot")]
    let mut profile = test_profile();
    eframe::run_native(
        "Kedr Studio",
        eframe::NativeOptions {
            viewport: eframe::egui::ViewportBuilder::default()
                .with_inner_size([1180.0, 780.0])
                .with_min_inner_size([820.0, 600.0]),
            ..Default::default()
        },
        Box::new(move |cc| {
            let mut studio = app::Studio::new(cc, preview_results);
            if managed {
                studio.start_local(cc.egui_ctx.clone(), local_root);
            }
            #[cfg(feature = "screenshot")]
            if let Some(profile) = profile.take() {
                studio.connect_test(profile);
            }
            Ok(Box::new(studio))
        }),
    )
}

#[cfg(feature = "screenshot")]
fn test_profile() -> Option<backend::Profile> {
    use std::io::Read;
    let args = std::env::args().collect::<Vec<_>>();
    let port = args
        .windows(2)
        .find(|args| args[0] == "--connect-test")?
        .get(1)?
        .parse()
        .ok()?;
    let ca_file = args
        .windows(2)
        .find(|args| args[0] == "--test-ca")
        .map(|args| args[1].clone());
    let mut password = zeroize::Zeroizing::new(String::new());
    std::io::stdin()
        .take(1027)
        .read_to_string(&mut password)
        .ok()?;
    if password.len() > 1026 {
        return None;
    }
    while password.ends_with(['\r', '\n']) {
        password.pop();
    }
    Some(backend::Profile {
        host: "127.0.0.1".into(),
        port,
        database: "kedrdb".into(),
        user: "admin".into(),
        password,
        ca_file,
    })
}
