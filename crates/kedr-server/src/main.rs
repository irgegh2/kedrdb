// SPDX-License-Identifier: Apache-2.0
// SPDX-License-Identifier: Apache-2.0
use kedr_server::{Config, Server, credentials::Credential};
use std::{
    io::{self, IsTerminal, Read},
    path::PathBuf,
};
use zeroize::Zeroizing;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!(
            "{}",
            serde_json::json!({"event":"startup_or_server_error","kind":"operation_failed"})
        );
        eprintln!("kedr-server: {error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.first().is_some_and(|arg| arg == "init-auth") && args.len() == 3 {
        let password = Zeroizing::new(if io::stdin().is_terminal() {
            rpassword::prompt_password("Password (8..1024 UTF-8 bytes): ")?
        } else {
            let mut bytes = Zeroizing::new(Vec::new());
            io::stdin().take(1027).read_to_end(&mut bytes)?;
            if bytes.len() > 1026 {
                return Err("password input exceeds limit".into());
            }
            let text = std::str::from_utf8(&bytes)?;
            text.strip_suffix('\n')
                .unwrap_or(text)
                .strip_suffix('\r')
                .unwrap_or(text.strip_suffix('\n').unwrap_or(text))
                .to_owned()
        });
        Credential::new(&args[2], &password)?.write_new(std::path::Path::new(&args[1]))?;
        println!("Created private SCRAM verifier file.");
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "serve") {
        let mut data = None;
        let mut auth = None;
        let mut listen = "127.0.0.1:55432".to_owned();
        let mut cert = None;
        let mut key = None;
        if (args.len() - 1) % 2 != 0 {
            return Err("options require values".into());
        }
        for pair in args[1..].chunks_exact(2) {
            match pair[0].as_str() {
                "--data" => data = Some(PathBuf::from(&pair[1])),
                "--auth" => auth = Some(PathBuf::from(&pair[1])),
                "--listen" => listen = pair[1].clone(),
                "--tls-cert" => cert = Some(PathBuf::from(&pair[1])),
                "--tls-key" => key = Some(PathBuf::from(&pair[1])),
                _ => return Err("unknown option".into()),
            }
        }
        let server = Server::bind(Config {
            data: data.ok_or("--data is required")?,
            auth: auth.ok_or("--auth is required")?,
            listen: listen.parse()?,
            tls_cert: cert,
            tls_key: key,
        })
        .await?;
        println!(
            "KedrDB {} listening on {}",
            env!("CARGO_PKG_VERSION"),
            server.local_addr()?
        );
        let (shutdown, receiver) = tokio::sync::watch::channel(false);
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    Ok(mut terminate) => {
                        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
                    }
                    Err(_) => {
                        let _ = tokio::signal::ctrl_c().await;
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = tokio::signal::ctrl_c().await;
            }
            let _ = shutdown.send(true);
        });
        server.run(receiver).await?;
        return Ok(());
    }
    Err("usage: kedr-server init-auth FILE USER | serve --data DIR --auth FILE [--listen 127.0.0.1:55432] [--tls-cert PEM --tls-key PEM]".into())
}
