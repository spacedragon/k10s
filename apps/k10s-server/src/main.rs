use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use k10s_backend::build_kernel;
use k10s_server::{ServerConfig, StandaloneConfig, resolve_backend_mode};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_DIST_DIR: &str = "dist";

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .init();
}

/// Cancel the runtime on SIGINT/SIGTERM so `run_with_assets` drains in order.
async fn forward_termination_signals(cancel: CancellationToken) {
    let interrupt = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = interrupt => {}
        () = terminate => {}
    }
    tracing::info!(target: "k10s_server_app", "termination signal received");
    cancel.cancel();
}

/// Parse development CLI flags. `--fake` is the only way to select fake
/// mode; normal launches default to the real kube-rs adapter.
#[derive(Debug, Default, PartialEq, Eq)]
struct CliOptions {
    fake_requested: bool,
    kubeconfig_path: Option<PathBuf>,
    token_file_path: Option<PathBuf>,
    listen: Option<SocketAddr>,
}

fn parse_backend_flags(args: &[String]) -> Result<CliOptions, Box<dyn Error>> {
    let mut options = CliOptions::default();
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "--fake" => options.fake_requested = true,
            "--kubeconfig" => {
                let path = iter
                    .next()
                    .map(PathBuf::from)
                    .ok_or("missing value for --kubeconfig")?;
                options.kubeconfig_path = Some(path);
            }
            "--token-file" => {
                let path = iter
                    .next()
                    .map(PathBuf::from)
                    .ok_or("missing value for --token-file")?;
                options.token_file_path = Some(path);
            }
            "--listen" => {
                let value = iter.next().ok_or("missing value for --listen")?;
                options.listen = Some(value.parse()?);
            }
            other => return Err(format!("unknown argument: {other}").into()),
        }
    }
    Ok(options)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    init_tracing();
    let args: Vec<String> = env::args().skip(1).collect();
    let cli = parse_backend_flags(&args)?;
    let bind_addr: SocketAddr = match cli.listen {
        Some(bind_addr) => bind_addr,
        None => env::var("K10S_BIND_ADDR")
            .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_owned())
            .parse()?,
    };
    // Documented precedence (see README security section): an explicitly
    // configured token file always wins over the environment value.
    let access_token_env = env::var("K10S_ACCESS_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let token_file_raw = env::var("K10S_ACCESS_TOKEN_FILE").unwrap_or_default();
    let token_file_path = cli
        .token_file_path
        .or_else(|| (!token_file_raw.trim().is_empty()).then(|| PathBuf::from(token_file_raw)));
    let access_token =
        k10s_server::resolve_access_token(access_token_env.as_deref(), token_file_path.as_deref())?;
    let dist_dir =
        PathBuf::from(env::var("K10S_DIST_DIR").unwrap_or_else(|_| DEFAULT_DIST_DIR.to_owned()));

    // Security-sensitive validation and asset checks intentionally precede bind.
    let standalone = StandaloneConfig::new(bind_addr, access_token, dist_dir)?;

    // Backend mode is resolved and validated through the same factory the
    // desktop app uses; a broken kubeconfig fails startup instead of falling
    // back to fake data.
    let backend_mode = resolve_backend_mode(cli.fake_requested, cli.kubeconfig_path.as_deref());
    let kernel = build_kernel(&backend_mode)?;

    if !standalone.dist_dir().join("index.html").is_file() {
        return Err("Trunk distribution is missing index.html".into());
    }

    let listener = TcpListener::bind(standalone.bind_addr()).await?;
    let config = ServerConfig {
        access_token: standalone.access_token().to_owned(),
        ..ServerConfig::default()
    };
    let cancel = CancellationToken::new();
    tokio::spawn(forward_termination_signals(cancel.clone()));
    k10s_server::run_with_assets(
        listener,
        config,
        kernel,
        cancel,
        Some(standalone.dist_dir().to_owned()),
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<String> {
        values.iter().map(|value| (*value).to_owned()).collect()
    }

    #[test]
    fn parses_browser_e2e_launch_flags() {
        let parsed = parse_backend_flags(&args(&[
            "--fake",
            "--token-file",
            "tests/browser/token.txt",
            "--listen",
            "127.0.0.1:18080",
        ]))
        .unwrap();
        assert!(parsed.fake_requested);
        assert_eq!(
            parsed.token_file_path,
            Some(PathBuf::from("tests/browser/token.txt"))
        );
        assert_eq!(parsed.listen, Some("127.0.0.1:18080".parse().unwrap()));
    }

    #[test]
    fn rejects_missing_or_invalid_cli_values() {
        for invalid in [
            args(&["--token-file"]),
            args(&["--listen"]),
            args(&["--listen", "not-an-address"]),
            args(&["--unknown"]),
        ] {
            assert!(
                parse_backend_flags(&invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }
}
