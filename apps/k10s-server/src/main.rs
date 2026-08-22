use std::env;
use std::error::Error;
use std::net::SocketAddr;
use std::path::PathBuf;

use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_server::{ServerConfig, StandaloneConfig};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const DEFAULT_BIND_ADDR: &str = "127.0.0.1:8080";
const DEFAULT_DIST_DIR: &str = "dist";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    let bind_addr: SocketAddr = env::var("K10S_BIND_ADDR")
        .unwrap_or_else(|_| DEFAULT_BIND_ADDR.to_owned())
        .parse()?;
    let access_token = env::var("K10S_ACCESS_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let dist_dir =
        PathBuf::from(env::var("K10S_DIST_DIR").unwrap_or_else(|_| DEFAULT_DIST_DIR.to_owned()));

    // Security-sensitive validation and asset checks intentionally precede bind.
    let standalone = StandaloneConfig::new(bind_addr, access_token, dist_dir)?;
    if !standalone.dist_dir().join("index.html").is_file() {
        return Err("Trunk distribution is missing index.html".into());
    }

    let listener = TcpListener::bind(standalone.bind_addr()).await?;
    let config = ServerConfig {
        access_token: standalone.access_token().to_owned(),
        ..ServerConfig::default()
    };
    k10s_server::run_with_assets(
        listener,
        config,
        BackendKernel::new(FakeKubernetes::standard()),
        CancellationToken::new(),
        Some(standalone.dist_dir().to_owned()),
    )
    .await?;
    Ok(())
}
