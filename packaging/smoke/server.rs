use std::fs;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

struct ServerProcess(Child);

impl Drop for ServerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn probe(port: u16, path: &str) -> Option<(u16, String)> {
    let mut stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(1))).ok()?;
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    let (head, body) = response.split_once("\r\n\r\n")?;
    let status = head
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some((status, body.to_owned()))
}

fn wait_for(port: u16, path: &str, status: u16, deadline: Duration) -> String {
    let started = Instant::now();
    while started.elapsed() < deadline {
        if let Some((actual, body)) = probe(port, path)
            && actual == status
        {
            return body;
        }
        thread::sleep(Duration::from_millis(10));
    }
    panic!("{path} did not report {status}");
}

#[test]
fn packaged_server_bootstraps_assets_and_drains_probes() {
    if std::env::var_os("K10S_PACKAGING_SMOKE").is_none() {
        return;
    }
    let reserved = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    let port = reserved.local_addr().unwrap().port();
    drop(reserved);
    let scratch = std::env::temp_dir().join(format!("k10s-server-smoke-{}", std::process::id()));
    let _ = fs::remove_dir_all(&scratch);
    fs::create_dir_all(&scratch).unwrap();
    let token = scratch.join("token");
    let shutdown = scratch.join("shutdown");
    fs::write(&token, "packaging-smoke-token-32-bytes-minimum").unwrap();

    let binary = std::env::var_os("K10S_SERVER_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_k10s-server")));
    let child = Command::new(binary)
        .args([
            "--fake",
            "--token-file",
            token.to_str().unwrap(),
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--shutdown-file",
            shutdown.to_str().unwrap(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut server = ServerProcess(child);

    assert_eq!(
        wait_for(port, "/healthz", 200, Duration::from_secs(10)),
        "ok\n"
    );
    assert_eq!(
        wait_for(port, "/readyz", 503, Duration::from_secs(1)),
        "starting\n"
    );
    assert_eq!(
        wait_for(port, "/readyz", 200, Duration::from_secs(2)),
        "ready\n"
    );
    let index = wait_for(port, "/", 200, Duration::from_secs(1));
    assert!(index.contains("k10s-web-"));
    assert!(index.contains("_bg.wasm"));

    fs::write(&shutdown, "drain").unwrap();
    assert_eq!(
        wait_for(port, "/readyz", 503, Duration::from_secs(1)),
        "draining\n"
    );
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(5) && probe(port, "/healthz").is_some() {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(
        probe(port, "/healthz").is_none(),
        "health remained after exit"
    );
    assert!(server.0.wait().unwrap().success());
    fs::remove_dir_all(scratch).unwrap();
}
