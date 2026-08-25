use std::collections::HashSet;
use std::net::{IpAddr, Ipv4Addr, TcpStream};
use std::thread;
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use k10s_backend::BackendMode;
use k10s_desktop::{DesktopApp, launch_embedded_server_with_mode};
use k10s_protocol::CONTROL_PATH;
use k10s_ui::client::ConnectTarget;
use k10s_ui::{AppView, K10sApp};

#[test]
fn launches_on_random_loopback_with_a_fresh_csprng_token() {
    // Explicit fake mode: normal launches now default to the real Kube
    // adapter, so lifecycle tests opt into the offline dataset themselves.
    let mut first = launch_embedded_server_with_mode(&BackendMode::Fake)
        .expect("first embedded server must become ready");
    let mut second = launch_embedded_server_with_mode(&BackendMode::Fake)
        .expect("second embedded server must become ready");

    assert_eq!(first.local_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_eq!(second.local_addr().ip(), IpAddr::V4(Ipv4Addr::LOCALHOST));
    assert_ne!(first.local_addr().port(), 0);
    assert_ne!(first.local_addr().port(), second.local_addr().port());
    assert_eq!(
        first.control_url(),
        format!("ws://{}{}", first.local_addr(), CONTROL_PATH)
    );
    assert_eq!(
        second.control_url(),
        format!("ws://{}{}", second.local_addr(), CONTROL_PATH)
    );
    assert!(!first.control_url().contains(first.access_token()));

    let first_bytes = URL_SAFE_NO_PAD
        .decode(first.access_token())
        .expect("launch token must be unpadded URL-safe base64");
    let second_bytes = URL_SAFE_NO_PAD
        .decode(second.access_token())
        .expect("launch token must be unpadded URL-safe base64");
    assert_eq!(first_bytes.len(), 32);
    assert_eq!(second_bytes.len(), 32);
    assert_ne!(first.access_token(), second.access_token());
    assert!(!format!("{first:?}").contains(first.access_token()));

    first.shutdown().expect("first server must stop cleanly");
    second.shutdown().expect("second server must stop cleanly");
}

#[test]
fn app_bootstraps_over_the_exact_control_websocket() {
    let mut server = launch_embedded_server_with_mode(&BackendMode::Fake)
        .expect("embedded server must become ready");
    let target = ConnectTarget::new(server.control_url(), server.access_token());
    let mut app = K10sApp::connect(target).expect("shared websocket transport must start");

    assert_eq!(app.view(), &AppView::Connecting);
    assert_eq!(app.render_text(), "Connecting");
    let deadline = Instant::now() + Duration::from_secs(3);
    while matches!(app.view(), AppView::Connecting) && Instant::now() < deadline {
        app.poll();
        thread::sleep(Duration::from_millis(5));
    }

    let AppView::Ready {
        server_instance_id,
        context_names,
        ..
    } = app.view()
    else {
        panic!("app did not bootstrap: {:?}", app.view());
    };
    assert!(!server_instance_id.is_empty());
    assert_eq!(
        context_names,
        &["dev-local".to_owned(), "prod-readonly".to_owned()]
    );
    assert_eq!(app.connection_url(), server.control_url());
    assert!(app.connection_url().ends_with(CONTROL_PATH));
    let rendered = app.render_text();
    assert!(rendered.contains(server_instance_id));
    assert!(rendered.contains("dev-local"));
    assert!(rendered.contains("prod-readonly"));

    drop(app);
    server.shutdown().expect("server must stop cleanly");
}

#[test]
fn shutdown_joins_the_runtime_thread_and_closes_the_listener() {
    let mut server = launch_embedded_server_with_mode(&BackendMode::Fake)
        .expect("embedded server must become ready");
    let addr = server.local_addr();
    assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok());

    server
        .shutdown()
        .expect("shutdown must join the server thread");

    let deadline = Instant::now() + Duration::from_secs(1);
    while TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_ok()
        && Instant::now() < deadline
    {
        thread::sleep(Duration::from_millis(10));
    }
    assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err());
    assert!(
        server.shutdown().is_err(),
        "shutdown may only be called once"
    );
}

#[test]
fn shutdown_is_bounded_with_an_authenticated_connection() {
    let mut server = launch_embedded_server_with_mode(&BackendMode::Fake)
        .expect("embedded server must become ready");
    let target = ConnectTarget::new(server.control_url(), server.access_token());
    let mut app = K10sApp::connect(target).expect("shared websocket transport must start");
    let bootstrap_deadline = Instant::now() + Duration::from_secs(3);
    while matches!(app.view(), AppView::Connecting) && Instant::now() < bootstrap_deadline {
        app.poll();
        thread::sleep(Duration::from_millis(5));
    }
    assert!(matches!(app.view(), AppView::Ready { .. }));

    let started = Instant::now();
    server
        .shutdown()
        .expect("active connection must be joined gracefully");
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn many_consecutive_launches_have_unique_ports_and_tokens() {
    let mut servers = Vec::new();
    for _ in 0..8 {
        servers.push(
            launch_embedded_server_with_mode(&BackendMode::Fake)
                .expect("embedded server must become ready"),
        );
    }

    let ports: HashSet<_> = servers
        .iter()
        .map(|server| server.local_addr().port())
        .collect();
    let tokens: HashSet<_> = servers.iter().map(|server| server.access_token()).collect();
    assert_eq!(ports.len(), servers.len());
    assert_eq!(tokens.len(), servers.len());

    for server in &mut servers {
        server.shutdown().expect("server must stop cleanly");
    }
}

#[test]
fn desktop_window_owner_shuts_down_server_on_exit() {
    // Explicit fake mode: this test verifies drop-driven shutdown, not
    // kubeconfig discovery.
    let desktop = DesktopApp::launch_with_mode(&BackendMode::Fake)
        .expect("desktop owner must launch after server readiness");
    let addr = desktop.local_addr();
    assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok());

    drop(desktop);

    assert!(TcpStream::connect_timeout(&addr, Duration::from_millis(50)).is_err());
}
