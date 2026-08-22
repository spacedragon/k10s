//! Native desktop bootstrap and embedded-server lifecycle.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use k10s_backend::{BackendKernel, FakeKubernetes};
use k10s_server::ServerConfig;
use k10s_ui::client::{ConnectTarget, TransportError};
use k10s_ui::{AppView, K10sApp};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Failure to start or stop the embedded runtime.
#[derive(Debug)]
pub enum EmbeddedServerError {
    /// Runtime, listener, or server I/O failed.
    Io(io::Error),
    /// OS randomness was unavailable.
    Randomness(getrandom::Error),
    /// The runtime thread did not report readiness in time.
    StartupTimeout,
    /// The runtime exited without reporting startup status.
    StartupChannelClosed,
    /// The runtime thread panicked.
    ThreadPanicked,
    /// Shutdown was already completed.
    AlreadyShutdown,
}

impl std::fmt::Display for EmbeddedServerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "embedded server I/O failed: {error}"),
            Self::Randomness(_) => formatter.write_str("OS randomness was unavailable"),
            Self::StartupTimeout => formatter.write_str("embedded server startup timed out"),
            Self::StartupChannelClosed => {
                formatter.write_str("embedded server exited before reporting readiness")
            }
            Self::ThreadPanicked => formatter.write_str("embedded server thread panicked"),
            Self::AlreadyShutdown => formatter.write_str("embedded server is already shut down"),
        }
    }
}

impl std::error::Error for EmbeddedServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Randomness(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for EmbeddedServerError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Owning handle for the dedicated embedded-server thread.
pub struct EmbeddedServerHandle {
    addr: SocketAddr,
    control_url: String,
    access_token: String,
    cancel: CancellationToken,
    thread: Option<JoinHandle<io::Result<()>>>,
}

impl std::fmt::Debug for EmbeddedServerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedServerHandle")
            .field("addr", &self.addr)
            .field("control_url", &self.control_url)
            .field("access_token", &"[REDACTED]")
            .field("running", &self.thread.is_some())
            .finish()
    }
}

impl EmbeddedServerHandle {
    /// Bound IPv4 loopback address.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Exact credential-free control WebSocket endpoint.
    #[must_use]
    pub fn control_url(&self) -> &str {
        &self.control_url
    }

    /// Launch credential sent only in the protocol `Hello` frame.
    #[must_use]
    pub fn access_token(&self) -> &str {
        &self.access_token
    }

    /// Cancel the server, gracefully finish connections, and join its OS thread.
    pub fn shutdown(&mut self) -> Result<(), EmbeddedServerError> {
        let thread = self
            .thread
            .take()
            .ok_or(EmbeddedServerError::AlreadyShutdown)?;
        self.cancel.cancel();
        thread
            .join()
            .map_err(|_| EmbeddedServerError::ThreadPanicked)??;
        Ok(())
    }
}

impl Drop for EmbeddedServerHandle {
    fn drop(&mut self) {
        if self.thread.is_some() {
            let _ = self.shutdown();
        }
    }
}

/// Native window owner that keeps the shared app and embedded server alive together.
pub struct DesktopApp {
    app: Option<K10sApp>,
    server: Option<EmbeddedServerHandle>,
}

impl std::fmt::Debug for DesktopApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DesktopApp")
            .field("app", &self.app)
            .field("server", &self.server)
            .finish()
    }
}

impl DesktopApp {
    /// Launch the server to readiness before constructing the protocol UI.
    pub fn launch() -> Result<Self, DesktopLaunchError> {
        let server = launch_embedded_server()?;
        let target = ConnectTarget::new(server.control_url(), server.access_token());
        let app = K10sApp::connect(target)?;
        Ok(Self {
            app: Some(app),
            server: Some(server),
        })
    }

    /// Bound address exposed for lifecycle verification.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.server
            .as_ref()
            .expect("desktop server exists until drop")
            .local_addr()
    }
}

impl eframe::App for DesktopApp {
    fn logic(&mut self, context: &eframe::egui::Context, _: &mut eframe::Frame) {
        let Some(app) = self.app.as_mut() else {
            return;
        };
        app.poll();
        context.request_repaint_after(Duration::from_millis(16));
    }

    fn ui(&mut self, ui: &mut eframe::egui::Ui, _: &mut eframe::Frame) {
        let Some(app) = self.app.as_ref() else {
            return;
        };
        let view = app.view().clone();
        eframe::egui::Frame::central_panel(ui.style())
            .fill(eframe::egui::Color32::from_rgb(242, 247, 252))
            .inner_margin(32.0)
            .show(ui, |ui| {
                ui.heading(
                    eframe::egui::RichText::new("k10s")
                        .size(30.0)
                        .strong()
                        .color(eframe::egui::Color32::from_rgb(32, 94, 166)),
                );
                ui.add_space(20.0);
                match view {
                    AppView::Connecting => {
                        ui.horizontal(|ui| {
                            ui.spinner();
                            ui.label("Connecting to the local control plane…");
                        });
                    }
                    AppView::Ready {
                        server_instance_id,
                        context_names,
                    } => {
                        ui.label(
                            eframe::egui::RichText::new("LOCAL CONTROL PLANE")
                                .small()
                                .strong()
                                .color(eframe::egui::Color32::from_rgb(73, 91, 109)),
                        );
                        ui.monospace(server_instance_id);
                        ui.add_space(24.0);
                        ui.heading("Kubernetes contexts");
                        for context_name in context_names {
                            ui.label(format!("• {context_name}"));
                        }
                    }
                    AppView::Failed { message } => {
                        ui.colored_label(
                            eframe::egui::Color32::from_rgb(178, 45, 45),
                            "Connection failed",
                        );
                        ui.label(message);
                    }
                }
            });
    }
}

impl Drop for DesktopApp {
    fn drop(&mut self) {
        drop(self.app.take());
        if let Some(mut server) = self.server.take() {
            let _ = server.shutdown();
        }
    }
}

/// Failure to construct the native desktop owner.
#[derive(Debug)]
pub enum DesktopLaunchError {
    /// Embedded server startup failed.
    Server(EmbeddedServerError),
    /// Shared WebSocket transport startup failed.
    Transport(TransportError),
}

impl std::fmt::Display for DesktopLaunchError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Server(error) => error.fmt(formatter),
            Self::Transport(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for DesktopLaunchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Server(error) => Some(error),
            Self::Transport(error) => Some(error),
        }
    }
}

impl From<EmbeddedServerError> for DesktopLaunchError {
    fn from(error: EmbeddedServerError) -> Self {
        Self::Server(error)
    }
}

impl From<TransportError> for DesktopLaunchError {
    fn from(error: TransportError) -> Self {
        Self::Transport(error)
    }
}

/// Start the fake-backed server on a dedicated OS thread and wait for readiness.
pub fn launch_embedded_server() -> Result<EmbeddedServerHandle, EmbeddedServerError> {
    launch_embedded_server_on(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)))
}

fn launch_embedded_server_on(
    bind_addr: SocketAddr,
) -> Result<EmbeddedServerHandle, EmbeddedServerError> {
    let mut token_bytes = [0_u8; 32];
    getrandom::fill(&mut token_bytes).map_err(EmbeddedServerError::Randomness)?;
    let access_token = URL_SAFE_NO_PAD.encode(token_bytes);
    let cancel = CancellationToken::new();
    let thread_cancel = cancel.clone();
    let thread_token = access_token.clone();
    let (ready_sender, ready_receiver) = mpsc::sync_channel(1);

    let thread = thread::Builder::new()
        .name("k10s-embedded-server".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let startup_error = io::Error::new(error.kind(), error.to_string());
                    let _ = ready_sender.send(Err(startup_error));
                    return Err(error);
                }
            };
            runtime.block_on(async move {
                let listener = match TcpListener::bind(bind_addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        let startup_error = io::Error::new(error.kind(), error.to_string());
                        let _ = ready_sender.send(Err(startup_error));
                        return Err(error);
                    }
                };
                let addr = listener.local_addr()?;
                if ready_sender.send(Ok(addr)).is_err() {
                    return Ok(());
                }
                let config = ServerConfig {
                    access_token: thread_token,
                    ..ServerConfig::default()
                };
                k10s_server::run(
                    listener,
                    config,
                    BackendKernel::new(FakeKubernetes::standard()),
                    thread_cancel,
                )
                .await
            })
        })?;

    let addr = match ready_receiver.recv_timeout(STARTUP_TIMEOUT) {
        Ok(Ok(addr)) => addr,
        Ok(Err(error)) => {
            let _ = thread.join();
            return Err(EmbeddedServerError::Io(error));
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            cancel.cancel();
            let _ = thread.join();
            return Err(EmbeddedServerError::StartupTimeout);
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            let _ = thread.join();
            return Err(EmbeddedServerError::StartupChannelClosed);
        }
    };
    let control_url = format!("ws://{addr}{}", k10s_protocol::CONTROL_PATH);
    Ok(EmbeddedServerHandle {
        addr,
        control_url,
        access_token,
        cancel,
        thread: Some(thread),
    })
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, TcpListener};

    use super::{EmbeddedServerError, launch_embedded_server_on};

    #[test]
    fn listener_startup_error_is_delivered_to_the_launcher() {
        let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = occupied.local_addr().unwrap();

        let error = launch_embedded_server_on(addr).unwrap_err();

        assert!(matches!(error, EmbeddedServerError::Io(_)));
    }
}
