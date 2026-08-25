//! Native desktop bootstrap and embedded-server lifecycle.

use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use k10s_backend::{AdapterError, BackendMode, build_kernel};
use k10s_server::ServerConfig;
use k10s_ui::K10sApp;
use k10s_ui::client::{ConnectTarget, TransportError};
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
    /// The backend factory rejected the selected adapter mode at startup;
    /// no implicit fake fallback may replace it.
    Backend(AdapterError),
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
            Self::Backend(error) => write!(formatter, "backend selection failed: {error}"),
        }
    }
}

impl std::error::Error for EmbeddedServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Randomness(error) => Some(error),
            Self::Backend(error) => Some(error),
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
    /// Normal desktop launch: real `Kube` adapter through standard kubeconfig
    /// discovery. Fake mode is never implicit on this production path.
    pub fn launch() -> Result<Self, DesktopLaunchError> {
        Self::launch_with_mode(&BackendMode::Kube { kubeconfig: None })
    }

    /// Launch with an explicitly selected backend mode through the shared
    /// runtime factory (tests and explicit development opt-in).
    pub fn launch_with_mode(mode: &BackendMode) -> Result<Self, DesktopLaunchError> {
        let server = launch_embedded_server_with_mode(mode)?;
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
        let Some(app) = self.app.as_mut() else {
            return;
        };
        app.render_ui(ui);
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

/// Start the normal-launch server on a dedicated OS thread and wait for
/// readiness. Normal launches use the real `Kube` adapter with standard
/// kubeconfig discovery; they fail cleanly when none is configured.
pub fn launch_embedded_server() -> Result<EmbeddedServerHandle, EmbeddedServerError> {
    launch_embedded_server_with_mode(&BackendMode::Kube { kubeconfig: None })
}

/// Start the embedded server with an explicitly selected backend mode through
/// the shared runtime factory and wait for readiness. Fake mode is only ever
/// reached this way (tests and explicit development opt-in), never implicitly.
pub fn launch_embedded_server_with_mode(
    mode: &BackendMode,
) -> Result<EmbeddedServerHandle, EmbeddedServerError> {
    launch_embedded_server_on(SocketAddr::from((Ipv4Addr::LOCALHOST, 0)), mode)
}

fn launch_embedded_server_on(
    bind_addr: SocketAddr,
    mode: &BackendMode,
) -> Result<EmbeddedServerHandle, EmbeddedServerError> {
    let mut token_bytes = [0_u8; 32];
    getrandom::fill(&mut token_bytes).map_err(EmbeddedServerError::Randomness)?;
    let access_token = URL_SAFE_NO_PAD.encode(token_bytes);
    let cancel = CancellationToken::new();
    let thread_cancel = cancel.clone();
    let thread_token = access_token.clone();
    let (ready_sender, ready_receiver) =
        mpsc::sync_channel::<Result<SocketAddr, EmbeddedServerError>>(1);
    let mode = mode.clone();

    let thread = thread::Builder::new()
        .name("k10s-embedded-server".to_owned())
        .spawn(move || {
            // Build the kernel through the shared factory before readiness is
            // reported: a broken kubeconfig must fail the launch, never fall
            // back to fake data.
            let kernel = match build_kernel(&mode) {
                Ok(kernel) => kernel,
                Err(error) => {
                    let _ = ready_sender.send(Err(EmbeddedServerError::Backend(error)));
                    return Ok(());
                }
            };
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let startup_error = io::Error::new(error.kind(), error.to_string());
                    let _ = ready_sender.send(Err(EmbeddedServerError::Io(startup_error)));
                    return Err(error);
                }
            };
            runtime.block_on(async move {
                let listener = match TcpListener::bind(bind_addr).await {
                    Ok(listener) => listener,
                    Err(error) => {
                        let startup_error = io::Error::new(error.kind(), error.to_string());
                        let _ = ready_sender.send(Err(EmbeddedServerError::Io(startup_error)));
                        return Err(error);
                    }
                };
                let addr = listener.local_addr()?;
                if ready_sender.send(Ok(addr)).is_err() {
                    return Ok(());
                }
                let config = ServerConfig {
                    access_token: thread_token,
                    capabilities: vec![
                        "logs.tail".to_owned(),
                        "exec.attach".to_owned(),
                        // Desktop-only: the embedded server owns loopback
                        // listeners; standalone and web never advertise this.
                        k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.to_owned(),
                    ],
                    ..ServerConfig::default()
                };
                k10s_server::run(listener, config, kernel, thread_cancel).await
            })
        })?;

    let addr = match ready_receiver.recv_timeout(STARTUP_TIMEOUT) {
        Ok(Ok(addr)) => addr,
        Ok(Err(error)) => {
            // Typed startup failures (I/O or backend selection) flow through
            // as-is; there is no implicit fake fallback.
            let _ = thread.join();
            return Err(error);
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

    use k10s_backend::BackendMode;

    use super::{EmbeddedServerError, launch_embedded_server_on};

    #[test]
    fn listener_startup_error_is_delivered_to_the_launcher() {
        let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = occupied.local_addr().unwrap();
        // Explicit fake mode keeps this transport-focused test independent of
        // any kubeconfig present on the host.
        let error = launch_embedded_server_on(addr, &BackendMode::Fake).unwrap_err();

        assert!(matches!(error, EmbeddedServerError::Io(_)));
    }
}
