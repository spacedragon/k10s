//! Native desktop bootstrap and embedded-server lifecycle.

use std::fs;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use k10s_backend::{AdapterError, BackendMode, build_kernel};
use k10s_server::ServerConfig;
use k10s_ui::K10sApp;
use k10s_ui::client::{ConnectTarget, TransportError};
use k10s_ui::workspace::{LoadedWorkspaceSnapshot, WorkspaceSnapshot};
use std::collections::BTreeMap;
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
    /// Workspace-state persistence for session restore; `None` when no
    /// writable config directory exists on this host.
    state_store: Option<StateStore>,
    context_state_store: Option<ContextStateStore>,
}

impl std::fmt::Debug for DesktopApp {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DesktopApp")
            .field("app", &self.app)
            .field("server", &self.server)
            .field(
                "state_store",
                &self.state_store.as_ref().map(StateStore::path),
            )
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
        Self::launch_with_mode_and_store(mode, StateStore::default_location())
    }

    /// Launch with an explicitly selected backend mode and one state store.
    /// Production launches pass the platform default; tests pass a store
    /// rooted in their own temp file so no host config is ever touched.
    fn launch_with_mode_and_store(
        mode: &BackendMode,
        state_store: Option<StateStore>,
    ) -> Result<Self, DesktopLaunchError> {
        let server = launch_embedded_server_with_mode(mode)?;
        let target = ConnectTarget::new(server.control_url(), server.access_token());
        let mut app = K10sApp::connect(target)?;
        // Restore the previous session's window layout; a missing, corrupt,
        // or version-mismatched state file simply yields first-launch
        // defaults and never blocks startup.
        let mut state_store = state_store;
        if let Some(store) = state_store.as_mut()
            && let Some(on_disk) = store.load()
        {
            // Record what the file actually holds. A restored workspace that
            // differs from it (mismatched version, normalized counters...)
            // is written back through the debounced save; an exact match
            // means relaunching never touches the file.
            if on_disk.migrated_from.is_none() {
                store.mark_loaded(&on_disk.snapshot);
            }
            app.restore_workspace_snapshot(on_disk.snapshot);
        }
        let mut context_state_store = state_store.as_ref().map(|store| {
            ContextStateStore::new(
                store
                    .path
                    .with_file_name("workspace-layouts-by-context.json"),
            )
        });
        if let Some(layouts) = context_state_store
            .as_mut()
            .and_then(ContextStateStore::load)
        {
            app.restore_workspace_layouts(layouts);
        }
        Ok(Self {
            app: Some(app),
            server: Some(server),
            state_store,
            context_state_store,
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

/// Context-keyed companion to the legacy single-layout file. Keeping the
/// legacy reader provides a migration fallback while new sessions restore
/// each Kubernetes context independently.
const CONTEXT_LAYOUTS_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
struct PersistedContextLayouts {
    version: u32,
    contexts: BTreeMap<String, WorkspaceSnapshot>,
}

#[derive(Debug)]
struct ContextStateStore {
    path: PathBuf,
    last_saved: Option<BTreeMap<String, WorkspaceSnapshot>>,
}

impl ContextStateStore {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            last_saved: None,
        }
    }

    fn load(&mut self) -> Option<BTreeMap<String, WorkspaceSnapshot>> {
        let persisted: PersistedContextLayouts =
            serde_json::from_str(&fs::read_to_string(&self.path).ok()?).ok()?;
        if persisted.version != CONTEXT_LAYOUTS_VERSION {
            return None;
        }
        self.last_saved = Some(persisted.contexts.clone());
        Some(persisted.contexts)
    }

    fn save(&mut self, layouts: &BTreeMap<String, WorkspaceSnapshot>) {
        if self.last_saved.as_ref() == Some(layouts) {
            return;
        }
        let persisted = PersistedContextLayouts {
            version: CONTEXT_LAYOUTS_VERSION,
            contexts: layouts.clone(),
        };
        match serde_json::to_string(&persisted)
            .map_err(io::Error::other)
            .and_then(|json| write_state_file(&self.path, &json))
        {
            Ok(()) => self.last_saved = Some(layouts.clone()),
            Err(error) => tracing::warn!("context workspace state save failed: {error}"),
        }
    }
}

/// How long a changed layout must stay stable before it is written. This
/// coalesces the continuous geometry updates of a window drag or resize into
/// one final write instead of touching the config file every frame.
const SAVE_DEBOUNCE: Duration = Duration::from_millis(300);

/// File-backed workspace-state persistence.
///
/// Stores one [`WorkspaceSnapshot`] as JSON in the platform config
/// directory. Saves are debounced and best-effort: a failure here must never
/// take down the desktop app, only the next restore falls back to
/// first-launch defaults.
pub struct StateStore {
    path: PathBuf,
    /// How long one layout must stay stable before being written; production
    /// uses [`SAVE_DEBOUNCE`], tests use a short interval.
    debounce: Duration,
    /// What we believe the file holds right now (last write, or what launch
    /// read back). Steady-state frames compare against this and stop there.
    last_saved: Option<WorkspaceSnapshot>,
    /// The layout waiting to be written, plus when that exact layout first
    /// appeared; `None` means nothing is queued behind the debounce timer.
    pending: Option<(Instant, WorkspaceSnapshot)>,
}

impl std::fmt::Debug for StateStore {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("StateStore")
            .field("path", &self.path)
            .finish()
    }
}

impl StateStore {
    /// The state file location on this host, or `None` when no config
    /// directory is resolvable (persistence stays disabled).
    pub fn default_location() -> Option<Self> {
        Some(Self::new(app_state_path()?, SAVE_DEBOUNCE))
    }

    #[cfg(test)]
    fn at(path: PathBuf) -> Self {
        // Short but real interval keeps timing tests fast and honest.
        Self::new(path, Duration::from_millis(16))
    }

    const fn new(path: PathBuf, debounce: Duration) -> Self {
        Self {
            path,
            debounce,
            last_saved: None,
            pending: None,
        }
    }

    /// State file location, exposed for tests and diagnostics.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the persisted snapshot; `None` when the file is absent, unreadable,
    /// or not valid JSON for this format.
    fn load(&self) -> Option<LoadedWorkspaceSnapshot> {
        let raw = fs::read_to_string(&self.path).ok()?;
        match serde_json::from_str::<LoadedWorkspaceSnapshot>(&raw) {
            Ok(snapshot) => Some(snapshot),
            Err(error) => {
                tracing::warn!("ignoring unreadable state file: {error}");
                None
            }
        }
    }

    /// Record what launch-time restore read from disk without writing. A
    /// restored workspace that differs from it (mismatched version, normalized
    /// counters...) is written back through the debounced save; an exact match
    /// means relaunching never touches the file.
    fn mark_loaded(&mut self, on_disk: &WorkspaceSnapshot) {
        self.last_saved = Some(on_disk.clone());
        self.pending = None;
    }

    /// Advance the debounced save with one frame's layout. A changed layout is
    /// written only once it has stayed stable for the whole debounce interval,
    /// so dragging or resizing windows coalesces into a single write; steady
    /// state costs one structural comparison and no I/O.
    fn tick(&mut self, snapshot: &WorkspaceSnapshot, now: Instant) {
        if let Some((since, queued)) = &self.pending {
            // Same layout already waiting: only the clock advances until it is stable.
            if *queued == *snapshot && now.duration_since(*since) >= self.debounce {
                self.write(snapshot);
                return;
            }
            // A newer layout supersedes whatever was waiting behind it.
            if *queued != *snapshot {
                self.pending = Some((now, snapshot.clone()));
            }
        } else if self.last_saved.as_ref() != Some(snapshot) {
            // Nothing queued yet: layouts already on disk cost nothing more.
            self.pending = Some((now, snapshot.clone()));
        }
    }

    /// Persist immediately regardless of the debounce timer; called at clean
    /// exit so the final layout is never lost behind a settling window.
    fn flush(&mut self, snapshot: &WorkspaceSnapshot) {
        if self.last_saved.as_ref() == Some(snapshot) {
            // The disk already holds it; drop any stale queued copy.
            self.pending = None;
            return;
        }
        self.write(snapshot);
    }

    /// Commit one layout through the atomic write. On failure the pending
    /// timer restarts so a broken disk rate-limits retries (one attempt per
    /// interval) instead of spamming; errors never propagate to the UI loop.
    fn write(&mut self, snapshot: &WorkspaceSnapshot) {
        let json = match serde_json::to_string(snapshot) {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!("workspace state serialization failed; dropping save: {error}");
                return;
            }
        };
        match write_state_file(&self.path, &json) {
            Ok(()) => {
                self.last_saved = Some(snapshot.clone());
                self.pending = None;
            }
            Err(error) => {
                tracing::warn!("workspace state save failed; retrying next interval: {error}");
                if let Some(pending) = self.pending.as_mut() {
                    pending.0 = Instant::now();
                }
            }
        }
    }
}

/// Write one state file atomically within its own directory.
fn write_state_file(path: &Path, json: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, json)?;
    fs::rename(&tmp, path)?;
    Ok(())
}

/// State file in the platform config directory for k10s; `None` when no
/// home or app-data location is resolvable (persistence stays disabled).
fn app_state_path() -> Option<PathBuf> {
    const STATE_FILE: &str = "workspace-state.json";

    let base_dir = if cfg!(windows) {
        // %APPDATA%\k10s; a missing APPDATA disables persistence cleanly.
        PathBuf::from(std::env::var_os("APPDATA")?).join("k10s")
    } else {
        let home = std::env::var_os("HOME").or_else(|| std::env::var_os("USERPROFILE"))?;
        if cfg!(target_os = "macos") {
            // ~/Library/Application Support/k10s
            PathBuf::from(home)
                .join("Library/Application Support")
                .join("k10s")
        } else {
            // XDG_CONFIG_HOME only counts when it is an absolute path.
            let config_dir = std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .filter(|dir| dir.is_absolute())
                .unwrap_or_else(|| PathBuf::from(home).join(".config"));
            config_dir.join("k10s")
        }
    };

    Some(base_dir.join(STATE_FILE))
}

impl eframe::App for DesktopApp {
    fn logic(&mut self, context: &eframe::egui::Context, _: &mut eframe::Frame) {
        let Some(app) = self.app.as_mut() else {
            return;
        };
        // Persist the window layout through the debounced save; geometry
        // updates land through queued commands during rendering, so a changed
        // layout is seen by this frame at most one repaint late and written
        // only once it stays stable for the debounce interval.
        if let Some(store) = self.state_store.as_mut() {
            store.tick(&app.workspace_snapshot(), Instant::now());
        }
        if let Some(store) = self.context_state_store.as_mut() {
            store.save(&app.workspace_layouts());
        }
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
        // Final save so a clean exit persists the last rendered layout even
        // while it was still settling behind the debounce timer.
        if let Some(app) = self.app.as_ref()
            && let Some(store) = self.state_store.as_mut()
        {
            store.flush(&app.workspace_snapshot());
        }
        if let Some(app) = self.app.as_ref()
            && let Some(store) = self.context_state_store.as_mut()
        {
            store.save(&app.workspace_layouts());
        }
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
    use std::path::PathBuf;
    use std::time::Instant;

    use k10s_backend::BackendMode;
    use k10s_ui::workspace::{
        LauncherItem, WindowGeom, WorkloadKind, WorkspaceCommand, WorkspaceState,
    };

    use super::{
        ContextStateStore, DesktopApp, EmbeddedServerError, StateStore, launch_embedded_server_on,
    };

    #[test]
    fn listener_startup_error_is_delivered_to_the_launcher() {
        let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = occupied.local_addr().unwrap();
        // Explicit fake mode keeps this transport-focused test independent of
        // any kubeconfig present on the host.
        let error = launch_embedded_server_on(addr, &BackendMode::Fake).unwrap_err();

        assert!(matches!(error, EmbeddedServerError::Io(_)));
    }

    /// Unique per-test state file inside the system temp dir; tests never
    /// touch the host's real config directory.
    fn tmp_state_file(test: &str) -> PathBuf {
        std::env::temp_dir().join(format!("k10s-state-test-{}-{test}", std::process::id()))
    }

    /// Identity stand-in so tests can exercise the pure workspace snapshot
    /// path without a protocol connection.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    struct Dummy;

    fn sample_state_json() -> String {
        let mut ws: WorkspaceState<Dummy> = WorkspaceState::new();
        // Open a Pods window and move/resize it like a user would.
        ws.apply(WorkspaceCommand::ActivateLauncherItem(
            LauncherItem::Workload(WorkloadKind::Pods),
        ));
        let pods = ws
            .windows()
            .iter()
            .find(|w| matches!(w.kind, k10s_ui::workspace::WindowKind::Workload(_)))
            .expect("pods window")
            .id;
        ws.apply(WorkspaceCommand::SetGeometry(
            pods,
            WindowGeom {
                position: [220.0, 140.0],
                size: [980.0, 660.0],
                collapsed: false,
            },
        ));
        serde_json::to_string(&ws.snapshot()).expect("serialize sample")
    }

    #[test]
    fn state_store_round_trips_a_snapshot() {
        let path = tmp_state_file("roundtrip");
        let mut store = StateStore::at(path.clone());

        // The flush path (clean exit) writes the file immediately.
        let snapshot: k10s_ui::workspace::WorkspaceSnapshot =
            serde_json::from_str(&sample_state_json()).expect("parse sample");
        store.flush(&snapshot);
        assert!(path.exists(), "state file must exist after save");

        // A fresh store at the same path reads it back unchanged.
        let reader = StateStore::at(path.clone());
        let loaded = reader.load().expect("load snapshot");
        assert_eq!(loaded.snapshot, snapshot);
        assert_eq!(loaded.migrated_from, None);
    }

    #[test]
    fn context_state_store_round_trips_independent_versioned_layouts() {
        let path = tmp_state_file("context-layouts");
        let snapshot: k10s_ui::workspace::WorkspaceSnapshot =
            serde_json::from_str(&sample_state_json()).expect("parse sample");
        let mut compact = snapshot.clone();
        compact.windows.truncate(1);
        let layouts = std::collections::BTreeMap::from([
            ("dev".to_owned(), snapshot.clone()),
            ("prod".to_owned(), compact.clone()),
        ]);

        ContextStateStore::new(path.clone()).save(&layouts);
        let raw: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("context state"))
                .expect("valid context state");
        assert_eq!(raw["version"], 1);
        assert_eq!(
            raw["contexts"]["dev"]["windows"].as_array().unwrap().len(),
            2
        );
        assert_eq!(
            raw["contexts"]["prod"]["windows"].as_array().unwrap().len(),
            1
        );

        assert_eq!(ContextStateStore::new(path).load(), Some(layouts));
    }

    #[test]
    fn context_state_store_ignores_unknown_versions_and_corrupt_files() {
        let path = tmp_state_file("context-layouts-invalid");
        std::fs::write(&path, r#"{"version":99,"contexts":{}}"#).unwrap();
        assert!(ContextStateStore::new(path.clone()).load().is_none());
        std::fs::write(&path, "not json").unwrap();
        assert!(ContextStateStore::new(path).load().is_none());
    }

    #[test]
    fn state_store_load_tolerates_missing_and_corrupt_files() {
        // Missing file: no snapshot, no panic.
        let missing = StateStore::at(tmp_state_file("missing"));
        assert!(missing.load().is_none());

        // Corrupt JSON: ignored with a warning, never an error.
        let corrupt_path = tmp_state_file("corrupt");
        std::fs::write(&corrupt_path, "this is { not json").unwrap();
        assert!(StateStore::at(corrupt_path).load().is_none());
    }

    #[test]
    fn steady_state_ticks_rewrite_nothing() {
        // Launch reads a file; every following frame sees an identical layout.
        // The debounced save must never touch the disk again (no mtime move).
        let path = tmp_state_file("steady-state");
        std::fs::write(&path, sample_state_json()).unwrap();

        let mut store = StateStore::at(path.clone());
        let on_disk: k10s_ui::workspace::WorkspaceSnapshot =
            serde_json::from_str(&std::fs::read_to_string(&path).expect("state file"))
                .expect("parse");
        store.mark_loaded(&on_disk);

        let first_write = std::fs::metadata(&path).and_then(|m| m.modified()).ok();
        for _ in 0..10 {
            // Frames arrive faster than the debounce interval.
            std::thread::sleep(std::time::Duration::from_millis(2));
            store.tick(&on_disk, Instant::now());
        }
        let second_write = std::fs::metadata(&path).and_then(|m| m.modified()).ok();

        assert_eq!(
            first_write, second_write,
            "steady state must not rewrite the file"
        );
    }

    #[test]
    fn migrated_v1_state_is_rewritten_as_v2_after_debounce() {
        let path = tmp_state_file("migrate-v1");
        let raw = r#"{"version":1,"next_id":2,"next_z":3,"windows":[{"kind":"overview","title":"Overview","geometry":{"position":[1.0,2.0],"size":[800.0,600.0],"collapsed":false},"z":1,"view":{"namespace":"prod","search":"web","filters":{},"sort":null,"split_ratio":0.4,"detail_visible":false,"custom_kind":null}}]}"#;
        std::fs::write(&path, raw).unwrap();
        let mut store = StateStore::at(path.clone());
        let loaded = store.load().expect("migrate v1");
        assert_eq!(loaded.migrated_from, Some(1));
        store.tick(&loaded.snapshot, Instant::now());
        std::thread::sleep(std::time::Duration::from_millis(25));
        store.tick(&loaded.snapshot, Instant::now());
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(json["version"], 2);
        assert_eq!(
            json["windows"][0]["view"]["namespace_scope"],
            serde_json::json!({"kind":"namespace","value":"prod"})
        );
        assert_eq!(json["windows"][0]["view"]["search"], "web");
    }

    #[test]
    fn debounced_save_coalesces_an_active_drag_into_one_final_write() {
        // Simulate an active drag: the layout changes on consecutive frames.
        // Nothing may hit disk until it has been stable for the debounce.
        let path = tmp_state_file("debounce-drag");
        let mut store = StateStore::at(path.clone());

        let final_geometry = WindowGeom {
            position: [64.0 + 12f32, 48.0],
            size: [840.0 + 12f32, 560.0],
            collapsed: false,
        };
        for frame in 0..12u32 {
            // A different layout every ~4ms, faster than the debounce.
            std::thread::sleep(std::time::Duration::from_millis(4));
            let mut ws: WorkspaceState<Dummy> = WorkspaceState::new();
            let geometry = WindowGeom {
                position: [64.0 + frame as f32, 48.0],
                size: [840.0 + frame as f32, 560.0],
                collapsed: false,
            };
            ws.apply(WorkspaceCommand::SetGeometry(ws.windows()[0].id, geometry));
            store.tick(&ws.snapshot(), Instant::now());
        }

        assert!(
            !path.exists(),
            "an unsettled layout must not be written yet"
        );

        // The drag ends on the final layout; once it stays stable past the
        // debounce interval, exactly one write lands.
        let mut settled_ws: WorkspaceState<Dummy> = WorkspaceState::new();
        settled_ws.apply(WorkspaceCommand::SetGeometry(
            settled_ws.windows()[0].id,
            final_geometry,
        ));
        let settled = settled_ws.snapshot();
        // First tick of the new layout restarts its stability window; once it
        // has stayed put past the debounce interval, exactly one write lands.
        std::thread::sleep(std::time::Duration::from_millis(40));
        store.tick(&settled, Instant::now());
        std::thread::sleep(std::time::Duration::from_millis(40));
        store.tick(&settled, Instant::now());

        assert!(
            path.exists(),
            "a stable layout must be written after the debounce"
        );
        let on_disk: k10s_ui::workspace::WorkspaceSnapshot =
            serde_json::from_str(&std::fs::read_to_string(path).expect("state file written"))
                .expect("parse persisted state");
        assert_eq!(on_disk, settled, "the final layout is what got written");
    }

    #[test]
    fn launch_restores_the_previous_window_layout() {
        let path = tmp_state_file("restore-launch");
        std::fs::write(&path, sample_state_json()).unwrap();

        // Explicit fake mode keeps the test independent of any kubeconfig.
        let desktop =
            DesktopApp::launch_with_mode_and_store(&BackendMode::Fake, Some(StateStore::at(path)))
                .expect("desktop launch with state restore");

        let workspace = desktop.app.as_ref().expect("app alive").workspace();
        // Both the persisted Overview and Pods windows are back.
        assert_eq!(workspace.windows().len(), 2);
        let pods = workspace
            .windows()
            .iter()
            .find(|w| matches!(w.kind, k10s_ui::workspace::WindowKind::Workload(_)))
            .expect("pods window restored");
        assert_eq!(pods.geometry.position, [220.0, 140.0]);
        assert_eq!(pods.geometry.size, [980.0, 660.0]);
    }

    #[test]
    fn corrupt_state_file_never_blocks_launch() {
        let path = tmp_state_file("corrupt-launch");
        std::fs::write(&path, "garbage state file").unwrap();

        let desktop =
            DesktopApp::launch_with_mode_and_store(&BackendMode::Fake, Some(StateStore::at(path)))
                .expect("corrupt state must not block launch");

        // First-launch defaults: Overview only.
        assert_eq!(
            desktop
                .app
                .as_ref()
                .expect("app alive")
                .workspace()
                .windows()
                .len(),
            1
        );
        drop(desktop);
    }

    #[test]
    fn clean_exit_persists_the_final_layout() {
        let path = tmp_state_file("exit-save");
        // Start with a persisted two-window layout.
        std::fs::write(&path, sample_state_json()).unwrap();

        let mut desktop = DesktopApp::launch_with_mode_and_store(
            &BackendMode::Fake,
            Some(StateStore::at(path.clone())),
        )
        .expect("desktop launch for exit-save test");

        // Change the layout after launch (open a Jobs window); per-frame and
        // drop-time saves must carry it to disk.
        desktop
            .app
            .as_mut()
            .expect("app alive")
            .web_activate_workload(WorkloadKind::Jobs);
        assert_eq!(
            desktop
                .app
                .as_ref()
                .expect("app alive")
                .workspace()
                .windows()
                .len(),
            3
        );

        // Dropping the app persists the final layout through Drop.
        drop(desktop);

        let on_disk: k10s_ui::workspace::WorkspaceSnapshot =
            serde_json::from_str(&std::fs::read_to_string(path).expect("state file written"))
                .expect("parse persisted state");
        assert_eq!(
            on_disk.windows.len(),
            3,
            "the final three-window layout is on disk"
        );
    }

    #[test]
    fn state_path_uses_the_k10s_config_directory() {
        let Some(path) = super::app_state_path() else {
            // Hosts without HOME/APPDATA legitimately disable persistence.
            return;
        };
        let file_name = path.file_name().and_then(|f| f.to_str());
        assert_eq!(file_name, Some("workspace-state.json"));
    }
}
