//! Native desktop bootstrap and embedded-server lifecycle.

pub mod external_shell;

use std::fs;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use k10s_backend::{AdapterError, BackendMode, prepare_backend};
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
    kubectl_launch: Option<external_shell::KubectlLaunchDescriptor>,
    kube_preparation: Option<k10s_backend::KubePreparation>,
    terminal_adapters: Vec<external_shell::TerminalAdapter>,
}

impl std::fmt::Debug for EmbeddedServerHandle {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddedServerHandle")
            .field("addr", &self.addr)
            .field("control_url", &self.control_url)
            .field("access_token", &"[REDACTED]")
            .field("running", &self.thread.is_some())
            .field("kubectl_launch_available", &self.kubectl_launch.is_some())
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

    /// Immutable kubectl reproduction snapshot, absent for fake or unsafe configurations.
    #[must_use]
    pub fn kubectl_launch_descriptor(&self) -> Option<&external_shell::KubectlLaunchDescriptor> {
        self.kubectl_launch.as_ref()
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
    external_shell_storage: Option<external_shell::TemporaryShellStorage>,
    external_shell_descriptor: Option<external_shell::KubectlLaunchDescriptor>,
    external_shell_generation: u64,
    kube_preparation: Option<k10s_backend::KubePreparation>,
    shell_environment: external_shell::EnvironmentSnapshot,
    terminal_adapters: Vec<external_shell::TerminalAdapter>,
    external_shell_error: Option<String>,
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
    fn advance_external_shell_generation(
        generation_slot: &mut u64,
        descriptor: &mut Option<external_shell::KubectlLaunchDescriptor>,
    ) -> Option<u64> {
        match generation_slot.checked_add(1) {
            Some(generation) => {
                *generation_slot = generation;
                Some(generation)
            }
            None => {
                *descriptor = None;
                None
            }
        }
    }
    fn clear_external_shell_status(app: &mut K10sApp, error_slot: &mut Option<String>) {
        *error_slot = None;
        app.clear_host_error();
    }
    fn apply_external_shell_result(
        app: &mut K10sApp,
        error_slot: &mut Option<String>,
        result: Result<(), external_shell::StorageError>,
    ) {
        match result {
            Ok(()) => Self::clear_external_shell_status(app, error_slot),
            Err(error) => {
                let sanitized = error.to_string();
                tracing::error!("{sanitized}");
                *error_slot = Some(sanitized.clone());
                app.set_host_error(k10s_ui::SafeUiError::new(format!(
                    "External shell failed: {sanitized}"
                )));
            }
        }
    }
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
        let storage_result = external_shell::TemporaryShellStorage::new(
            std::env::temp_dir().join("finback-external-shells"),
        );
        let storage_result = storage_result.and_then(|storage| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            storage.cleanup_expired(now)?;
            Ok(storage)
        });
        let initial_storage_error = storage_result.as_ref().err().map(ToString::to_string);
        let storage = storage_result.ok();
        let descriptor = storage
            .as_ref()
            .and_then(|_| server.kubectl_launch_descriptor().cloned());
        app.set_external_shell_availability(descriptor.as_ref().map_or(
            k10s_ui::ui::ExternalShellAvailability::Unavailable,
            |descriptor| k10s_ui::ui::ExternalShellAvailability::Available {
                generation: descriptor.generation,
            },
        ));
        if let Some(message) = &initial_storage_error {
            app.set_host_error(k10s_ui::SafeUiError::new(format!(
                "Shell storage unavailable: {message}"
            )));
        }
        let kube_preparation = server.kube_preparation.clone();
        let terminal_adapters = server.terminal_adapters.clone();
        let shell_environment = external_shell::EnvironmentSnapshot::capture();
        Ok(Self {
            app: Some(app),
            server: Some(server),
            state_store,
            context_state_store,
            external_shell_storage: storage,
            external_shell_descriptor: descriptor,
            external_shell_generation: 1,
            kube_preparation,
            shell_environment,
            terminal_adapters,
            external_shell_error: None,
        })
    }

    #[must_use]
    pub fn external_shell_error(&self) -> Option<&str> {
        self.external_shell_error.as_deref()
    }

    fn drain_external_shell(&mut self) {
        let Some(app) = self.app.as_mut() else {
            return;
        };
        let events = app.drain_app_events();
        let (connection_changed, rebuild_context) =
            events
                .into_iter()
                .fold((false, None), |_, event| match event {
                    k10s_ui::K10sAppEvent::CommittedContextChanged { context } => {
                        (true, Some(context))
                    }
                    k10s_ui::K10sAppEvent::ControlConnectionReestablished { context } => {
                        (true, context)
                    }
                });
        if connection_changed {
            app.set_external_shell_availability(
                k10s_ui::ui::ExternalShellAvailability::Unavailable,
            );
            Self::clear_external_shell_status(app, &mut self.external_shell_error);
            let Some(generation) = Self::advance_external_shell_generation(
                &mut self.external_shell_generation,
                &mut self.external_shell_descriptor,
            ) else {
                self.terminal_adapters.clear();
                return;
            };
            let preparation = rebuild_context.as_ref().and_then(|context| {
                self.kube_preparation
                    .as_ref()
                    .and_then(|value| value.for_context(context).ok())
            });
            let terminal = external_shell::probe_system_terminals(&self.shell_environment);
            let rebuilt = preparation
                .as_ref()
                .and_then(|value| {
                    external_shell::KubectlLaunchDescriptor::from_preparation(
                        generation,
                        value,
                        &self.shell_environment,
                    )
                    .ok()
                })
                .filter(|_| !terminal.is_empty());
            self.terminal_adapters = terminal;
            self.external_shell_descriptor = rebuilt;
            if let Some(descriptor) = &self.external_shell_descriptor {
                app.set_external_shell_availability(
                    k10s_ui::ui::ExternalShellAvailability::Available {
                        generation: descriptor.generation,
                    },
                );
            }
        }
        for requested in app.drain_external_shell_requests() {
            let Some(descriptor) = self
                .external_shell_descriptor
                .as_ref()
                .filter(|value| value.generation == requested.generation)
            else {
                app.set_host_error(k10s_ui::SafeUiError::new(
                    "External shell failed: request belongs to a stale connection generation",
                ));
                continue;
            };
            let target = external_shell::ExternalShellTarget {
                generation: requested.generation,
                namespace: requested.namespace,
                pod: requested.pod,
                uid: requested.uid,
                container: requested.container,
                program: requested.program,
            };
            let result = (|| {
                let storage = self
                    .external_shell_storage
                    .as_ref()
                    .ok_or(external_shell::StorageError::InvalidParent)?;
                let command = external_shell::KubectlExecCommand::new(descriptor, target)?;
                let script = storage.create(&command)?;
                external_shell::launch_with_adapters(&script, &self.terminal_adapters)
            })();
            Self::apply_external_shell_result(app, &mut self.external_shell_error, result);
        }
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

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
struct PersistedContextLayouts {
    version: u32,
    contexts: BTreeMap<String, WorkspaceSnapshot>,
    #[serde(skip)]
    migrated: bool,
}

impl<'de> serde::Deserialize<'de> for PersistedContextLayouts {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wire {
            version: u32,
            contexts: BTreeMap<String, LoadedWorkspaceSnapshot>,
        }

        let wire = Wire::deserialize(deserializer)?;
        let migrated = wire
            .contexts
            .values()
            .any(|snapshot| snapshot.migrated_from.is_some());
        Ok(Self {
            version: wire.version,
            contexts: wire
                .contexts
                .into_iter()
                .map(|(context, loaded)| (context, loaded.snapshot))
                .collect(),
            migrated,
        })
    }
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
        if !persisted.migrated {
            self.last_saved = Some(persisted.contexts.clone());
        }
        Some(persisted.contexts)
    }

    fn save(&mut self, layouts: &BTreeMap<String, WorkspaceSnapshot>) {
        if self.last_saved.as_ref() == Some(layouts) {
            return;
        }
        let persisted = PersistedContextLayouts {
            version: CONTEXT_LAYOUTS_VERSION,
            contexts: layouts.clone(),
            migrated: false,
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
        self.drain_external_shell();
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
    let prepared = prepare_backend(mode).map_err(EmbeddedServerError::Backend)?;
    let shell_environment = external_shell::EnvironmentSnapshot::capture();
    let kube_preparation = prepared.kube().cloned();
    let descriptor = prepared.kube().and_then(|kube| {
        external_shell::KubectlLaunchDescriptor::from_preparation(1, kube, &shell_environment).ok()
    });
    let terminal_adapters = external_shell::probe_system_terminals(&shell_environment);
    let kubectl_launch = external_shell::descriptor_when_terminal_available(
        descriptor,
        terminal_adapters.first().cloned(),
    );
    let kernel = prepared.into_kernel();
    let mut token_bytes = [0_u8; 32];
    getrandom::fill(&mut token_bytes).map_err(EmbeddedServerError::Randomness)?;
    let access_token = URL_SAFE_NO_PAD.encode(token_bytes);
    let cancel = CancellationToken::new();
    let thread_cancel = cancel.clone();
    let thread_token = access_token.clone();
    let (ready_sender, ready_receiver) =
        mpsc::sync_channel::<Result<SocketAddr, EmbeddedServerError>>(1);
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
                let config = embedded_server_config(thread_token);
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
        kubectl_launch,
        kube_preparation,
        terminal_adapters,
    })
}

fn embedded_server_config(access_token: String) -> ServerConfig {
    ServerConfig {
        access_token,
        capabilities: vec![
            "logs.tail".to_owned(),
            // Desktop-only: the embedded server owns loopback listeners;
            // standalone and web never advertise this.
            k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.to_owned(),
            k10s_protocol::CAPABILITY_POD_PORT_FORWARD.to_owned(),
        ],
        ..ServerConfig::default()
    }
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
        ContextStateStore, DesktopApp, EmbeddedServerError, StateStore, embedded_server_config,
        launch_embedded_server_on,
    };

    #[test]
    fn embedded_server_bootstrap_does_not_advertise_exec() {
        let config = embedded_server_config("test-token".to_owned());
        assert_eq!(
            config.capabilities,
            [
                "logs.tail".to_owned(),
                k10s_protocol::CAPABILITY_SERVICE_PORT_FORWARD.to_owned(),
            ]
        );
        assert!(
            !config
                .capabilities
                .iter()
                .any(|value| value == "exec.attach")
        );
    }

    #[test]
    fn shell_generation_is_not_reused_across_unavailable_contexts() {
        let mut desktop = DesktopApp::launch_with_mode_and_store(&BackendMode::Fake, None).unwrap();
        desktop.external_shell_descriptor = Some(crate::external_shell::KubectlLaunchDescriptor {
            generation: 1,
            kubectl: PathBuf::from("kubectl"),
            context: "first".to_owned(),
            kubeconfig_sources: vec![PathBuf::from("first-config")],
            kubeconfig_snapshot: Vec::new(),
            environment: Default::default(),
            exec_plugins: Vec::new(),
        });

        assert_eq!(
            DesktopApp::advance_external_shell_generation(
                &mut desktop.external_shell_generation,
                &mut desktop.external_shell_descriptor,
            ),
            Some(2)
        );
        desktop.external_shell_descriptor = None;
        assert_eq!(
            DesktopApp::advance_external_shell_generation(
                &mut desktop.external_shell_generation,
                &mut desktop.external_shell_descriptor,
            ),
            Some(3)
        );
        desktop.external_shell_descriptor = Some(crate::external_shell::KubectlLaunchDescriptor {
            generation: 3,
            kubectl: PathBuf::from("kubectl"),
            context: "third".to_owned(),
            kubeconfig_sources: vec![PathBuf::from("third-config")],
            kubeconfig_snapshot: Vec::new(),
            environment: Default::default(),
            exec_plugins: Vec::new(),
        });

        assert!(
            desktop
                .external_shell_descriptor
                .as_ref()
                .filter(|descriptor| descriptor.generation == 1)
                .is_none(),
            "a request from the first available context must stay stale"
        );

        desktop.external_shell_generation = u64::MAX;
        assert_eq!(
            DesktopApp::advance_external_shell_generation(
                &mut desktop.external_shell_generation,
                &mut desktop.external_shell_descriptor,
            ),
            None
        );
        assert!(desktop.external_shell_descriptor.is_none());
    }

    #[test]
    fn listener_startup_error_is_delivered_to_the_launcher() {
        let occupied = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        let addr = occupied.local_addr().unwrap();
        // Explicit fake mode keeps this transport-focused test independent of
        // any kubeconfig present on the host.
        let error = launch_embedded_server_on(addr, &BackendMode::Fake).unwrap_err();

        assert!(matches!(error, EmbeddedServerError::Io(_)));
    }

    #[test]
    fn external_shell_host_error_clears_after_success_and_context_transition() {
        let mut desktop = DesktopApp::launch_with_mode_and_store(&BackendMode::Fake, None).unwrap();
        let app = desktop.app.as_mut().unwrap();
        DesktopApp::apply_external_shell_result(
            app,
            &mut desktop.external_shell_error,
            Err(crate::external_shell::StorageError::NoTerminalLauncher),
        );
        assert!(desktop.external_shell_error.is_some());
        assert!(app.host_error().is_some());
        DesktopApp::apply_external_shell_result(app, &mut desktop.external_shell_error, Ok(()));
        assert!(desktop.external_shell_error.is_none());
        assert!(app.host_error().is_none());
        DesktopApp::apply_external_shell_result(
            app,
            &mut desktop.external_shell_error,
            Err(crate::external_shell::StorageError::NoTerminalLauncher),
        );
        DesktopApp::clear_external_shell_status(app, &mut desktop.external_shell_error);
        assert!(desktop.external_shell_error.is_none());
        assert!(app.host_error().is_none());
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
    fn state_store_round_trips_free_resize_true() {
        let path = tmp_state_file("roundtrip-free-resize");
        let mut workspace: WorkspaceState<Dummy> = WorkspaceState::new();
        workspace.apply(WorkspaceCommand::ToggleFreeWindowResizing);
        let mut store = StateStore::at(path.clone());
        store.flush(&workspace.snapshot());

        let loaded = StateStore::at(path).load().expect("load snapshot");
        assert!(loaded.snapshot.free_window_resizing);
        assert!(
            WorkspaceState::<Dummy>::from_snapshot(&loaded.snapshot)
                .unwrap()
                .free_window_resizing()
        );
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
    fn context_state_store_rewrites_migrated_nested_snapshots() {
        let path = tmp_state_file("context-layouts-migrate-v2");
        let snapshot = serde_json::json!({
            "version": 2,
            "next_id": 2,
            "next_z": 3,
            "windows": [{
                "kind": "overview",
                "title": "Overview",
                "geometry": {"position": [8.0, 9.0], "size": [801.0, 602.0], "collapsed": false},
                "z": 1,
                "view": null
            }]
        });
        std::fs::write(
            &path,
            serde_json::json!({"version": 1, "contexts": {"dev": snapshot}}).to_string(),
        )
        .unwrap();

        let mut store = ContextStateStore::new(path.clone());
        let layouts = store.load().expect("migrate nested v2 snapshot");
        store.save(&layouts);

        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(json["contexts"]["dev"]["version"], 3);
        assert_eq!(json["contexts"]["dev"]["free_window_resizing"], false);
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
    fn migrated_v1_state_is_rewritten_as_v3_after_debounce() {
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
        assert_eq!(json["version"], 3);
        assert_eq!(json["free_window_resizing"], false);
        assert_eq!(
            json["windows"][0]["geometry"]["position"],
            serde_json::json!([1.0, 2.0])
        );
        assert_eq!(
            json["windows"][0]["view"]["namespace_scope"],
            serde_json::json!({"kind":"namespace","value":"prod"})
        );
        assert_eq!(json["windows"][0]["view"]["search"], "web");
    }

    #[test]
    fn migrated_v2_state_is_rewritten_as_v3_after_debounce() {
        let path = tmp_state_file("migrate-v2");
        let raw = r#"{"version":2,"next_id":2,"next_z":3,"windows":[{"kind":"overview","title":"Overview","geometry":{"position":[8.0,9.0],"size":[801.0,602.0],"collapsed":true},"z":1,"view":{"namespace_scope":{"kind":"all_namespaces"},"search":"needle","filters":{"phase":"Running"},"sort":null,"split_ratio":0.4,"detail_visible":false,"custom_kind":null}}]}"#;
        std::fs::write(&path, raw).unwrap();
        let mut store = StateStore::at(path.clone());
        let loaded = store.load().expect("migrate v2");
        assert_eq!(loaded.migrated_from, Some(2));
        store.tick(&loaded.snapshot, Instant::now());
        std::thread::sleep(std::time::Duration::from_millis(25));
        store.tick(&loaded.snapshot, Instant::now());
        let json: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(json["version"], 3);
        assert_eq!(json["free_window_resizing"], false);
        assert_eq!(
            json["windows"][0]["geometry"]["position"],
            serde_json::json!([8.0, 9.0])
        );
        assert_eq!(
            json["windows"][0]["geometry"]["size"],
            serde_json::json!([801.0, 602.0])
        );
        assert_eq!(json["windows"][0]["view"]["search"], "needle");
        assert_eq!(json["windows"][0]["view"]["filters"]["phase"], "Running");
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
        let mut toggled = desktop
            .app
            .as_ref()
            .expect("app alive")
            .workspace_snapshot();
        toggled.free_window_resizing = !toggled.free_window_resizing;
        desktop
            .app
            .as_mut()
            .expect("app alive")
            .restore_workspace_snapshot(toggled);
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
        assert!(on_disk.free_window_resizing);
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
