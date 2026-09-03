//! Credential-free launch descriptions and pure shell-script rendering.
//!
//! It also owns the private temporary-script lifecycle and typed platform launch.

#[cfg(unix)]
mod unix;
#[cfg(windows)]
mod windows;

use std::collections::BTreeMap;
use std::ffi::OsString;
#[cfg(unix)]
use std::ffi::OsString as PlatformString;
use std::fmt;
use std::path::Path;
use std::path::PathBuf;

use k10s_backend::KubePreparation;

/// Immutable environment view used during descriptor preparation.
#[derive(Clone, Debug, Default)]
pub struct EnvironmentSnapshot(BTreeMap<String, OsString>);

impl EnvironmentSnapshot {
    #[must_use]
    pub fn from_os(values: BTreeMap<String, OsString>) -> Self {
        Self(values)
    }

    #[must_use]
    pub fn from_unicode(values: BTreeMap<String, String>) -> Self {
        Self(
            values
                .into_iter()
                .map(|(key, value)| (key, value.into()))
                .collect(),
        )
    }

    #[must_use]
    pub fn capture() -> Self {
        Self(
            std::env::vars_os()
                .filter_map(|(key, value)| key.into_string().ok().map(|key| (key, value)))
                .collect(),
        )
    }

    fn unicode(&self, key: &str) -> Result<Option<String>, DescriptorError> {
        self.0
            .get(key)
            .map(|value| {
                value
                    .clone()
                    .into_string()
                    .map_err(|_| DescriptorError::Unrepresentable)
            })
            .transpose()
    }
}

/// Resolved executable metadata for one kubeconfig exec credential plugin.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedExecPlugin {
    pub command: PathBuf,
}

/// Exact terminal executable and fixed arguments selected by a read-only probe.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TerminalAdapter {
    pub executable: PathBuf,
    pub arguments_before_script: Vec<String>,
}

/// Publish a prepared descriptor only when the current platform probe succeeded.
#[must_use]
pub fn descriptor_when_terminal_available(
    descriptor: Option<KubectlLaunchDescriptor>,
    terminal: Option<TerminalAdapter>,
) -> Option<KubectlLaunchDescriptor> {
    terminal.and(descriptor)
}

/// Probe terminal availability without opening a window (launching is Task 3).
pub fn probe_system_terminal(_environment: &EnvironmentSnapshot) -> Option<TerminalAdapter> {
    probe_system_terminals(_environment).into_iter().next()
}

pub fn probe_system_terminals(_environment: &EnvironmentSnapshot) -> Vec<TerminalAdapter> {
    #[cfg(target_os = "macos")]
    {
        executable(PathBuf::from("/usr/bin/open"))
            .map(|executable| TerminalAdapter {
                executable,
                arguments_before_script: Vec::new(),
            })
            .into_iter()
            .collect()
    }
    #[cfg(target_os = "windows")]
    {
        let Some(path) = _environment.unicode("PATH").ok().flatten() else {
            return Vec::new();
        };
        resolve_executable(
            "powershell.exe",
            &path,
            _environment.unicode("PATHEXT").ok().flatten().as_deref(),
        )
        .ok()
        .map(|executable| TerminalAdapter {
            executable,
            arguments_before_script: vec![
                "-NoLogo".into(),
                "-NoProfile".into(),
                "-ExecutionPolicy".into(),
                "Bypass".into(),
                "-File".into(),
            ],
        })
        .into_iter()
        .collect()
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let Some(path) = _environment.unicode("PATH").ok().flatten() else {
            return Vec::new();
        };
        let mut adapters = Vec::new();
        for (name, arguments) in [
            ("xdg-terminal-exec", vec!["--"]),
            ("x-terminal-emulator", vec!["-e"]),
            ("gnome-terminal", vec!["--"]),
            ("konsole", vec!["-e"]),
            ("kitty", vec!["--"]),
        ] {
            if let Ok(executable) = resolve_executable(name, &path, None) {
                adapters.push(TerminalAdapter {
                    executable,
                    arguments_before_script: arguments.into_iter().map(str::to_owned).collect(),
                });
            }
        }
        adapters
    }
}

/// Immutable kubectl inputs captured from the same kube preparation as the server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KubectlLaunchDescriptor {
    pub generation: u64,
    pub kubectl: PathBuf,
    pub context: String,
    pub kubeconfig_sources: Vec<PathBuf>,
    pub environment: BTreeMap<String, String>,
    pub exec_plugins: Vec<ResolvedExecPlugin>,
}

impl KubectlLaunchDescriptor {
    pub fn from_preparation(
        generation: u64,
        preparation: &KubePreparation,
        environment: &EnvironmentSnapshot,
    ) -> Result<Self, DescriptorError> {
        let path = environment
            .unicode("PATH")?
            .ok_or_else(|| DescriptorError::MissingExecutable("kubectl".into()))?;
        let pathext = environment.unicode("PATHEXT")?;
        let kubectl = resolve_executable("kubectl", &path, pathext.as_deref())?;
        let mut allowed = BTreeMap::new();
        allowed.insert("PATH".into(), path.clone());
        let profile_key = if cfg!(windows) { "USERPROFILE" } else { "HOME" };
        if let Some(profile) = environment.unicode(profile_key)? {
            allowed.insert(profile_key.into(), profile);
        }
        let kubeconfig = std::env::join_paths(&preparation.source_paths)
            .map_err(|_| DescriptorError::Unrepresentable)?;
        allowed.insert(
            "KUBECONFIG".into(),
            kubeconfig
                .into_string()
                .map_err(|_| DescriptorError::Unrepresentable)?,
        );
        let mut plugins = Vec::with_capacity(preparation.exec_plugins.len());
        for plugin in &preparation.exec_plugins {
            for (key, value) in &plugin.environment {
                if is_sensitive(key) || is_sensitive_value(value) {
                    return Err(DescriptorError::SensitiveEnvironment(key.clone()));
                }
                if !matches!(key.as_str(), "PATH" | "HOME" | "USERPROFILE" | "KUBECONFIG") {
                    return Err(DescriptorError::UnsupportedEnvironment(key.clone()));
                }
                validate_value(value).map_err(|_| DescriptorError::Unrepresentable)?;
            }
            plugins.push(ResolvedExecPlugin {
                command: resolve_executable(&plugin.command, &path, pathext.as_deref())?,
            });
        }
        Self::new(
            generation,
            kubectl,
            preparation.selected_context.clone(),
            preparation.source_paths.clone(),
            allowed,
            plugins,
        )
    }

    pub fn new(
        generation: u64,
        kubectl: PathBuf,
        context: String,
        kubeconfig_sources: Vec<PathBuf>,
        environment: BTreeMap<String, String>,
        exec_plugins: Vec<ResolvedExecPlugin>,
    ) -> Result<Self, DescriptorError> {
        if generation == 0
            || kubectl.as_os_str().is_empty()
            || context.is_empty()
            || kubeconfig_sources.is_empty()
        {
            return Err(DescriptorError::Unreproducible);
        }
        if kubectl.to_str().is_none()
            || kubeconfig_sources
                .iter()
                .any(|path| path.to_str().is_none())
            || exec_plugins
                .iter()
                .any(|plugin| plugin.command.to_str().is_none())
        {
            return Err(DescriptorError::Unrepresentable);
        }
        validate_value(&context).map_err(|_| DescriptorError::Unrepresentable)?;
        for (name, value) in &environment {
            if is_sensitive(name) || is_sensitive_value(value) {
                return Err(DescriptorError::SensitiveEnvironment(name.clone()));
            }
            if !matches!(
                name.as_str(),
                "PATH" | "HOME" | "USERPROFILE" | "KUBECONFIG"
            ) {
                return Err(DescriptorError::UnsupportedEnvironment(name.clone()));
            }
            validate_value(value).map_err(|_| DescriptorError::Unrepresentable)?;
        }
        Ok(Self {
            generation,
            kubectl,
            context,
            kubeconfig_sources,
            environment,
            exec_plugins,
        })
    }
}

fn is_sensitive(name: &str) -> bool {
    let upper = name.to_ascii_uppercase();
    [
        "TOKEN",
        "SECRET",
        "PASSWORD",
        "PASSWD",
        "PRIVATE",
        "CREDENTIAL",
        "API_KEY",
        "ACCESS_KEY",
    ]
    .iter()
    .any(|marker| upper.contains(marker))
}

fn is_sensitive_value(value: &str) -> bool {
    let upper = value.to_ascii_uppercase();
    upper.contains("-----BEGIN ")
        || upper.starts_with("BEARER ")
        || (value.len() >= 48
            && value.split('.').count() == 3
            && value
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || "-_.=".contains(character)))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DescriptorError {
    Unreproducible,
    Unrepresentable,
    SensitiveEnvironment(String),
    UnsupportedEnvironment(String),
    MissingExecutable(String),
}

impl fmt::Display for DescriptorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unreproducible => f.write_str("configuration cannot be reproduced by kubectl"),
            Self::Unrepresentable => f.write_str("configuration contains an unrepresentable value"),
            Self::SensitiveEnvironment(name) => write!(
                f,
                "sensitive environment variable {name} cannot be rendered"
            ),
            Self::UnsupportedEnvironment(name) => write!(
                f,
                "environment variable {name} is outside the fixed allowlist"
            ),
            Self::MissingExecutable(name) => {
                write!(f, "required executable {name} could not be resolved")
            }
        }
    }
}

fn resolve_executable(
    command: &str,
    path: &str,
    _pathext: Option<&str>,
) -> Result<PathBuf, DescriptorError> {
    let candidate = PathBuf::from(command);
    let path_qualified = candidate.is_absolute() || command.contains(['/', '\\']);
    #[cfg(windows)]
    let candidates = windows_executable_candidates(&candidate, _pathext);
    #[cfg(not(windows))]
    let candidates = vec![candidate];
    if path_qualified {
        candidates
            .into_iter()
            .find_map(executable)
            .ok_or_else(|| DescriptorError::MissingExecutable(command.into()))
    } else {
        std::env::split_paths(path)
            .flat_map(|directory| candidates.iter().map(move |name| directory.join(name)))
            .find_map(executable)
            .ok_or_else(|| DescriptorError::MissingExecutable(command.into()))
    }
}

#[cfg(windows)]
fn windows_executable_candidates(
    candidate: &std::path::Path,
    pathext: Option<&str>,
) -> Vec<PathBuf> {
    if candidate.extension().is_some() {
        return vec![candidate.to_path_buf()];
    }
    let extensions = pathext
        .filter(|value| !value.is_empty())
        .unwrap_or(".EXE;.CMD;.BAT;.COM");
    extensions
        .split(';')
        .filter(|extension| !extension.is_empty())
        .map(|extension| {
            let extension = extension.strip_prefix('.').unwrap_or(extension);
            candidate.with_extension(extension)
        })
        .collect()
}

fn executable(path: PathBuf) -> Option<PathBuf> {
    let metadata = std::fs::metadata(&path).ok()?;
    if !metadata.is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        if metadata.permissions().mode() & 0o111 == 0 {
            return None;
        }
    }
    Some(path)
}

impl std::error::Error for DescriptorError {}

/// Fully structured, generation-bound Pod shell target.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalShellTarget {
    pub generation: u64,
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
    pub program: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RenderError {
    GenerationMismatch,
    InvalidField { field: &'static str },
}

impl fmt::Display for RenderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GenerationMismatch => {
                f.write_str("shell target belongs to a different connection generation")
            }
            Self::InvalidField { field } => write!(
                f,
                "shell target field {field} is empty or contains a line break or NUL"
            ),
        }
    }
}

impl std::error::Error for RenderError {}

#[derive(Debug)]
pub struct KubectlExecCommand<'a> {
    descriptor: &'a KubectlLaunchDescriptor,
    target: ExternalShellTarget,
}

impl<'a> KubectlExecCommand<'a> {
    pub fn new(
        descriptor: &'a KubectlLaunchDescriptor,
        target: ExternalShellTarget,
    ) -> Result<Self, RenderError> {
        if descriptor.generation != target.generation {
            return Err(RenderError::GenerationMismatch);
        }
        for (field, value) in [
            ("namespace", &target.namespace),
            ("pod", &target.pod),
            ("uid", &target.uid),
            ("container", &target.container),
            ("program", &target.program),
        ] {
            validate_value(value).map_err(|()| RenderError::InvalidField { field })?;
        }
        Ok(Self { descriptor, target })
    }

    pub fn render_posix(&self) -> Result<String, RenderError> {
        let q = posix_literal;
        let d = self.descriptor;
        let t = &self.target;
        let mut script = String::from("#!/bin/sh\n");
        script.push_str(&format!(
            "K10S_KUBECTL={}\n",
            q(d.kubectl.to_str().expect("descriptor paths were validated"))
        ));
        let clean_environment = d
            .environment
            .iter()
            .map(|(name, value)| q(&format!("{name}={value}")))
            .collect::<Vec<_>>()
            .join(" ");
        script.push_str(&format!("K10S_UID=$(env -i {clean_environment} \"$K10S_KUBECTL\" --context {} --namespace {} get pod {} -o {})\nK10S_STATUS=$?\n", q(&d.context), q(&t.namespace), q(&t.pod), q("jsonpath={.metadata.uid}")));
        script.push_str("if [ \"$K10S_STATUS\" -ne 0 ]; then printf '%s\\n' 'Finback shell: Pod UID lookup failed.' >&2\nelif [ \"$K10S_UID\" != ");
        script.push_str(&q(&t.uid));
        script.push_str(" ]; then K10S_STATUS=66; printf '%s\\n' 'Finback shell: Pod UID changed; refusing exec.' >&2\nelse\n  env -i ");
        script.push_str(&clean_environment);
        script.push_str(" \"$K10S_KUBECTL\" --context ");
        script.push_str(&q(&d.context));
        script.push_str(" --namespace ");
        script.push_str(&q(&t.namespace));
        script.push_str(" exec -it ");
        script.push_str(&q(&t.pod));
        script.push_str(" --container ");
        script.push_str(&q(&t.container));
        script.push_str(" -- ");
        script.push_str(&q(&t.program));
        script.push_str("\n  K10S_STATUS=$?\n  if [ \"$K10S_STATUS\" -ne 0 ]; then printf '%s\\n' 'Finback shell: kubectl exec failed.' >&2; fi\nfi\nif [ \"$K10S_STATUS\" -ne 0 ] && [ -t 0 ]; then printf '%s' 'Press Enter to close...'; IFS= read -r _ || :; fi\nexit \"$K10S_STATUS\"\n");
        Ok(script)
    }

    pub fn render_powershell(&self) -> Result<String, RenderError> {
        let q = powershell_literal;
        let d = self.descriptor;
        let t = &self.target;
        let mut script = String::from(
            "$ErrorActionPreference = 'Continue'\r\nGet-ChildItem Env: | ForEach-Object { Remove-Item -LiteralPath $_.PSPath }\r\n",
        );
        for (name, value) in &d.environment {
            script.push_str(&format!("$env:{name} = {}\r\n", q(value)));
        }
        script.push_str(&format!(
            "$K10sKubectl = {}\r\n",
            q(d.kubectl.to_str().expect("descriptor paths were validated"))
        ));
        script.push_str("$global:LASTEXITCODE = 125\r\n");
        script.push_str(&format!("try {{ $K10sUid = & $K10sKubectl --context {} --namespace {} get pod {} -o {}; $K10sStatus = $LASTEXITCODE }} catch {{ $K10sStatus = 125; $K10sUid = $null }}\r\n", q(&d.context), q(&t.namespace), q(&t.pod), q("jsonpath={.metadata.uid}")));
        script.push_str(&format!("if ($K10sStatus -ne 0) {{ [Console]::Error.WriteLine('Finback shell: Pod UID lookup failed.') }} elseif ($K10sUid -ne {}) {{ $K10sStatus = 66; [Console]::Error.WriteLine('Finback shell: Pod UID changed; refusing exec.') }} else {{\r\n$global:LASTEXITCODE = 125\r\ntry {{ & $K10sKubectl --context {} --namespace {} exec -it {} --container {} -- {}; $K10sStatus = $LASTEXITCODE }} catch {{ $K10sStatus = 125 }}\r\nif ($K10sStatus -ne 0) {{ [Console]::Error.WriteLine('Finback shell: kubectl exec failed.') }}\r\n}}\r\nif ($K10sStatus -ne 0 -and -not [Console]::IsInputRedirected) {{ [void][Console]::ReadLine() }}\r\nexit $K10sStatus\r\n", q(&t.uid), q(&d.context), q(&t.namespace), q(&t.pod), q(&t.container), q(&t.program)));
        Ok(script)
    }
}

fn validate_value(value: &str) -> Result<(), ()> {
    if value.is_empty() || value.contains(['\n', '\r', '\0']) {
        Err(())
    } else {
        Ok(())
    }
}

fn posix_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}
fn powershell_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

const MANIFEST_NAME: &str = "manifest.json";
const MANIFEST_VERSION: u32 = 1;

#[derive(Debug)]
pub enum StorageError {
    Io(std::io::Error),
    Randomness(getrandom::Error),
    InvalidParent,
    Render(RenderError),
    NoTerminalLauncher,
    LaunchFailed(Vec<SafeLaunchAttempt>),
    RollbackFailed,
}

impl fmt::Display for StorageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(_) => formatter.write_str("temporary shell storage is unavailable"),
            Self::Randomness(_) => formatter.write_str("secure launch-name generation failed"),
            Self::InvalidParent => formatter
                .write_str("temporary shell parent failed ownership or permission validation"),
            Self::Render(error) => write!(formatter, "shell request is invalid: {error}"),
            Self::NoTerminalLauncher => {
                formatter.write_str("no system terminal accepted the shell launch")
            }
            Self::LaunchFailed(attempts) => {
                formatter.write_str("system terminal failed to start")?;
                for attempt in attempts {
                    write!(formatter, "; {} ({})", attempt.executable, attempt.category)?;
                }
                Ok(())
            }
            Self::RollbackFailed => formatter.write_str("temporary shell rollback failed"),
        }
    }
}

impl std::error::Error for StorageError {}
impl From<std::io::Error> for StorageError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}
impl From<getrandom::Error> for StorageError {
    fn from(value: getrandom::Error) -> Self {
        Self::Randomness(value)
    }
}
impl From<RenderError> for StorageError {
    fn from(value: RenderError) -> Self {
        Self::Render(value)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct LaunchManifest {
    version: u32,
    launch_id: String,
    created_unix_seconds: u64,
    script: String,
}

#[derive(Clone, Debug)]
pub struct TemporaryShellStorage {
    parent: platform::PrivateParent,
    fault: Option<StorageFaultPoint>,
}

#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageFaultPoint {
    DirectoryCreate,
    DirectoryPermissions,
    ManifestCreate,
    ManifestWrite,
    ManifestSync,
    Render,
    ScriptCreate,
    ScriptWrite,
    ScriptSync,
    ScriptPermissions,
}

impl TemporaryShellStorage {
    pub fn new(parent: PathBuf) -> Result<Self, StorageError> {
        let parent = platform::ensure_private_parent(&parent)?;
        Ok(Self {
            parent,
            fault: None,
        })
    }

    #[doc(hidden)]
    pub fn with_fault_for_test(mut self, fault: StorageFaultPoint) -> Self {
        self.fault = Some(fault);
        self
    }

    fn injected(&self, point: StorageFaultPoint) -> Result<(), StorageError> {
        if self.fault == Some(point) {
            Err(StorageError::Io(std::io::Error::other(
                "injected storage failure",
            )))
        } else {
            Ok(())
        }
    }

    pub fn create(
        &self,
        command: &KubectlExecCommand<'_>,
    ) -> Result<TemporaryShellScript, StorageError> {
        platform::validate_private_parent(&self.parent)?;
        let launch_id = random_name()?;
        let suffix = if cfg!(windows) {
            "ps1"
        } else if cfg!(target_os = "macos") {
            "command"
        } else {
            "sh"
        };
        let script_name = format!("launch.{suffix}");
        self.injected(StorageFaultPoint::DirectoryCreate)?;
        let child = platform::create_private_directory(&self.parent, &launch_id)?;
        let directory = platform::child_path(&child).to_owned();
        let mut transaction = CreationTransaction {
            child: child.clone(),
            manifest: None,
            script: None,
            committed: false,
        };
        if let Err(error) = self.injected(StorageFaultPoint::DirectoryPermissions) {
            transaction
                .rollback()
                .map_err(|()| StorageError::RollbackFailed)?;
            return Err(error);
        }
        let outcome = (|| {
            let manifest = LaunchManifest {
                version: MANIFEST_VERSION,
                launch_id,
                created_unix_seconds: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                script: script_name.clone(),
            };
            let manifest_path = directory.join(MANIFEST_NAME);
            self.injected(StorageFaultPoint::ManifestCreate)?;
            platform::create_private_file(
                &child,
                MANIFEST_NAME,
                &serde_json::to_vec(&manifest).map_err(std::io::Error::other)?,
                false,
                || transaction.manifest = Some(manifest_path.clone()),
            )?;
            self.injected(StorageFaultPoint::ManifestWrite)?;
            self.injected(StorageFaultPoint::ManifestSync)?;
            let script_path = directory.join(&script_name);
            self.injected(StorageFaultPoint::Render)?;
            let body = render_self_cleaning(command, &manifest_path, &directory)?;
            self.injected(StorageFaultPoint::ScriptCreate)?;
            platform::create_private_file(&child, &script_name, &body, true, || {
                transaction.script = Some(script_path.clone());
            })?;
            self.injected(StorageFaultPoint::ScriptWrite)?;
            self.injected(StorageFaultPoint::ScriptSync)?;
            self.injected(StorageFaultPoint::ScriptPermissions)?;
            transaction.committed = true;
            Ok(TemporaryShellScript {
                directory,
                path: script_path,
                manifest: manifest_path,
                child,
            })
        })();
        match outcome {
            Ok(script) => Ok(script),
            Err(error) => match transaction.rollback() {
                Ok(()) => Err(error),
                Err(()) => Err(StorageError::RollbackFailed),
            },
        }
    }

    pub fn cleanup_expired(&self, now_unix_seconds: u64) -> Result<CleanupReport, StorageError> {
        platform::validate_private_parent(&self.parent)?;
        let mut candidates = Vec::new();
        for entry in std::fs::read_dir(platform::parent_path(&self.parent))? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if !valid_launch_id(name) {
                continue;
            }
            let Ok(child) = platform::open_child(&self.parent, name) else {
                continue;
            };
            let directory = platform::child_path(&child).to_owned();
            let Ok(bytes) = platform::read_regular_file(&child, MANIFEST_NAME) else {
                continue;
            };
            let Ok(manifest) = serde_json::from_slice::<LaunchManifest>(&bytes) else {
                continue;
            };
            if manifest.version != MANIFEST_VERSION
                || manifest.launch_id != name
                || !valid_script_name(&manifest.script)
            {
                continue;
            }
            candidates.push((
                manifest.created_unix_seconds,
                directory,
                manifest.script,
                child,
            ));
        }
        candidates.sort_by_key(|candidate| candidate.0);
        let mut report = CleanupReport::default();
        for (created, directory, script_name, child) in candidates.into_iter().take(128) {
            report.examined += 1;
            if now_unix_seconds.saturating_sub(created) <= 24 * 60 * 60 {
                continue;
            }
            let entries = std::fs::read_dir(&directory)?.collect::<Result<Vec<_>, _>>()?;
            if entries.len() != 2
                || entries.iter().any(|entry| {
                    let name = entry.file_name();
                    name != std::ffi::OsStr::new(MANIFEST_NAME)
                        && name != std::ffi::OsStr::new(&script_name)
                })
            {
                continue;
            }
            platform::validate_regular_file(&child, &script_name)?;
            platform::validate_regular_file(&child, MANIFEST_NAME)?;
            platform::remove_regular_file(&child, &script_name)?;
            platform::remove_regular_file(&child, MANIFEST_NAME)?;
            platform::remove_empty_directory(&child)?;
            report.removed += 1;
        }
        Ok(report)
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CleanupReport {
    pub examined: usize,
    pub removed: usize,
}

fn valid_launch_id(value: &str) -> bool {
    value.len() == 24
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
}
fn valid_script_name(value: &str) -> bool {
    matches!(value, "launch.sh" | "launch.command" | "launch.ps1")
}

struct CreationTransaction {
    child: platform::PrivateChild,
    manifest: Option<PathBuf>,
    script: Option<PathBuf>,
    committed: bool,
}
impl Drop for CreationTransaction {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let _rollback_already_reported = self.rollback();
    }
}
impl CreationTransaction {
    fn rollback(&mut self) -> Result<(), ()> {
        let mut failed = false;
        if let Some(path) = self.script.take() {
            failed |= platform::remove_regular_file(
                &self.child,
                path.file_name().and_then(|v| v.to_str()).unwrap_or(""),
            )
            .is_err();
        }
        if let Some(path) = self.manifest.take() {
            failed |= platform::remove_regular_file(
                &self.child,
                path.file_name().and_then(|v| v.to_str()).unwrap_or(""),
            )
            .is_err();
        }
        failed |= platform::remove_empty_directory(&self.child).is_err();
        if failed {
            Err(())
        } else {
            self.committed = true;
            Ok(())
        }
    }
}

#[derive(Debug)]
pub struct TemporaryShellScript {
    directory: PathBuf,
    path: PathBuf,
    manifest: PathBuf,
    child: platform::PrivateChild,
}
impl TemporaryShellScript {
    pub fn directory(&self) -> &Path {
        &self.directory
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn manifest_path(&self) -> &Path {
        &self.manifest
    }
    pub fn cleanup(&self) -> Result<(), StorageError> {
        platform::validate_launch_directory(&self.child)?;
        platform::remove_regular_file(
            &self.child,
            self.path
                .file_name()
                .and_then(|v| v.to_str())
                .ok_or(StorageError::InvalidParent)?,
        )?;
        platform::remove_regular_file(&self.child, MANIFEST_NAME)?;
        platform::remove_empty_directory(&self.child)?;
        Ok(())
    }
}

fn render_self_cleaning(
    command: &KubectlExecCommand<'_>,
    manifest: &Path,
    directory: &Path,
) -> Result<Vec<u8>, StorageError> {
    #[cfg(windows)]
    {
        return windows::render_with_cleanup(command, manifest, directory).map_err(Into::into);
    }
    #[cfg(unix)]
    {
        let mut body = command.render_posix()?;
        let exit = "exit \"$K10S_STATUS\"\n";
        let cleanup = format!(
            "rm -f -- \"$0\" {}\nrmdir -- {} 2>/dev/null || :\nexit \"$K10S_STATUS\"\n",
            posix_literal(manifest.to_str().ok_or(StorageError::InvalidParent)?),
            posix_literal(directory.to_str().ok_or(StorageError::InvalidParent)?)
        );
        body = body
            .strip_suffix(exit)
            .ok_or(StorageError::InvalidParent)?
            .to_owned()
            + &cleanup;
        Ok(body.into_bytes())
    }
}

fn random_name() -> Result<String, StorageError> {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    let mut bytes = [0_u8; 24];
    getrandom::fill(&mut bytes)?;
    Ok(bytes
        .into_iter()
        .map(|value| ALPHABET[usize::from(value & 63)] as char)
        .collect())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchAttempt {
    Missing,
    Spawn(String),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SafeLaunchAttempt {
    pub executable: String,
    pub category: &'static str,
}

fn safe_attempt(adapter: &TerminalAdapter, error: &std::io::Error) -> SafeLaunchAttempt {
    SafeLaunchAttempt {
        executable: adapter
            .executable
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("terminal")
            .chars()
            .filter(|value| value.is_ascii_alphanumeric() || matches!(value, '.' | '-' | '_'))
            .take(64)
            .collect(),
        category: match error.kind() {
            std::io::ErrorKind::NotFound => "not found",
            std::io::ErrorKind::PermissionDenied => "permission denied",
            std::io::ErrorKind::WouldBlock => "temporarily unavailable",
            _ => "spawn error",
        },
    }
}

pub fn launch_system_terminal(script: &TemporaryShellScript) -> Result<(), StorageError> {
    #[cfg(target_os = "macos")]
    {
        launch_macos_with(script, |program, args| {
            std::process::Command::new(program)
                .args(args)
                .spawn()
                .map(|_| ())
                .map_err(|error| LaunchAttempt::Spawn(error.to_string()))
        })
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        return launch_linux_with(script, |program, args| {
            std::process::Command::new(program)
                .args(args)
                .spawn()
                .map(|_| ())
                .map_err(|error| {
                    if error.kind() == std::io::ErrorKind::NotFound {
                        LaunchAttempt::Missing
                    } else {
                        LaunchAttempt::Spawn(error.to_string())
                    }
                })
        });
    }
    #[cfg(windows)]
    {
        return windows::launch(script);
    }
}

pub fn launch_with_adapter(
    script: &TemporaryShellScript,
    adapter: &TerminalAdapter,
) -> Result<(), StorageError> {
    launch_with_adapters(script, std::slice::from_ref(adapter))
}

pub fn launch_with_adapters(
    script: &TemporaryShellScript,
    adapters: &[TerminalAdapter],
) -> Result<(), StorageError> {
    if adapters.is_empty() {
        script.cleanup()?;
        return Err(StorageError::NoTerminalLauncher);
    }
    let mut attempts = Vec::new();
    for adapter in adapters {
        let mut command = std::process::Command::new(&adapter.executable);
        command
            .args(&adapter.arguments_before_script)
            .arg(script.path());
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            command.creation_flags(0x0000_0010);
        }
        match command.spawn() {
            Ok(_) => return Ok(()),
            Err(error) => attempts.push(safe_attempt(adapter, &error)),
        }
    }
    script.cleanup()?;
    Err(StorageError::LaunchFailed(attempts))
}

#[cfg(target_os = "macos")]
pub fn launch_macos_with<F>(script: &TemporaryShellScript, mut spawn: F) -> Result<(), StorageError>
where
    F: FnMut(&str, &[PlatformString]) -> Result<(), LaunchAttempt>,
{
    let arguments = [script.path.as_os_str().to_owned()];
    if spawn("/usr/bin/open", &arguments).is_ok() {
        Ok(())
    } else {
        let _ = script.cleanup();
        Err(StorageError::NoTerminalLauncher)
    }
}

#[cfg(all(unix, not(target_os = "macos")))]
pub fn launch_linux_with<F>(script: &TemporaryShellScript, mut spawn: F) -> Result<(), StorageError>
where
    F: FnMut(&str, &[PlatformString]) -> Result<(), LaunchAttempt>,
{
    for (program, marker) in [
        ("xdg-terminal-exec", "--"),
        ("x-terminal-emulator", "-e"),
        ("gnome-terminal", "--"),
        ("konsole", "-e"),
        ("kitty", "--"),
    ] {
        let arguments = [
            PlatformString::from(marker),
            script.path.as_os_str().to_owned(),
        ];
        if spawn(program, &arguments).is_ok() {
            return Ok(());
        }
    }
    let _ = script.cleanup();
    Err(StorageError::NoTerminalLauncher)
}

#[cfg(unix)]
mod platform {
    pub(super) use super::unix::*;
}
#[cfg(windows)]
mod platform {
    pub(super) use super::windows::*;
}
