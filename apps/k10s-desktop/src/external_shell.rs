//! Credential-free launch descriptions and pure shell-script rendering.
//!
//! Temporary-file creation and terminal process launch deliberately live
//! outside this module: this boundary only validates structured values and
//! turns them into literal-safe scripts.

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fmt;
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
    #[cfg(target_os = "macos")]
    {
        return executable(PathBuf::from("/usr/bin/open")).map(|executable| TerminalAdapter {
            executable,
            arguments_before_script: Vec::new(),
        });
    }
    #[cfg(target_os = "windows")]
    {
        let path = _environment.unicode("PATH").ok().flatten()?;
        return resolve_executable("powershell.exe", &path)
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
            });
    }
    #[cfg(all(unix, not(target_os = "macos")))]
    {
        let path = _environment.unicode("PATH").ok().flatten()?;
        for (name, arguments) in [
            ("xdg-terminal-exec", vec!["--"]),
            ("x-terminal-emulator", vec!["-e"]),
            ("gnome-terminal", vec!["--"]),
            ("konsole", vec!["-e"]),
            ("kitty", vec!["--"]),
        ] {
            if let Ok(executable) = resolve_executable(name, &path) {
                return Some(TerminalAdapter {
                    executable,
                    arguments_before_script: arguments.into_iter().map(str::to_owned).collect(),
                });
            }
        }
        None
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
        let kubectl = resolve_executable("kubectl", &path)?;
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
                command: resolve_executable(&plugin.command, &path)?,
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

fn resolve_executable(command: &str, path: &str) -> Result<PathBuf, DescriptorError> {
    let candidate = PathBuf::from(command);
    if candidate.components().count() > 1 {
        return executable(candidate)
            .ok_or_else(|| DescriptorError::MissingExecutable(command.into()));
    }
    std::env::split_paths(path)
        .map(|directory| directory.join(command))
        .find_map(executable)
        .ok_or_else(|| DescriptorError::MissingExecutable(command.into()))
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
            q(&d.kubectl.to_string_lossy())
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
            q(&d.kubectl.to_string_lossy())
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
