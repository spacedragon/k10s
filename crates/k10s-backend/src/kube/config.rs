//! Kubeconfig loading boundary: the only place kube-rs types may appear.
//!
//! Everything crossing this module is either a validated, credential-free
//! [`ContextInfo`] summary or a normalized [`AdapterError`]. Raw parse and I/O
//! errors are mapped to operator-facing messages that never echo file
//! contents (which may include tokens).

use std::io;
use std::path::Path;

use kube::config::{Config, Kubeconfig, KubeconfigError};

use crate::port::{AdapterError, ContextAvailability, ContextInfo};

/// Load credential-free context summaries from an explicit kubeconfig path or
/// standard discovery (`KUBECONFIG`, then `~/.kube/config`), along with the
/// parsed kube-rs config that seeds per-context cluster client construction.
pub(crate) fn load_with_source(
    explicit_path: Option<&Path>,
) -> Result<(Vec<ContextInfo>, Kubeconfig), AdapterError> {
    let (kubeconfig, source) = match explicit_path {
        Some(path) => Kubeconfig::read_from(path)
            .map(|kubeconfig| (kubeconfig, path.display().to_string()))
            .map_err(|error| normalize_load_error(error, || path.display().to_string())),
        None => Kubeconfig::read()
            .map(|kubeconfig| (kubeconfig, discovery_source()))
            .map_err(normalize_discovery_error),
    }?;

    validate_and_map(&kubeconfig, &source).map(|summaries| (summaries, kubeconfig))
}

/// Describe where standard discovery looked, for operator-facing errors.
fn discovery_source() -> String {
    if let Some(value) = std::env::var_os("KUBECONFIG")
        && !value.is_empty()
    {
        return format!("KUBECONFIG={}", value.to_string_lossy());
    }
    std::env::home_dir()
        .map(|home| home.join(".kube").join("config"))
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "~/.kube/config".into())
}

/// Normalize a kubeconfig load failure for an explicit file path.
fn normalize_load_error(
    error: KubeconfigError,
    source_hint: impl FnOnce() -> String,
) -> AdapterError {
    match error {
        KubeconfigError::FindPath => AdapterError::KubeconfigNotConfigured,
        // The named file is absent (ENOENT): report it precisely.
        KubeconfigError::ReadConfig(source, path) if source.kind() == io::ErrorKind::NotFound => {
            AdapterError::KubeconfigMissing(path)
        }
        KubeconfigError::ReadConfig(_, path) => AdapterError::KubeconfigInvalid {
            source: path.display().to_string(),
            detail: "the file exists but could not be read".into(),
        },
        // Parse and structural failures carry safe messages only.
        other => AdapterError::KubeconfigInvalid {
            source: source_hint(),
            detail: describe(other),
        },
    }
}

/// Normalize a kubeconfig load failure from standard discovery, where the
/// failing file may be any entry of `KUBECONFIG` or the default path.
fn normalize_discovery_error(error: KubeconfigError) -> AdapterError {
    match error {
        // Nothing configured anywhere: no env value and no default file.
        KubeconfigError::FindPath => AdapterError::KubeconfigNotConfigured,
        KubeconfigError::ReadConfig(source, path) if source.kind() == io::ErrorKind::NotFound => {
            AdapterError::KubeconfigMissing(path)
        }
        // For every other failure we know the discovery source to name.
        other => AdapterError::KubeconfigInvalid {
            source: discovery_source(),
            detail: describe(other),
        },
    }
}

/// Map kube-rs error variants to safe, credential-free operator messages.
fn describe(error: KubeconfigError) -> String {
    match error {
        KubeconfigError::Parse(_) => "the kubeconfig YAML could not be parsed".to_owned(),
        KubeconfigError::KindMismatch => "merged kubeconfigs declare conflicting kinds".to_owned(),
        KubeconfigError::ApiVersionMismatch => {
            "merged kubeconfigs declare conflicting apiVersions".to_owned()
        }
        KubeconfigError::CurrentContextNotSet | KubeconfigError::LoadContext(_) => {
            "no current context could be determined from the kubeconfig".to_owned()
        }
        KubeconfigError::LoadClusterOfContext(name) => {
            format!("context references cluster '{name}', which is not defined in the kubeconfig")
        }
        KubeconfigError::MissingClusterUrl => {
            "the selected cluster has no server URL configured".to_owned()
        }
        // URI errors may embed userinfo credentials; never echo them.
        KubeconfigError::ParseClusterUrl(_) => "the cluster server URL is invalid".to_owned(),
        KubeconfigError::ParseProxyUrl(_) => "the configured proxy URL is invalid".to_owned(),
        KubeconfigError::LoadCertificateAuthority(_)
        | KubeconfigError::LoadClientCertificate(_)
        | KubeconfigError::LoadClientKey(_) => {
            "cluster or client certificate data could not be loaded".to_owned()
        }
        KubeconfigError::ParseCertificates(_) => "certificate material is not valid PEM".to_owned(),
        // Defensive: these variants are handled by the normalization helpers
        // above, but describe must stay exhaustive as kube-rs evolves.
        KubeconfigError::FindPath => "no kubeconfig path could be found".to_owned(),
        KubeconfigError::ReadConfig(_, _) => "the kubeconfig file could not be read".to_owned(),
    }
}

/// Validate the parsed kubeconfig and map it to credential-free summaries.
fn validate_and_map(
    kubeconfig: &Kubeconfig,
    source: &str,
) -> Result<Vec<ContextInfo>, AdapterError> {
    let mut summaries = Vec::with_capacity(kubeconfig.contexts.len());

    for named in &kubeconfig.contexts {
        // Refuse exec-based credential helpers before anything is committed:
        // k10s never executes external binaries to obtain credentials.
        if let Some(user_name) = named
            .context
            .as_ref()
            .and_then(|context| context.user.clone())
            .filter(|user| !user.is_empty())
            && has_exec_plugin(kubeconfig, &user_name)
        {
            return Err(AdapterError::ExecPluginRejected {
                context: named.name.clone(),
                user: user_name,
            });
        }

        let Some(context) = named.context.as_ref() else {
            return Err(AdapterError::KubeconfigInvalid {
                source: source.to_owned(),
                detail: format!("context '{}' has no cluster definition", named.name),
            });
        };

        // Resolve and validate the referenced cluster before anything is
        // committed: every context exposed through bootstrap must point at a
        // defined cluster with a parseable server URL.
        let Some(cluster_named) = kubeconfig
            .clusters
            .iter()
            .find(|entry| entry.name == *context.cluster)
        else {
            return Err(AdapterError::KubeconfigInvalid {
                source: source.to_owned(),
                detail: format!(
                    "context '{}' references cluster '{}', which is not defined in the kubeconfig",
                    named.name, context.cluster
                ),
            });
        };
        if cluster_named.cluster.is_none() {
            return Err(AdapterError::KubeconfigInvalid {
                source: source.to_owned(),
                detail: format!("cluster '{}' has no definition", context.cluster),
            });
        }
        summaries.push(ContextInfo {
            name: named.name.clone(),
            cluster: context.cluster.clone(),
            namespace: context.namespace.clone(),
            is_current: kubeconfig.current_context.as_deref() == Some(named.name.as_str()),
            availability: ContextAvailability::Unknown,
            unavailable_reason: None,
        });
    }

    if let Some(current) = &kubeconfig.current_context
        && !summaries.iter().any(|summary| summary.name == *current)
    {
        return Err(AdapterError::KubeconfigInvalid {
            source: source.to_owned(),
            detail: format!("current-context '{current}' does not exist in the kubeconfig"),
        });
    }

    if summaries.is_empty() {
        return Err(AdapterError::KubeconfigInvalid {
            source: source.to_owned(),
            detail: "the kubeconfig contains no contexts".to_owned(),
        });
    }

    // Final gate: run kube-rs's own config loader over the selected context.
    // Kubeconfig::read only deserializes and merges, so cluster references,
    // server URLs, and current-context resolution are validated here instead.
    // This is safe offline: exec plugins were rejected above (and the loader
    // never executes them at this stage), no network calls happen.
    Config::try_from(kubeconfig.clone()).map_err(|error| AdapterError::KubeconfigInvalid {
        source: source.to_owned(),
        detail: describe(error),
    })?;

    Ok(summaries)
}

/// Whether the referenced user relies on an exec-based credential plugin.
fn has_exec_plugin(kubeconfig: &Kubeconfig, user_name: &str) -> bool {
    kubeconfig
        .auth_infos
        .iter()
        .find(|named| named.name == user_name)
        .and_then(|named| named.auth_info.as_ref())
        .is_some_and(|auth_info| auth_info.exec.is_some())
}
