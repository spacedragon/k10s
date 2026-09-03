//! Kubeconfig loading boundary: the only place kube-rs types may appear.
//!
//! Everything crossing this module is either a validated, credential-free
//! [`ContextInfo`] summary or a normalized [`AdapterError`]. Raw parse and I/O
//! errors are mapped to operator-facing messages that never echo file
//! contents (which may include tokens).

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use kube::config::{Config, ExecInteractiveMode, Kubeconfig, KubeconfigError};
use sha2::{Digest, Sha256};

use crate::port::{AdapterError, ContextAvailability, ContextInfo};

type LoadedKubeconfig = (Vec<ContextInfo>, Kubeconfig, Vec<PathBuf>, Vec<[u8; 32]>);

/// Load credential-free context summaries from an explicit kubeconfig path or
/// standard discovery (`KUBECONFIG`, then `~/.kube/config`), along with the
/// parsed kube-rs config that seeds per-context cluster client construction.
pub(crate) fn load_with_source(
    explicit_path: Option<&Path>,
) -> Result<LoadedKubeconfig, AdapterError> {
    // Freeze discovery before touching any file, then freeze every file's
    // bytes before parsing. Kernel construction and launch metadata therefore
    // cannot observe different KUBECONFIG/HOME values or a later rewrite.
    let paths = source_paths(explicit_path)?;
    load_from_paths(paths)
}

pub(crate) fn load_from_paths(paths: Vec<PathBuf>) -> Result<LoadedKubeconfig, AdapterError> {
    if paths.is_empty() {
        return Err(AdapterError::KubeconfigNotConfigured);
    }
    let source = paths
        .iter()
        .map(|path| unicode_path(path))
        .collect::<Result<Vec<_>, _>>()?
        .join(if cfg!(windows) { ";" } else { ":" });
    let frozen = paths
        .iter()
        .map(|path| {
            fs::read(path)
                .map_err(|error| {
                    if error.kind() == io::ErrorKind::NotFound {
                        AdapterError::KubeconfigMissing(path.clone())
                    } else {
                        AdapterError::KubeconfigInvalid {
                            source: unicode_path(path)
                                .unwrap_or_else(|_| "non-Unicode kubeconfig path".into()),
                            detail: "the file exists but could not be read".into(),
                        }
                    }
                })
                .and_then(|bytes| {
                    let digest: [u8; 32] = Sha256::digest(&bytes).into();
                    decode_kubeconfig(&bytes, path).map(|document| (document, digest))
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    let (documents, digests): (Vec<_>, Vec<_>) = frozen.into_iter().unzip();
    let mut kubeconfig = Kubeconfig::default();
    for (path, document) in paths.iter().zip(documents) {
        let mut next =
            Kubeconfig::from_yaml(&document).map_err(|error| AdapterError::KubeconfigInvalid {
                source: unicode_path(path).unwrap_or_else(|_| "non-Unicode kubeconfig path".into()),
                detail: describe(error),
            })?;
        resolve_relative_references(&mut next, path.parent().unwrap_or_else(|| Path::new(".")))?;
        kubeconfig = kubeconfig
            .merge(next)
            .map_err(|error| AdapterError::KubeconfigInvalid {
                source: source.clone(),
                detail: describe(error),
            })?;
    }
    validate_and_map(&kubeconfig, &source).map(|summaries| (summaries, kubeconfig, paths, digests))
}

fn decode_kubeconfig(bytes: &[u8], path: &Path) -> Result<String, AdapterError> {
    let invalid = || AdapterError::KubeconfigInvalid {
        source: unicode_path(path).unwrap_or_else(|_| "non-Unicode kubeconfig path".into()),
        detail: "the kubeconfig text encoding is invalid".into(),
    };
    if let Some(encoded) = bytes.strip_prefix(&[0xff, 0xfe]) {
        if encoded.len() % 2 != 0 {
            return Err(invalid());
        }
        let units = encoded
            .chunks_exact(2)
            .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
            .collect::<Vec<_>>();
        return String::from_utf16(&units).map_err(|_| invalid());
    }
    if let Some(encoded) = bytes.strip_prefix(&[0xfe, 0xff]) {
        if encoded.len() % 2 != 0 {
            return Err(invalid());
        }
        let units = encoded
            .chunks_exact(2)
            .map(|pair| u16::from_be_bytes([pair[0], pair[1]]))
            .collect::<Vec<_>>();
        return String::from_utf16(&units).map_err(|_| invalid());
    }
    let utf8 = bytes.strip_prefix(&[0xef, 0xbb, 0xbf]).unwrap_or(bytes);
    std::str::from_utf8(utf8)
        .map(str::to_owned)
        .map_err(|_| invalid())
}

/// Exact ordered files whose merged contents produced a kube client snapshot.
fn source_paths(explicit_path: Option<&Path>) -> Result<Vec<PathBuf>, AdapterError> {
    let kubeconfig = std::env::var_os("KUBECONFIG");
    let home = std::env::home_dir();
    source_paths_from(explicit_path, kubeconfig.as_deref(), home.as_deref())
}

fn source_paths_from(
    explicit_path: Option<&Path>,
    kubeconfig: Option<&std::ffi::OsStr>,
    home: Option<&Path>,
) -> Result<Vec<PathBuf>, AdapterError> {
    if let Some(path) = explicit_path {
        return Ok(vec![path.to_path_buf()]);
    }
    if let Some(value) = kubeconfig
        && !value.is_empty()
    {
        let paths = std::env::split_paths(&value)
            .filter(|path| !path.as_os_str().is_empty())
            .collect::<Vec<_>>();
        if !paths.is_empty() {
            return Ok(paths);
        }
    }
    home.map(|home| vec![home.join(".kube").join("config")])
        .ok_or(AdapterError::KubeconfigNotConfigured)
}

fn unicode_path(path: &Path) -> Result<String, AdapterError> {
    path.to_str()
        .map(str::to_owned)
        .ok_or_else(|| AdapterError::KubeconfigInvalid {
            source: "non-Unicode kubeconfig path".into(),
            detail: "kubeconfig paths must be valid Unicode for faithful kubectl reproduction"
                .into(),
        })
}

fn resolve_relative_references(
    kubeconfig: &mut Kubeconfig,
    directory: &Path,
) -> Result<(), AdapterError> {
    fn resolve(
        directory: &Path,
        value: &mut Option<String>,
        bare_command: bool,
    ) -> Result<(), AdapterError> {
        let Some(current) = value.as_ref() else {
            return Ok(());
        };
        let path = Path::new(current);
        if path.is_relative() && (!bare_command || current.contains(['/', '\\'])) {
            *value = Some(unicode_path(&directory.join(path))?);
        }
        Ok(())
    }
    for named in &mut kubeconfig.clusters {
        if let Some(cluster) = &mut named.cluster {
            resolve(directory, &mut cluster.certificate_authority, false)?;
        }
    }
    for named in &mut kubeconfig.auth_infos {
        if let Some(auth) = &mut named.auth_info {
            resolve(directory, &mut auth.client_certificate, false)?;
            resolve(directory, &mut auth.client_key, false)?;
            resolve(directory, &mut auth.token_file, false)?;
            if let Some(exec) = &mut auth.exec {
                resolve(directory, &mut exec.command, true)?;
            }
        }
    }
    Ok(())
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
    // This is safe offline: the loader does not execute credential plugins at
    // this stage and no network calls happen.
    let mut structural = kubeconfig.clone();
    for named in &mut structural.auth_infos {
        if let Some(auth) = named.auth_info.as_mut() {
            auth.exec = None;
        }
    }
    Config::try_from(structural).map_err(|error| AdapterError::KubeconfigInvalid {
        source: source.to_owned(),
        detail: describe(error),
    })?;

    Ok(summaries)
}

/// Clone a kubeconfig for one context and force credential helpers to be
/// non-interactive. Desktop execution never has a terminal contract.
pub(crate) fn noninteractive_for_context(
    kubeconfig: &Kubeconfig,
    context_name: &str,
) -> Result<Kubeconfig, String> {
    let mut normalized = kubeconfig.clone();
    let user_name = normalized
        .contexts
        .iter()
        .find(|named| named.name == context_name)
        .and_then(|named| named.context.as_ref())
        .and_then(|context| context.user.as_deref());
    let Some(user_name) = user_name else {
        return Ok(normalized);
    };
    let Some(exec) = normalized
        .auth_infos
        .iter_mut()
        .find(|named| named.name == user_name)
        .and_then(|named| named.auth_info.as_mut())
        .and_then(|auth| auth.exec.as_mut())
    else {
        return Ok(normalized);
    };
    match exec.interactive_mode {
        Some(ExecInteractiveMode::Always) => {
            Err("credential plugin requires interactive input, which is unavailable in k10s".into())
        }
        Some(ExecInteractiveMode::IfAvailable) | None => {
            exec.interactive_mode = Some(ExecInteractiveMode::Never);
            Ok(normalized)
        }
        Some(ExecInteractiveMode::Never) => Ok(normalized),
    }
}

/// Whether the named context resolves to a kubeconfig exec credential plugin.
/// Callers carry this fact into runtime auth error classification so generic
/// token-provider failures are never mislabeled as exec failures.
pub(crate) fn context_uses_exec(kubeconfig: &Kubeconfig, context_name: &str) -> bool {
    let user_name = kubeconfig
        .contexts
        .iter()
        .find(|named| named.name == context_name)
        .and_then(|named| named.context.as_ref())
        .and_then(|context| context.user.as_deref());
    let Some(user_name) = user_name else {
        return false;
    };
    kubeconfig
        .auth_infos
        .iter()
        .find(|named| named.name == user_name)
        .and_then(|named| named.auth_info.as_ref())
        .is_some_and(|auth| auth.exec.is_some())
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use kube::config::{ExecInteractiveMode, Kubeconfig};

    use super::{
        load_from_paths, noninteractive_for_context, resolve_relative_references, source_paths_from,
    };

    #[test]
    fn discovery_paths_are_explicit_or_ordered_environment_or_default() {
        let explicit = PathBuf::from("/chosen/config");
        assert_eq!(
            source_paths_from(
                Some(&explicit),
                Some("ignored".as_ref()),
                Some(Path::new("/home/u"))
            )
            .unwrap(),
            [explicit]
        );
        let joined = std::env::join_paths([Path::new("/first"), Path::new("/second")]).unwrap();
        assert_eq!(
            source_paths_from(None, Some(&joined), Some(Path::new("/home/u"))).unwrap(),
            [PathBuf::from("/first"), PathBuf::from("/second")]
        );
        assert_eq!(
            source_paths_from(None, None, Some(Path::new("/home/u"))).unwrap(),
            [PathBuf::from("/home/u/.kube/config")]
        );
    }

    #[test]
    fn exec_commands_with_either_platform_separator_are_relative_file_references() {
        let directory = Path::new("/config/root");
        for command in ["helpers/login", "helpers\\login"] {
            let mut config = kubeconfig("Never");
            config.auth_infos[0]
                .auth_info
                .as_mut()
                .unwrap()
                .exec
                .as_mut()
                .unwrap()
                .command = Some(command.into());
            resolve_relative_references(&mut config, directory).unwrap();
            assert_eq!(
                config.auth_infos[0]
                    .auth_info
                    .as_ref()
                    .unwrap()
                    .exec
                    .as_ref()
                    .unwrap()
                    .command
                    .as_deref(),
                Some(directory.join(command).to_str().unwrap())
            );
        }
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_kubeconfig_path_is_a_typed_error() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        let path = PathBuf::from(OsString::from_vec(vec![b'/', b't', b'm', b'p', b'/', 0xff]));
        assert!(matches!(
            load_from_paths(vec![path]),
            Err(crate::port::AdapterError::KubeconfigInvalid { .. })
        ));
    }

    #[test]
    fn frozen_snapshot_decodes_utf8_bom_and_utf16_bom() {
        let yaml = kubeconfig_yaml();
        let mut fixtures = vec![(
            "utf8",
            [vec![0xef, 0xbb, 0xbf], yaml.as_bytes().to_vec()].concat(),
        )];
        for (name, little_endian) in [("utf16le", true), ("utf16be", false)] {
            let mut bytes = if little_endian {
                vec![0xff, 0xfe]
            } else {
                vec![0xfe, 0xff]
            };
            for unit in yaml.encode_utf16() {
                bytes.extend(if little_endian {
                    unit.to_le_bytes()
                } else {
                    unit.to_be_bytes()
                });
            }
            fixtures.push((name, bytes));
        }
        for (name, bytes) in fixtures {
            let path =
                std::env::temp_dir().join(format!("k10s-encoding-{name}-{}", std::process::id()));
            std::fs::write(&path, bytes).unwrap();
            let (_, parsed, _, _) = load_from_paths(vec![path.clone()]).unwrap();
            assert_eq!(parsed.current_context.as_deref(), Some("exec-context"));
            std::fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn frozen_snapshot_rejects_invalid_utf8_without_a_bom() {
        let path = std::env::temp_dir().join(format!("k10s-invalid-utf8-{}", std::process::id()));
        std::fs::write(&path, [0xff, 0x41]).unwrap();
        assert!(matches!(
            load_from_paths(vec![path.clone()]),
            Err(crate::port::AdapterError::KubeconfigInvalid { .. })
        ));
        std::fs::remove_file(path).unwrap();
    }

    fn kubeconfig_yaml() -> String {
        serde_yaml::to_string(&kubeconfig("Never")).unwrap()
    }

    fn kubeconfig(mode: &str) -> Kubeconfig {
        serde_yaml::from_str(&format!(
            r#"apiVersion: v1
kind: Config
current-context: exec-context
clusters:
- name: cluster
  cluster:
    server: https://example.invalid
contexts:
- name: exec-context
  context:
    cluster: cluster
    user: exec-user
users:
- name: exec-user
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1
      command: helper
      interactiveMode: {mode}
"#
        ))
        .expect("fixture kubeconfig parses")
    }

    #[test]
    fn interactive_exec_policy_is_normalized() {
        let error = noninteractive_for_context(&kubeconfig("Always"), "exec-context")
            .expect_err("always-interactive plugins are unavailable");
        assert!(error.contains("requires interactive input"));

        for mode in ["IfAvailable", "Never"] {
            let normalized = noninteractive_for_context(&kubeconfig(mode), "exec-context")
                .expect("non-interactive policy is accepted");
            let exec = normalized.auth_infos[0]
                .auth_info
                .as_ref()
                .and_then(|auth| auth.exec.as_ref())
                .expect("exec config remains present");
            assert_eq!(exec.interactive_mode, Some(ExecInteractiveMode::Never));
        }

        let mut omitted = kubeconfig("Never");
        omitted.auth_infos[0]
            .auth_info
            .as_mut()
            .and_then(|auth| auth.exec.as_mut())
            .expect("exec config exists")
            .interactive_mode = None;
        let normalized = noninteractive_for_context(&omitted, "exec-context")
            .expect("an omitted policy is forced non-interactive");
        let exec = normalized.auth_infos[0]
            .auth_info
            .as_ref()
            .and_then(|auth| auth.exec.as_ref())
            .expect("exec config remains present");
        assert_eq!(exec.interactive_mode, Some(ExecInteractiveMode::Never));
    }
}
