//! Lossless YAML decoding and immutable Kubernetes identity extraction.

use k10s_protocol::YamlDiagnostic;
use serde_json::Value;

use crate::port::Gvk;

#[derive(Debug)]
pub(crate) struct ParsedManifest {
    pub(crate) object: kube::core::DynamicObject,
    pub(crate) gvk: Gvk,
    pub(crate) name: String,
    pub(crate) namespace: Option<String>,
    pub(crate) uid: String,
    pub(crate) resource_version: String,
}

pub(crate) fn parse(input: &str) -> Result<ParsedManifest, Vec<YamlDiagnostic>> {
    let yaml: serde_yaml_ng::Value = serde_yaml_ng::from_str(input).map_err(|error| {
        vec![YamlDiagnostic {
            line: error
                .location()
                .map_or(1, |location| location.line() as u32),
            message: "the YAML document could not be parsed".into(),
        }]
    })?;
    let value = serde_json::to_value(yaml)
        .map_err(|_| diagnostic("the YAML document cannot be represented as JSON"))?;
    let object: kube::core::DynamicObject = serde_json::from_value(value.clone())
        .map_err(|_| diagnostic("the YAML document is not a Kubernetes object"))?;
    let field = |pointer: &str, label: &str| {
        value
            .pointer(pointer)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| diagnostic(&format!("missing required field {label}")))
    };
    let api_version = field("/apiVersion", "apiVersion")?;
    let kind = field("/kind", "kind")?;
    let name = field("/metadata/name", "metadata.name")?;
    let uid = field("/metadata/uid", "metadata.uid")?;
    let resource_version = field("/metadata/resourceVersion", "metadata.resourceVersion")?;
    let namespace = value
        .pointer("/metadata/namespace")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let (group, version) = match api_version.split_once('/') {
        Some((group, version)) => (group.to_owned(), version.to_owned()),
        None => (String::new(), api_version),
    };
    Ok(ParsedManifest {
        object,
        gvk: Gvk::new(group, version, kind),
        name,
        namespace,
        uid,
        resource_version,
    })
}

fn diagnostic(message: &str) -> Vec<YamlDiagnostic> {
    vec![YamlDiagnostic {
        line: 1,
        message: message.into(),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_immutable_identity_and_bad_yaml_without_echoing_input() {
        let missing = parse("apiVersion: v1\nkind: Secret\nmetadata:\n  name: x\n").unwrap_err();
        assert!(missing.iter().any(|d| d.message.contains("metadata.uid")));
        let secret = "token: TOP-SECRET\n  broken";
        let broken = parse(secret).unwrap_err();
        assert!(broken.iter().all(|d| !d.message.contains("TOP-SECRET")));
    }
}
