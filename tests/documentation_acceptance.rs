//! Prevent operator documentation from silently falling behind configuration.

use std::fs;
use std::path::{Path, PathBuf};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    fs::read_to_string(root().join(relative))
        .unwrap_or_else(|error| panic!("cannot read {relative}: {error}"))
}

fn assert_documented(document: &str, values: impl IntoIterator<Item = String>) {
    for value in values {
        assert!(
            document.contains(&format!("`{value}`")),
            "operator documentation is missing `{value}`"
        );
    }
}

fn quoted_values(source: &str, prefix: &str) -> Vec<String> {
    source
        .match_indices(prefix)
        .filter_map(|(offset, _)| {
            let rest = &source[offset + prefix.len()..];
            rest.split_once('"').map(|(value, _)| value.to_owned())
        })
        .collect()
}

#[test]
fn every_standalone_input_is_documented() {
    let source = read("apps/k10s-server/src/main.rs");
    let docs = read("docs/configuration.md");
    let mut inputs = quoted_values(&source, "env::var(\"");
    inputs.extend(quoted_values(&source, "env::var_os(\""));
    inputs.extend(
        [
            "--fake",
            "--kubeconfig",
            "--token-file",
            "--listen",
            "--shutdown-file",
        ]
        .into_iter()
        .map(str::to_owned),
    );
    inputs.sort();
    inputs.dedup();
    assert_documented(&docs, inputs);
}

#[test]
fn every_embeddable_server_config_field_is_documented() {
    let source = read("crates/k10s-server/src/config.rs");
    let struct_body = source
        .split_once("pub struct ServerConfig {")
        .unwrap()
        .1
        .split_once("\n}")
        .unwrap()
        .0;
    let fields = struct_body.lines().filter_map(|line| {
        let line = line.trim();
        line.strip_prefix("pub ")
            .and_then(|field| field.split_once(':'))
            .map(|(name, _)| name.to_owned())
    });
    assert_documented(&read("docs/configuration.md"), fields);
}

#[test]
fn operational_contracts_have_acceptance_coverage() {
    let configuration = read("docs/configuration.md");
    let deployment = read("docs/deployment.md");
    let security = read("docs/security.md");
    let troubleshooting = read("docs/troubleshooting.md");
    let protocol = read("docs/protocol.md");

    for required in [
        "`K10S_ACCESS_TOKEN_FILE` wins over `K10S_ACCESS_TOKEN`",
        "`KUBECONFIG`",
        "never falls back to `--fake`",
    ] {
        assert!(
            configuration.contains(required),
            "missing contract: {required}"
        );
    }
    for required in [
        "`/healthz`",
        "`/readyz`",
        "`503 starting\\n`",
        "`503 initialization failed\\n`",
        "`503 draining\\n`",
        "`200 ready\\n`",
        "proxy_set_header Host $host",
        "proxy_set_header Origin $http_origin",
    ] {
        assert!(
            deployment.contains(required),
            "missing contract: {required}"
        );
    }
    for required in ["`[REDACTED]`", "correlation IDs", "never log payloads"] {
        assert!(security.contains(required), "missing contract: {required}");
    }
    assert!(troubleshooting.contains("correlation ID"));
    for required in ["major `1`", "minor `0..=1`", "`resyncRequired`"] {
        assert!(protocol.contains(required), "missing contract: {required}");
    }
}
