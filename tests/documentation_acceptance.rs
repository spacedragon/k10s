//! Prevent operator documentation from silently falling behind configuration.

use std::fs;
use std::path::{Path, PathBuf};

use k10s_protocol::{
    CAPABILITY_POD_PORT_FORWARD, CAPABILITY_SERVICE_PORT_FORWARD, CONTROL_PATH, EXEC_PATH,
    LOGS_PATH, PROTOCOL_MINOR,
};
use k10s_server::ServerConfig;
use k10s_server::port_forward::{
    MAX_SESSION_CONNECTIONS, MAX_SESSIONS, MAX_TOTAL_CONNECTIONS, PortForwardManager,
};

fn root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

fn read(relative: &str) -> String {
    fs::read_to_string(root().join(relative))
        .unwrap_or_else(|error| panic!("cannot read {relative}: {error}"))
}

fn normalized(document: &str) -> String {
    document.split_whitespace().collect::<Vec<_>>().join(" ")
}

#[test]
fn desktop_port_forward_security_contract_is_documented() {
    let protocol = normalized(&read("docs/protocol.md"));
    let configuration = normalized(&read("docs/configuration.md"));
    let security_source = read("docs/security.md");
    let troubleshooting = normalized(&read("docs/troubleshooting.md"));

    for capability in [CAPABILITY_SERVICE_PORT_FORWARD, CAPABILITY_POD_PORT_FORWARD] {
        for (guide, document) in [("protocol", &protocol), ("configuration", &configuration)] {
            assert!(
                document.contains(&format!("`{capability}`")),
                "{guide} guide is missing capability `{capability}`"
            );
        }
    }
    assert!(
        protocol.contains(&format!("minor `{PROTOCOL_MINOR}`")),
        "protocol guide's current minor must match the implementation"
    );
    for required in [
        "exact core/v1 Pod identity including UID",
        "regular container",
        "declared numeric TCP port",
        "shared session manager",
        "Pod and Service sessions",
        "context switch",
        "shutdown",
    ] {
        assert!(
            protocol.contains(required),
            "protocol guide is missing port-forward contract: {required}"
        );
    }
    assert!(
        protocol.contains(&format!(
            "terminal snapshots are retained for {} seconds",
            PortForwardManager::TERMINAL_RETENTION.as_secs()
        )),
        "protocol guide's terminal retention must match the manager"
    );
    for required in [
        "legacy `{service, port, localPort}` shape",
        "accepts both the legacy and target-discriminated Service shapes",
        "Pod starts use the target-discriminated shape",
        "`requestedLocalPort`",
        "lossy",
        "automatic `0` becomes the assigned explicit port on Retry",
    ] {
        assert!(
            protocol.contains(required),
            "protocol guide is missing port-forward compatibility contract: {required}"
        );
    }

    for required in [
        "desktop-only",
        "standalone server and browser deployment advertise neither",
        "Blank or `0` selects an available local port",
        "explicit port must be in `1..=65535`",
        "global Port Forwards window manages both Pod and Service sessions",
    ] {
        assert!(
            configuration.contains(required),
            "configuration guide is missing port-forward contract: {required}"
        );
    }
    assert!(
        configuration.contains(&format!("`{}`", std::net::Ipv4Addr::LOCALHOST)),
        "configuration guide must document the implementation's loopback address"
    );
    for expected in [
        format!("{MAX_SESSIONS} active sessions"),
        format!("{MAX_TOTAL_CONNECTIONS} accepted connections globally"),
        format!("{MAX_SESSION_CONNECTIONS} per session"),
    ] {
        assert!(
            configuration.contains(&expected),
            "documented port-forward limit does not match implementation: {expected}"
        );
    }

    let direct_pod_security = security_source
        .split_once("### Direct Pod forwarding")
        .map(|(_, section)| normalized(section))
        .expect("security guide must give direct Pod forwarding its own RBAC section");
    for required in [
        "exact core/v1 Pod identity and UID",
        "named regular container",
        "declared numeric TCP port",
        "`get` on `pods`",
        "`create` on `pods/portforward`",
    ] {
        assert!(
            direct_pod_security.contains(required),
            "security guide is missing port-forward contract: {required}"
        );
    }

    for required in ["local port is in use", "no ready endpoint"] {
        assert!(
            troubleshooting.contains(required),
            "troubleshooting guide is missing port-forward contract: {required}"
        );
    }
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
fn embeddable_server_defaults_match_documentation() {
    let defaults = ServerConfig::default();
    let docs = read("docs/configuration.md");
    let expected = [
        (
            "access_token",
            if defaults.access_token.is_empty() {
                "empty".to_owned()
            } else {
                defaults.access_token.clone()
            },
        ),
        (
            "startup_readiness_delay",
            defaults.startup_readiness_delay.as_millis().to_string(),
        ),
        (
            "probe_drain_grace",
            defaults.probe_drain_grace.as_millis().to_string(),
        ),
        (
            "hello_timeout",
            format!("{} s", defaults.hello_timeout.as_secs()),
        ),
        (
            "graceful_flush_timeout",
            format!("{} ms", defaults.graceful_flush_timeout.as_millis()),
        ),
        (
            "max_frame_size",
            format!("{} MiB", defaults.max_frame_size >> 20),
        ),
        (
            "max_message_size",
            format!("{} MiB", defaults.max_message_size >> 20),
        ),
        (
            "max_unauthenticated_connections",
            defaults.max_unauthenticated_connections.to_string(),
        ),
        (
            "max_authenticated_connections",
            defaults.max_authenticated_connections.to_string(),
        ),
        (
            "outbound_queue_capacity",
            defaults.outbound_queue_capacity.to_string(),
        ),
        (
            "max_resource_subscriptions_per_session",
            defaults.max_resource_subscriptions_per_session.to_string(),
        ),
        (
            "snapshot_rows_per_chunk",
            defaults.snapshot_rows_per_chunk.to_string(),
        ),
        (
            "drain_grace_timeout",
            format!("{} ms", defaults.drain_grace_timeout.as_millis()),
        ),
        (
            "drain_timeout",
            format!("{} s", defaults.drain_timeout.as_secs()),
        ),
        ("capabilities", defaults.capabilities.join(", ")),
        (
            "max_stream_frame_size",
            format!("{} KiB", defaults.max_stream_frame_size >> 10),
        ),
        (
            "max_stream_message_size",
            format!("{} KiB", defaults.max_stream_message_size >> 10),
        ),
        (
            "stream_hello_timeout",
            format!("{} s", defaults.stream_hello_timeout.as_secs()),
        ),
        (
            "stream_rate_budget_bytes_per_sec",
            format!("{} KiB/s", defaults.stream_rate_budget_bytes_per_sec >> 10),
        ),
        (
            "max_stream_connections",
            defaults.max_stream_connections.to_string(),
        ),
        (
            "resume_max_journal_entries",
            format!(
                "{},{:03}",
                defaults.resume_max_journal_entries / 1_000,
                defaults.resume_max_journal_entries % 1_000
            ),
        ),
        (
            "resume_max_sessions",
            defaults.resume_max_sessions.to_string(),
        ),
        (
            "resume_entry_max_age",
            format!("{} s", defaults.resume_entry_max_age.as_secs()),
        ),
    ];
    for (field, value) in expected {
        let row = format!("| `{field}` | {value} |");
        assert!(
            docs.contains(&row),
            "documented default does not match implementation: {row}"
        );
    }
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
        "proxy_set_header Host $http_host",
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
    for required in [
        "major `1`".to_owned(),
        format!("minor `0..={PROTOCOL_MINOR}`"),
        "`resyncRequired`".to_owned(),
    ] {
        assert!(protocol.contains(&required), "missing contract: {required}");
    }

    for route in [CONTROL_PATH, LOGS_PATH, EXEC_PATH] {
        assert!(
            protocol.contains(&format!("`{route}`")),
            "protocol guide is missing canonical route `{route}`"
        );
    }

    let kubeconfig_loader = read("crates/k10s-backend/src/kube/config.rs");
    assert!(
        kubeconfig_loader.contains("ExecInteractiveMode::IfAvailable")
            && kubeconfig_loader.contains("ExecInteractiveMode::Never"),
        "backend no longer normalizes exec credential plugins to non-interactive mode"
    );
    for document in [&deployment, &security] {
        assert!(document.contains("exec credential plugin"));
        assert!(normalized(document).contains("plugin failure disables only its context"));
    }
}

#[test]
fn real_kind_visual_baseline_is_reproducible_and_safe() {
    let design = read("docs/design/08-improvements.html");
    let archive = read("docs/design/README.md");
    let capture = read("docs/testing/real-kind-visual-validation.md");
    let test = read("tests/browser/real-kind-visual.spec.ts");

    assert!(design.contains("issue-159/design-08-reference.png"));
    assert!(archive.contains("authoritative visual artifact"));
    for required in [
        "1280 × 800",
        "egui dark theme",
        "kind-bunyip",
        "ImagePullBackOff",
        "completed Job",
        "StatefulSet",
        "dense-list",
        "Windows native-surface fallback",
        "never replace the real backend with `--fake`",
    ] {
        assert!(
            capture.contains(required),
            "missing capture contract: {required}"
        );
    }
    for required in ["K10S_REAL_KIND", "kind-bunyip", "Complete", "StatefulSets"] {
        assert!(
            test.contains(required),
            "missing real-kind assertion: {required}"
        );
    }
}
