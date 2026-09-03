# Pod Port Forward and Unified Session Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add validated Pod TCP port forwarding to the desktop app and a singleton window that manages all Pod and Service port-forward sessions.

**Architecture:** Generalize the existing port-forward protocol and backend connector around a typed Service-or-Pod target while preserving the legacy Service wire shape under protocol-minor negotiation. Keep one server-side `PortForwardManager` and one authoritative client session feed, then add reusable start-modal state, structured Pod port rows, and a projection-only management window.

**Tech Stack:** Rust 2024, serde, Tokio, kube-rs, egui/eframe, existing k10s protocol/backend/server/UI test harnesses, cargo-nextest, kind.

**Design reference:** `docs/superpowers/specs/2026-09-03-pod-port-forward-management-design.md`

---

## File structure

- Modify `crates/k10s-protocol/src/port_forward.rs`: generalized target/request/session wire contracts, target-specific validation, Pod capability.
- Modify `crates/k10s-protocol/src/lib.rs`: export new protocol types and constants.
- Modify `crates/k10s-protocol/src/envelope.rs`: increment the protocol minor version if that constant is defined here.
- Modify `crates/k10s-protocol/tests/port_forward_contract.rs`: legacy/new wire compatibility and validation tests.
- Modify `crates/k10s-backend/src/port_forward.rs`: generalized backend request and connector method.
- Modify `crates/k10s-backend/src/kube/port_forward.rs`: exact Pod resolution and unchanged Service resolution dispatch.
- Modify `crates/k10s-backend/src/fake.rs`: generalized fake seam behavior.
- Modify `crates/k10s-backend/tests/port_forward_resolution.rs`: Pod resolution and rejection coverage.
- Modify `crates/k10s-server/src/port_forward.rs`: generalized session records, duplicate keys, retry, and terminal retention.
- Modify `crates/k10s-server/src/control.rs`: decode legacy/generalized starts and target-specific capability enforcement.
- Modify `crates/k10s-server/src/lifecycle.rs`: advertise both desktop capabilities and activate one shared manager.
- Modify `crates/k10s-server/tests/port_forward_loopback.rs`: mixed-target lifecycle and negotiated compatibility.
- Modify `crates/k10s-server/tests/kind_port_forward.rs`: automated real-kind Pod and Service loopback coverage.
- Modify `crates/k10s-ui/src/client/state.rs`: separate capability helpers and authoritative terminal-session retention.
- Modify `crates/k10s-ui/tests/port_forward_state.rs`: mixed snapshots, terminal retention, legacy conversion.
- Create `crates/k10s-ui/src/workspace/port_forward.rs`: focused, non-authoritative management-window view state.
- Modify `crates/k10s-ui/src/workspace/{mod.rs,window.rs,snapshot.rs}`: singleton Port Forwards window, commands, geometry persistence.
- Modify `crates/k10s-ui/tests/{workspace_state.rs,workspace_snapshot.rs}`: singleton and snapshot behavior.
- Create `crates/k10s-ui/src/ui/port_forward.rs`: reusable start modal and management table.
- Modify `crates/k10s-ui/src/ui/{mod.rs,launcher.rs,window.rs}`: route the new window, actions, launcher badge.
- Modify `crates/k10s-ui/src/ui/detail/{presentation.rs,pod.rs,service.rs}`: typed Pod rows and shared Service/Pod start modal.
- Modify `crates/k10s-ui/src/app.rs`: request orchestration, duplicate focus, retry, copy, and feed projection.
- Modify `crates/k10s-ui/tests/{ui_pod_details.rs,ui_services.rs,ui_shell.rs}` and add `crates/k10s-ui/tests/ui_port_forwards.rs`: interaction coverage.
- Modify `apps/k10s-desktop/tests/embedded_lifecycle.rs`: desktop capability coverage.
- Modify `docs/{protocol.md,configuration.md,security.md}`: generalized target and desktop-only behavior.

### Task 1: Generalize the protocol without breaking Service clients

**Files:**
- Modify: `crates/k10s-protocol/src/port_forward.rs`
- Modify: `crates/k10s-protocol/src/lib.rs`
- Modify: `crates/k10s-protocol/src/envelope.rs`
- Test: `crates/k10s-protocol/tests/port_forward_contract.rs`

- [ ] **Step 1: Write failing target and compatibility contract tests**

Add tests that assert both targets round-trip, `pod.portForward` is exported, legacy Service JSON still decodes, old Service session JSON converts to the generalized model, and Pod validation rejects the wrong GVK, missing namespace/UID/container, and non-positive remote ports at construction boundaries.

```rust
let target = PortForwardTarget::Pod {
    identity: pod_identity("api", "pod-uid"),
    container_name: "api".into(),
    remote_port: 8080,
};
let json = serde_json::to_value(PortForwardStartRequest::target(target.clone(), 8080))?;
assert_eq!(json["target"]["kind"], "pod");
assert_eq!(serde_json::from_value::<PortForwardStartRequest>(json)?.target(), &target);

let legacy = serde_json::json!({
    "service": service_identity("api", "svc-uid"),
    "port": { "kind": "number", "number": 80 },
    "localPort": 0
});
assert!(matches!(
    serde_json::from_value::<PortForwardStartRequest>(legacy)?.target(),
    PortForwardTarget::Service { .. }
));
```

- [ ] **Step 2: Run the focused test and verify failure**

Run: `cargo nextest run -p k10s-protocol --test port_forward_contract`

Expected: FAIL because `PortForwardTarget`, `CAPABILITY_POD_PORT_FORWARD`, and generalized decoding do not exist.

- [ ] **Step 3: Add the generalized in-memory model and explicit wire compatibility**

Implement `PortForwardTarget`, `PortForwardStartRequest`, and generalized session source fields. Use an internal untagged wire enum or custom serde implementation so the decoder accepts both:

```rust
#[serde(untagged)]
enum StartRequestWire {
    LegacyService {
        service: ResourceIdentity,
        port: PortForwardPortSelector,
        local_port: u16,
    },
    Target {
        target: PortForwardTarget,
        local_port: u16,
    },
}
```

Expose constructors/accessors rather than leaking the compatibility enum. Keep new Service clients serializing the legacy shape; serialize Pod starts with the tagged target shape. Add `CAPABILITY_POD_PORT_FORWARD: &str = "pod.portForward"`. Add conversion from legacy Service session snapshots, with requested local port derived from `local_addr`. Increment `PROTOCOL_MINOR`, not `PROTOCOL_MAJOR`.

- [ ] **Step 4: Run protocol tests and formatting**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-protocol --test port_forward_contract`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-protocol
git commit -m "feat(protocol): generalize port forward targets"
```

### Task 2: Resolve exact Pod targets in the backend

**Files:**
- Modify: `crates/k10s-backend/src/port_forward.rs`
- Modify: `crates/k10s-backend/src/kube/port_forward.rs`
- Modify: `crates/k10s-backend/src/fake.rs`
- Test: `crates/k10s-backend/tests/port_forward_resolution.rs`

- [ ] **Step 1: Add failing Pod resolution tests**

Extend the fixture API server with a Pod containing regular containers and TCP/UDP ports. Test exact UID/container/TCP success plus wrong UID, init/ephemeral-only container, missing container, undeclared port, UDP/SCTP, missing namespace, and forbidden `get pods`.

```rust
let resolved = connector.resolve(PortForwardRequest {
    context: "kind-k10s".into(),
    target: PortForwardTarget::Pod {
        identity: pod_identity("api", "pod-uid"),
        container_name: "api".into(),
        remote_port: 8080,
    },
}).await?;
assert_eq!(resolved.pod_name, "api");
assert_eq!(resolved.pod_uid, "pod-uid");
assert_eq!(resolved.pod_port, 8080);
```

- [ ] **Step 2: Verify the new tests fail**

Run: `cargo nextest run -p k10s-backend --test port_forward_resolution`

Expected: FAIL because the backend request and seam are Service-specific.

- [ ] **Step 3: Generalize the connector seam**

Rename `resolve_service_port` to `resolve`, make the backend request carry `PortForwardTarget`, and dispatch Service targets to the current resolver unchanged. For Pod targets, fetch core/v1 Pod, compare UID, search only `spec.containers`, verify the named container declares the numeric port with TCP default semantics, and return the existing opaque `ResolvedPortForward`. Map failures to stable categories; introduce `UnsupportedTarget` only if the existing `UnsupportedService` name cannot accurately represent Pod validation.

- [ ] **Step 4: Update the fake seam and run backend tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-backend --test port_forward_resolution`

Expected: PASS, including all pre-existing Service cases.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-backend
git commit -m "feat(backend): resolve declared pod ports"
```

### Task 3: Generalize the server manager and control protocol

**Files:**
- Modify: `crates/k10s-server/src/port_forward.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-server/src/lifecycle.rs`
- Test: `crates/k10s-server/tests/port_forward_loopback.rs`
- Test: `apps/k10s-desktop/tests/embedded_lifecycle.rs`

- [ ] **Step 1: Add failing mixed-target lifecycle tests**

Test that the embedded server advertises both capabilities, standalone remains disabled, Pod start/list/subscribe/stop uses the same manager, Pod duplicate identity is `(uid, container, port)`, Service duplicate behavior is unchanged, combined limits cover both targets, stopped/failed snapshots are retained until expiry, and context transition drains both.

- [ ] **Step 2: Add negotiated wire tests**

Exercise an old-minor client sending/receiving legacy Service shapes and a current-minor client receiving generalized mixed snapshots. With both Pod and Service sessions active, assert prior-minor list responses and events filter out Pod snapshots entirely and encode retained Service snapshots in the legacy shape. Assert Pod start is rejected without `pod.portForward` even if `service.portForward` exists.

- [ ] **Step 3: Run focused server tests and verify failure**

Run: `cargo nextest run -p k10s-server --test port_forward_loopback && cargo nextest run -p k10s-desktop --test embedded_lifecycle`

Expected: FAIL on missing Pod dispatch and capability.

- [ ] **Step 4: Generalize manager state and duplicate keys**

Store the typed target and requested local port in `SessionInner`. Replace Service-only duplicate matching with a private key enum:

```rust
enum SessionTargetKey {
    Service { uid: String, port: PortForwardPortSelector },
    Pod { uid: String, container: String, port: u16 },
}
```

Keep target key comparison independent of requested local port. Add a retry operation that starts from the failed snapshot's target/requested port without mutating the failed row unless a new active session succeeds. Preserve the existing transition gate, limits, loopback binding, per-connection streams, and failure threshold.

- [ ] **Step 5: Enforce capabilities and negotiated encoding in control dispatch**

Decode both start shapes, require `service.portForward` or `pod.portForward` according to the target, and never trust the client capability alone. Encode legacy Service snapshots for prior-minor connections and generalized snapshots for current-minor connections. Before legacy list/event encoding, filter out every Pod session so an older decoder never receives an unknown shape. Advertise both capabilities only from the desktop embedded lifecycle.

- [ ] **Step 6: Run server and desktop tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-server --test port_forward_loopback && cargo nextest run -p k10s-desktop --test embedded_lifecycle`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/k10s-server apps/k10s-desktop/tests/embedded_lifecycle.rs
git commit -m "feat(server): manage pod and service forwards together"
```

### Task 4: Make the UI client feed authoritative for mixed and terminal sessions

**Files:**
- Modify: `crates/k10s-ui/src/client/state.rs`
- Test: `crates/k10s-ui/tests/port_forward_state.rs`

- [ ] **Step 1: Write failing client-state tests**

Test separate Service/Pod capability helpers, mixed list/event application, stale revision rejection, retention of `Stopped` and `Failed`, removal only after an authoritative later list omits a session, reconnect reconstruction, and legacy Service snapshot conversion.

- [ ] **Step 2: Run the focused tests and verify failure**

Run: `cargo nextest run -p k10s-ui --test port_forward_state`

Expected: FAIL because terminal sessions are currently removed eagerly and only the Service capability is known.

- [ ] **Step 3: Implement capability and feed changes**

Add `service_port_forward_available`, `pod_port_forward_available`, and `any_port_forward_available`. Keep all snapshots received from list/events in `port_forward_sessions`, including terminal states. A fresh list replaces the map and is the only normal expiry signal. Generalize request construction while keeping Service starts on the legacy wire constructor.

- [ ] **Step 4: Run focused tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-ui --test port_forward_state`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui/src/client/state.rs crates/k10s-ui/tests/port_forward_state.rs
git commit -m "feat(ui): retain unified port forward sessions"
```

### Task 5: Add the singleton Port Forwards workspace model

**Files:**
- Create: `crates/k10s-ui/src/workspace/port_forward.rs`
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Modify: `crates/k10s-ui/src/workspace/window.rs`
- Modify: `crates/k10s-ui/src/workspace/snapshot.rs`
- Test: `crates/k10s-ui/tests/workspace_state.rs`
- Test: `crates/k10s-ui/tests/workspace_snapshot.rs`

- [ ] **Step 1: Write failing singleton and persistence tests**

Assert repeated launcher activation opens one window and focuses it; focusing a session stores only its ID; geometry/sort restore; active sessions and modal state never enter the snapshot; old snapshots still load.

- [ ] **Step 2: Run tests and verify failure**

Run: `cargo nextest run -p k10s-ui --test workspace_state --test workspace_snapshot`

Expected: FAIL because the launcher/window/content variants do not exist.

- [ ] **Step 3: Add focused window state**

Create:

```rust
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PortForwardWindowState {
    pub sort: Option<SortSpec>,
    pub focused_session: Option<String>,
}
```

Add `LauncherItem::PortForwards`, `WindowKind::PortForwards`, and `WindowContent::PortForwards`. Treat it as a singleton with Services-sized geometry. Add commands to focus a session and set its sort. Persist only the window kind, geometry, sort, and focused ID; ignore a stale focused ID harmlessly at render time.

- [ ] **Step 4: Run workspace tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-ui --test workspace_state --test workspace_snapshot`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui/src/workspace crates/k10s-ui/tests/workspace_state.rs crates/k10s-ui/tests/workspace_snapshot.rs
git commit -m "feat(ui): add port forwards workspace window"
```

### Task 6: Build the shared start modal and application actions

**Files:**
- Create: `crates/k10s-ui/src/ui/port_forward.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Test: `crates/k10s-ui/tests/ui_port_forwards.rs`

- [ ] **Step 1: Add failing modal state tests**

Cover remote-port default, blank and `0` normalization, invalid text, upper bound, disabled pending Start, Cancel, safe recoverable error preservation, success close, and duplicate success focusing the management window.

- [ ] **Step 2: Run the new test target and verify failure**

Run: `cargo nextest run -p k10s-ui --test ui_port_forwards`

Expected: FAIL because the module and test target behavior do not exist.

- [ ] **Step 3: Implement a pure modal model before rendering**

```rust
pub struct PortForwardStartModal {
    pub target: PortForwardTarget,
    pub remote_label: String,
    pub local_port_draft: String,
    pub pending: bool,
    pub error: Option<String>,
}

impl PortForwardStartModal {
    pub fn requested_port(&self) -> Result<u16, LocalPortError> {
        let value = self.local_port_draft.trim();
        if value.is_empty() { return Ok(0); }
        value.parse::<u16>().map_err(|_| LocalPortError)
    }
}
```

Render it with stable egui IDs and accessible widget labels. Generalize `PortForwardAction` to `OpenStart`, `Start`, `Stop`, `Retry`, `FocusSession`, and `CopyAddress`. Keep clipboard writes in the UI/application boundary, not the workspace model.

- [ ] **Step 4: Wire pending responses in `app.rs`**

Associate each pending start with its modal/target. On recoverable modal rejection, clear pending and preserve the draft/error. Associate each retry request with its source session ID and store retry-only presentation errors in an application-owned `BTreeMap<PortForwardSessionId, String>`. On local-port conflict, show “Local port is in use; start a new forward from the Pod or Service with another port.” on that failed row. Clear the overlay when the session expires, a later retry succeeds, or its authoritative revision changes. On success, close the modal, activate the singleton Port Forwards window, and focus the returned ID. Retry constructs a start from the retained failed snapshot. Never mutate the authoritative session feed optimistically.

- [ ] **Step 5: Run modal and existing Service tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-ui --test ui_port_forwards --test ui_services`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src/ui/port_forward.rs crates/k10s-ui/src/ui/mod.rs crates/k10s-ui/src/app.rs crates/k10s-ui/tests/ui_port_forwards.rs
git commit -m "feat(ui): add shared port forward start dialog"
```

### Task 7: Add Pod port rows and migrate the Service start surface

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/presentation.rs`
- Modify: `crates/k10s-ui/src/ui/detail/pod.rs`
- Modify: `crates/k10s-ui/src/ui/detail/service.rs`
- Test: `crates/k10s-ui/tests/ui_pod_details.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`

- [ ] **Step 1: Add failing Pod row tests**

Render a Pod with multiple regular-container ports. Assert Pod-spec order, port/container/name labels, TCP action only when `pod.portForward` is available and mutations are allowed, no action for UDP/SCTP/web, and the emitted target carries the exact loaded identity and container.

- [ ] **Step 2: Add failing Service migration tests**

Assert the Service Ports tab retains inline active/stop controls, but new Start opens the shared modal with blank/`0` automatic semantics and the correct Service target.

- [ ] **Step 3: Run focused tests and verify failure**

Run: `cargo nextest run -p k10s-ui --test ui_pod_details --test ui_services`

Expected: FAIL on missing Pod actions and old inline Service draft behavior.

- [ ] **Step 4: Keep structured ports in the Pod presentation**

Replace `Vec<String>` with a row projection containing `PodContainerPort` authority plus display labels. Add a **PORTS** section with one row per declaration and `Port Forward` only for TCP. Never parse `format_port` output back into a request.

- [ ] **Step 5: Route Service Start through the shared modal**

Remove duplicated inline local-port parsing while keeping Service session status, copy, and Stop rendering. Opening the modal pre-fills the resolved numeric remote port when known; if a named Service target is not yet numerically resolved, prefill the Service port and let the backend remain authoritative.

- [ ] **Step 6: Run focused tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-ui --test ui_pod_details --test ui_services`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/k10s-ui/src/ui/detail crates/k10s-ui/tests/ui_pod_details.rs crates/k10s-ui/tests/ui_services.rs
git commit -m "feat(ui): start forwards from pod ports"
```

### Task 8: Render the launcher badge and unified management window

**Files:**
- Modify: `crates/k10s-ui/src/ui/port_forward.rs`
- Modify: `crates/k10s-ui/src/ui/launcher.rs`
- Modify: `crates/k10s-ui/src/ui/window.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Test: `crates/k10s-ui/tests/ui_port_forwards.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`

- [ ] **Step 1: Add failing management-window tests**

Cover mixed Pod/Service rows, target/namespace/remote/local/status columns, stable sorting, copy, Stop, Retry, retry local-port-conflict overlay and cleanup, muted actionless Stopped, Failed reason, empty state, disconnected state, focused duplicate row, and expiry after a replacement list omits the terminal session.

- [ ] **Step 2: Add failing launcher tests**

Assert the Network group contains Port Forwards when either capability exists, filters by its label, opens a singleton, and badges only Starting/Active/Stopping.

- [ ] **Step 3: Run focused tests and verify failure**

Run: `cargo nextest run -p k10s-ui --test ui_port_forwards --test ui_shell`

Expected: FAIL because the new window is not routed or rendered.

- [ ] **Step 4: Implement the projection-only table**

Render directly from `ResourceFeed.port_forward_sessions`; do not copy sessions into window state. Give each row a stable ID based on session ID. Map actions strictly by state: copy after bind, stop for Starting/Active, disabled for Stopping, retry for Failed, none for Stopped. Present safe failure messages verbatim.

- [ ] **Step 5: Wire launcher and routing**

Pass capability/session-count data to `launcher::show`, add Port Forwards under Network after Services, and route `WindowContent::PortForwards` through the main window renderer. Focus the requested session row on duplicate or successful Start.

- [ ] **Step 6: Run UI tests**

Run: `cargo fmt --all -- --check && cargo nextest run -p k10s-ui --test ui_port_forwards --test ui_shell --test ui_command_palette`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/k10s-ui/src crates/k10s-ui/tests/ui_port_forwards.rs crates/k10s-ui/tests/ui_shell.rs crates/k10s-ui/tests/ui_command_palette.rs
git commit -m "feat(ui): manage all port forwards in one window"
```

### Task 9: Documentation and full regression verification

**Files:**
- Modify: `docs/protocol.md`
- Modify: `docs/configuration.md`
- Modify: `docs/security.md`
- Modify: `README.md` if it inventories desktop capabilities
- Modify: `crates/k10s-server/tests/kind_port_forward.rs`
- Test: `tests/documentation_acceptance.rs`

- [ ] **Step 1: Add or update documentation acceptance assertions**

Require documentation to name both capabilities, loopback-only binding, exact Pod UID/container/declared TCP validation, unified session management, and context-switch cleanup.

- [ ] **Step 2: Run documentation tests and verify failure**

Run: `cargo nextest run --test documentation_acceptance`

Expected: FAIL until the docs describe Pod forwarding.

- [ ] **Step 3: Update operator and security documentation**

Document that the desktop advertises `service.portForward` and `pod.portForward`, standalone/web advertise neither, Service and Pod targets share limits, and required Pod RBAC is `get pods` plus `create pods/portforward`. Include the blank/`0` local-port behavior and terminal-session retention.

- [ ] **Step 4: Run crate-level regression suites**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run -p k10s-protocol -p k10s-backend -p k10s-server -p k10s-ui -p k10s-desktop
cargo nextest run --test documentation_acceptance
```

Expected: PASS with no warnings.

- [ ] **Step 5: Extend and run the automated real-kind port-forward test**

In `crates/k10s-server/tests/kind_port_forward.rs`, extend the opt-in `K10S_KIND_KUBECONFIG` fixture to create a Pod with a declared TCP port. Start a Pod forward with local port `0`, exchange bytes twice across separate local connections, verify the session stays Active, list both Pod and Service target variants, then stop it and confirm the loopback listener closes. Preserve the existing skip behavior when the kind environment variable is absent.

Run: `K10S_KIND_KUBECONFIG=/absolute/path/to/kubeconfig cargo nextest run -p k10s-server --test kind_port_forward --run-ignored all`

Expected: PASS; both Pod and Service paths exchange traffic, mixed list snapshots decode, and Stop/application shutdown leave no listener.

- [ ] **Step 6: Commit documentation and any test-only fixes**

```bash
git add docs README.md tests/documentation_acceptance.rs crates/k10s-server/tests/kind_port_forward.rs
git commit -m "docs: document pod port forwarding"
```

- [ ] **Step 7: Inspect final scope**

Run: `git status --short && git log --oneline --decorate -10`

Expected: clean worktree; commits correspond to the nine tasks without unrelated changes.
