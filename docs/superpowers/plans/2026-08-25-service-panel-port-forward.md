# k10s Service Panel and Desktop Port Forward Implementation Plan

**Goal:** Add a connected Services panel to the shared k10s workspace and provide bounded, loopback-only Service port-forward sessions in the native desktop application.

**Architecture:** Service list/detail data continues through the normalized protocol and `BackendKernel`. A dedicated server-owned `PortForwardManager` binds local loopback listeners, while a backend-owned `PortForwardConnector` resolves an exact Service UID through EndpointSlices to one ready Pod UID and opens kube-rs port-forward byte streams. Desktop enables the capability on its embedded server; standalone/web remains disabled.

**Design:** [Service Panel and Desktop Port Forward Design](../specs/2026-08-25-service-panel-port-forward-design.md)

**Tech Stack:** Existing Rust workspace, egui/eframe 0.36.1, Axum/Tokio, kube 4.2.0 with WebSocket support, Kubernetes core/v1 Service and discovery.k8s.io/v1 EndpointSlice APIs.

---

## Implementation rules

- Work task-by-task in the order below; later tasks depend on earlier contracts.
- Use TDD: add the focused failing test, run it to confirm the intended failure, implement the smallest behavior, then run the focused and affected suites.
- All Cargo commands use `--locked`.
- UI code never imports kube-rs, parses Service YAML, binds sockets, or selects Pods.
- Only `k10s-server` binds local listeners; only `k10s-backend` talks to Kubernetes.
- Port forwarding does not enter the durable Kubernetes mutation `OperationEngine`.
- Do not advertise `service.portForward` until Task 7 completes and the security/lifecycle tests pass.
- Preserve unrelated changes because other agents or worktrees may be active.

## Task 1: Define normalized Service and port-forward contracts

**Files:**

- Create: `crates/k10s-protocol/src/port_forward.rs`
- Modify: `crates/k10s-protocol/src/{lib,resource,bootstrap}.rs`
- Modify: `crates/k10s-protocol/tests/{resource_contract,golden_protocol}.rs`
- Create: `crates/k10s-protocol/tests/port_forward_contract.rs`
- Modify: `docs/protocol.md`

- [ ] Add failing serialization tests for `ServiceProjection`, `ServicePort`, `TargetPort`, TCP/UDP protocol values, optional `ResourceDetailResponse.projection`, and stable ordering.
- [ ] Add failing request/response tests for `portForward.start`, `portForward.stop`, `portForward.list`, session snapshots, session revisions, and the `portForward.sessions` subscription selector.
- [ ] Test validation rules: exact core/v1 Service identity, port selected by name or number, local port `0..=65535`, non-empty session IDs, and safe terminal failures.
- [ ] Test backward decoding of resource details without a projection and bump protocol minor version without changing the major version.
- [ ] Run `cargo test --locked -p k10s-protocol`; expect unresolved contracts.
- [ ] Implement normalized payloads and exports. Keep all Kubernetes and socket types out of the protocol crate.
- [ ] Re-run protocol tests and `cargo fmt --all -- --check`; expect PASS.
- [ ] Commit `feat: define service port-forward contracts`.

## Task 2: Project Service list and detail data through the backend

**Files:**

- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-backend/src/kube/{normalize,read}.rs`
- Modify: `crates/k10s-protocol/src/resource.rs`
- Modify: `crates/k10s-backend/tests/{resource_normalization,resource_details,kube_contract}.rs`
- Modify: `crates/k10s-server/tests/{resource_loopback,kube_detail_loopback}.rs`

- [ ] Add failing fake and recorded-API tests for core/v1 Service lists, namespace filtering, stable sorting, Service type, cluster IP/headless state, selector, session affinity, traffic policy, named/unnamed ports, numeric/named target ports, node ports, protocol, and appProtocol.
- [ ] Add failing tests proving the UI-facing projection contains no raw Kubernetes object and no credential-bearing fields.
- [ ] Run the focused backend tests; expect Service normalization/projection failures.
- [ ] Extend the existing resource list/detail path. Use the generic normalized list row for watches and an optional `ResourceProjection::Service` for structured details.
- [ ] Keep YAML generation and events on the existing authoritative detail path.
- [ ] Run backend tests plus `cargo test --locked -p k10s-server --test resource_loopback --test kube_detail_loopback`; expect PASS.
- [ ] Commit `feat: project kubernetes services`.

## Task 3: Add the Services workspace window and read-only UI

**Files:**

- Modify: `crates/k10s-ui/src/workspace/{mod,window,guard}.rs`
- Create: `crates/k10s-ui/src/workspace/service.rs`
- Modify: `crates/k10s-ui/src/ui/{mod,launcher,window}.rs`
- Create: `crates/k10s-ui/src/ui/service_window.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Create: `crates/k10s-ui/src/ui/detail/service.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/tests/{workspace_state,ui_shell,ui_details}.rs`
- Create: `crates/k10s-ui/tests/ui_services.rs`
- Modify: `apps/k10s-web/src/lib.rs`

- [ ] Add failing pure workspace tests for a singleton Services window, launcher highlight/focus, geometry, namespace/search/sort state, selection, integrated details, pop-out details, and context-switch reset.
- [ ] Add failing egui tests for the Network launcher group, list columns, loading/empty/filtered-empty/stale/gone states, Ports tab, structured port labels, and accessibility names.
- [ ] Add a browser semantic-host test proving Services can be listed and inspected without exposing Start/Stop controls.
- [ ] Run `cargo test --locked -p k10s-ui --test workspace_state --test ui_services`; expect missing window behavior.
- [ ] Implement `WindowKind::Services`, `LauncherItem::Services`, `WindowContent::Services`, and rendering from normalized feeds only.
- [ ] Subscribe to core/v1 Service rows through the same bounded resource-watch machinery used by workload windows.
- [ ] Run UI suites and `cargo check --locked -p k10s-web --target wasm32-unknown-unknown`; expect PASS.
- [ ] Commit `feat: add services panel`.

## Task 4: Implement exact Service-to-Pod resolution and connector seam

**Files:**

- Create: `crates/k10s-backend/src/port_forward.rs`
- Create: `crates/k10s-backend/src/kube/port_forward.rs`
- Modify: `crates/k10s-backend/src/{lib,kernel,port}.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Create: `crates/k10s-backend/tests/port_forward_resolution.rs`
- Modify: `crates/k10s-backend/src/testkit.rs`

- [ ] Add failing recorded-API tests for exact Service UID validation, recreated Service rejection, named and numeric port selection, EndpointSlice label scoping, ready/not-ready endpoints, same-namespace Pod targetRefs, deterministic Pod name/UID ordering, numeric endpoint ports, and Pod UID revalidation.
- [ ] Add rejection tests for UDP/SCTP, ExternalName, missing or ambiguous port, no ready endpoint, endpoint without Pod targetRef, cross-namespace target, forbidden Service/EndpointSlice/Pod calls, and sanitized API failures.
- [ ] Add connector tests proving `resolve_service_port` returns only backend-owned identifiers and `connect` opens the requested numeric Pod port through kube-rs.
- [ ] Run `cargo test --locked -p k10s-backend --test port_forward_resolution`; expect missing connector behavior.
- [ ] Implement cloneable `PortForwardConnector`, `ResolvedPortForward`, and an opaque boxed async read/write stream. Do not place streams in `QueryResult` or expose kube-rs types outside the backend crate.
- [ ] Use `discovery.k8s.io/v1` EndpointSlices only; do not add selector, legacy Endpoints, or raw-IP fallbacks.
- [ ] Run the focused test, backend suite, and clippy; expect PASS.
- [ ] Commit `feat: resolve service port-forward targets`.

## Task 5: Build the bounded server PortForwardManager

**Files:**

- Create: `crates/k10s-server/src/port_forward.rs`
- Modify: `crates/k10s-server/src/{lib,lifecycle,config}.rs`
- Create: `crates/k10s-server/tests/port_forward_manager.rs`
- Modify: `crates/k10s-server/tests/{shutdown,budget_config}.rs`

- [ ] Add failing tests proving listeners bind only `127.0.0.1`, local port `0` returns an assigned port, explicit occupied ports fail without leaking a task, and no request field can select another interface.
- [ ] Add failing lifecycle tests for Starting/Active/Stopping/Stopped/Failed, duplicate Service UID + port focus, multiple independent sessions, idempotent Stop, terminal retention/expiry, and monotonically revised snapshots.
- [ ] Add data-path tests for one connector stream per accepted local TCP connection, bidirectional byte flow, per-connection isolation, client disconnect, upstream disconnect, and failed pinned Pod sessions.
- [ ] Add hard-limit tests for 16 sessions, 32 global connections, 8 connections per session, bounded buffers, and overload errors.
- [ ] Add shutdown tests proving listeners and pumps are cancelled and joined before the server runtime exits and their ports can immediately be rebound.
- [ ] Run `cargo test --locked -p k10s-server --test port_forward_manager`; expect missing manager behavior.
- [ ] Implement the manager under the server lifecycle cancellation token. Bind before reporting Active and never auto-retarget a failed session.
- [ ] Run manager, shutdown, budget, and system-shutdown suites; expect PASS without advertised capability.
- [ ] Commit `feat: manage bounded port forwards`.

## Task 6: Connect control requests, subscriptions, and client state

**Files:**

- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-server/src/{auth,config}.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Create: `crates/k10s-server/tests/port_forward_loopback.rs`
- Create: `crates/k10s-ui/tests/port_forward_state.rs`
- Modify: `crates/k10s-server/tests/{control_socket,resume,security,backpressure}.rs`

- [ ] Add failing real-control-socket tests for start/list/stop dispatch, authentication, capability negotiation, disabled-server rejection, malformed payloads, request cancellation, typed failures, and session snapshot subscription.
- [ ] Add reconnect tests proving active data forwarding survives control loss and `portForward.list` plus `portForward.sessions` reconstructs client state without duplicate sessions.
- [ ] Add scheduler tests assigning lifecycle events a bounded/coalescible priority that cannot starve control responses or operation terminal events.
- [ ] Add client-state tests for pending Start, Active response, reordered/stale revisions, Failed/Stopped transitions, duplicate request suppression, Retry, Stop, and terminal cleanup.
- [ ] Run focused server and UI tests; expect missing dispatch/state behavior.
- [ ] Wire requests through authenticated control envelopes to `PortForwardManager`; enforce the server configuration gate before target resolution or binding.
- [ ] Implement client request correlation and authoritative session storage. Never infer Active solely from a button click.
- [ ] Run control, security, backpressure, resume, loopback, and client-state suites; expect PASS.
- [ ] Commit `feat: connect port-forward sessions`.

## Task 7: Enable desktop controls and context-switch guards

**Files:**

- Modify: `apps/k10s-desktop/src/lib.rs`
- Modify: `apps/k10s-server/src/main.rs`
- Modify: `crates/k10s-server/src/config.rs`
- Modify: `crates/k10s-ui/src/ui/detail/service.rs`
- Modify: `crates/k10s-ui/src/workspace/guard.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `apps/k10s-desktop/tests/embedded_lifecycle.rs`
- Create: `apps/k10s-desktop/tests/port_forward.rs`
- Modify: `crates/k10s-ui/tests/{ui_services,workspace_state}.rs`
- Modify: `crates/k10s-server/tests/{standalone_startup,security}.rs`

- [ ] Add failing desktop tests proving the embedded server advertises `service.portForward`, renders Start/Stop, accepts automatic and explicit local ports, copies a loopback address, and stops sessions on desktop shutdown.
- [ ] Add failing standalone/web tests proving the capability is absent and requests are rejected even if a client manually sends them.
- [ ] Add UI tests for blank/zero/invalid/occupied local ports, active-session display, selected Pod/remote port, safe failures, Retry, Stop, and active session count after closing/reopening the window.
- [ ] Add guard tests for context switch with active sessions: Cancel preserves context/sessions; Stop all and switch waits for terminal Stop before submitting the context switch. Window close remains unguarded.
- [ ] Run focused tests; expect desktop capability and controls to be absent.
- [ ] Enable forwarding only in `DesktopApp`'s embedded `ServerConfig`, then render controls from negotiated capability plus authoritative session state.
- [ ] Keep the standalone binary disabled with no CLI/environment opt-in in this version.
- [ ] Run desktop, standalone, UI, security, and WASM checks; expect PASS.
- [ ] Commit `feat: enable desktop service port forwarding`.

## Task 8: Verify kind behavior, cross-platform lifecycle, and documentation

**Files:**

- Create: `tests/kind/port-forward.yaml`
- Create: `crates/k10s-server/tests/kind_port_forward.rs`
- Modify: `.github/workflows/ci.yml`
- Modify: `docs/{configuration,security,troubleshooting,protocol}.md`
- Modify: `README.md`
- Modify: `tests/documentation_acceptance.rs`
- Modify: `apps/k10s-desktop/tests/port_forward.rs`

- [ ] Add an ignored real-kind test that creates a Deployment and ClusterIP Service, waits for a ready EndpointSlice, starts an automatic local port, sends HTTP through it, stops it, proves the port is released, and repeats with an explicit port.
- [ ] Add real-kind negative cases for forbidden `pods/portforward`, no ready endpoint, Service UID replacement, UDP Service port, and Pod deletion during an active session.
- [ ] Add a Windows loopback lifecycle test using a fake connector: automatic bind, bidirectional bytes, Stop, port reuse, and process shutdown. Do not require a Windows-accessible Kubernetes cluster in CI.
- [ ] Add documentation acceptance assertions for desktop-only availability, loopback binding, RBAC (`get services`, `list endpointslices`, `get pods`, `create pods/portforward`), limits, unsupported Service types, context-switch behavior, and troubleshooting occupied ports/no endpoints.
- [ ] Run the kind test and confirm it fails before fixtures/CI wiring are complete.
- [ ] Wire the kind test to the Linux self-hosted runner and the loopback lifecycle test to the Windows self-hosted runner, preserving persistent local Cargo caches.
- [ ] Run the complete verification gate below and fix every failure.
- [ ] Commit `test: verify desktop service port forwarding`.

## Verification gate

Run locally where supported:

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-targets
cargo check --locked -p k10s-web --target wasm32-unknown-unknown
cargo test --locked -p k10s-server --test kind_port_forward -- --ignored --nocapture
npm ci
npx playwright test
```

Required CI outcomes:

- Protocol, backend, server, UI, desktop, WASM, browser, and documentation suites pass.
- Linux kind E2E proves real Service-to-Pod traffic and all negative cases.
- Windows runner proves native loopback listener and shutdown lifecycle.
- Standalone/web never advertises or accepts port forwarding.
- Every listener is loopback-only; every target is bound to exact Service and Pod UIDs.
- Limits, cancellation, reconnect reconstruction, context switching, and shutdown are covered.
- Release packaging remains green on Linux/OCI and Windows.

The feature is complete only after the capability is enabled in desktop, all checks are green, review blockers are resolved, and the implementation PR is merged.
