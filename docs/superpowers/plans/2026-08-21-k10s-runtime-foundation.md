# k10s Runtime Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a minimal native and web k10s application that authenticates and retrieves deterministic bootstrap data through the real versioned WebSocket server and Backend Kernel.

**Architecture:** The workspace establishes the permanent crate seams before full UI work begins. A deterministic fake Kubernetes adapter feeds `BackendKernel`; Axum exposes it through the control WebSocket; native and WASM clients share the same protocol state machine. Desktop embeds the server on a random loopback port, while the standalone server hosts web assets and a connection gate.

**Tech Stack:** Rust 1.97.1/edition 2024; Serde 1.0.229; serde_json 1.0.151; UUID 1.24.1; Tokio 1.53.1; tokio-util 0.7.19; Axum 0.8.9; ewebsock 0.8.0; eframe/egui 0.36.1; tracing 0.1.44.

---

## Scope

This plan implements protocol and deployment foundations only. The visible UI is a connection gate plus a bootstrap screen listing fake contexts and server status. Resource windows and operational workflows belong to Plan 2.

## File map

- `Cargo.toml` and `Cargo.lock`: workspace members, shared lints, exact core dependency versions, and the committed dependency graph.
- `rust-toolchain.toml`: Rust 1.97.1, rustfmt, Clippy, wasm target.
- `crates/k10s-protocol/src/{lib,ids,envelope,error,bootstrap,route}.rs`: target-neutral wire and route contract.
- `crates/k10s-backend/src/{lib,kernel,port,fake}.rs`: behavior-level Kubernetes port, Backend Kernel, and fake adapter.
- `crates/k10s-server/src/{lib,config,auth,control,outbound,probes,lifecycle}.rs`: Axum adapter, bounded priority output, health/readiness probes, and embeddable server.
- `crates/k10s-ui/src/{lib,client,connection,app}.rs`: shared UI and protocol client state.
- `apps/k10s-desktop/src/main.rs`: native entry and embedded-server launcher.
- `apps/k10s-web/src/lib.rs`: WASM entry.
- `apps/k10s-server/src/main.rs`: standalone server entry.
- `tests/fixtures/protocol/*.json`: golden protocol transcripts.

### Task 1: Scaffold the workspace and enforce dependency direction

**Files:**
- Create: `rust-toolchain.toml`
- Create: `Cargo.toml`
- Create: `Cargo.lock`
- Create: `crates/k10s-protocol/Cargo.toml`
- Create: `crates/k10s-protocol/src/lib.rs`
- Create: `crates/k10s-backend/Cargo.toml`
- Create: `crates/k10s-backend/src/lib.rs`
- Create: `crates/k10s-server/Cargo.toml`
- Create: `crates/k10s-server/src/lib.rs`
- Create: `crates/k10s-ui/Cargo.toml`
- Create: `crates/k10s-ui/src/lib.rs`
- Create: `apps/k10s-desktop/{Cargo.toml,src/main.rs}`
- Create: `apps/k10s-web/{Cargo.toml,src/lib.rs}`
- Create: `apps/k10s-server/{Cargo.toml,src/main.rs}`

- [ ] **Step 1: Write a failing workspace smoke test**

Create `crates/k10s-protocol/tests/workspace_contract.rs`:

```rust
#[test]
fn protocol_crate_has_no_platform_features() {
    assert_eq!(k10s_protocol::PROTOCOL_MAJOR, 1);
}
```

- [ ] **Step 2: Verify the empty workspace fails**

Run: `cargo test --locked -p k10s-protocol --test workspace_contract`

Expected: FAIL because the workspace and crate do not exist.

- [ ] **Step 3: Create the minimal workspace**

Pin Rust 1.97.1 and create all members above. Set workspace lints to deny unsafe code, missing debug implementations, and unused must-use results. Export only:

```rust
pub const PROTOCOL_MAJOR: u16 = 1;
pub const PROTOCOL_MINOR: u16 = 1;
```

Keep kube-rs and Tokio out of `k10s-protocol`; keep kube-rs out of `k10s-ui`. Run `cargo generate-lockfile` and commit `Cargo.lock`; every later Cargo command in all five plans uses `--locked`.

- [ ] **Step 4: Verify every target skeleton builds**

Run: `cargo test --locked -p k10s-protocol --test workspace_contract && cargo check --locked --workspace`

Expected: PASS with no warnings.

- [ ] **Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock rust-toolchain.toml crates apps
git commit -m "build: scaffold k10s runtime workspace"
```

### Task 2: Define the versioned control protocol

**Files:**
- Create: `crates/k10s-protocol/src/ids.rs`
- Create: `crates/k10s-protocol/src/envelope.rs`
- Create: `crates/k10s-protocol/src/error.rs`
- Create: `crates/k10s-protocol/src/bootstrap.rs`
- Create: `crates/k10s-protocol/src/route.rs`
- Modify: `crates/k10s-protocol/src/lib.rs`
- Create: `crates/k10s-protocol/tests/golden_protocol.rs`
- Create: `tests/fixtures/protocol/bootstrap-v1.0.json`
- Create: `tests/fixtures/protocol/bootstrap-v1.1.json`

- [ ] **Step 1: Write failing golden and unknown-message tests**

```rust
#[test]
fn bootstrap_response_matches_v1_fixture() {
    let frame = ServerFrame::response(
        RequestId::from_u128(1),
        BootstrapResponse::fixture(),
    );
    assert_eq!(serde_json::to_value(frame).unwrap(), fixture("bootstrap-v1.1.json"));
}

#[test]
fn unknown_kind_is_reported_without_panicking() {
    let raw = r#"{"kind":"future.notice","payload":{"x":1}}"#;
    assert_eq!(decode_client_frame(raw).unwrap_err().code, ErrorCode::UnsupportedMessage);
}

#[test]
fn current_client_decodes_previous_minor_and_ignores_optional_fields() {
    let frame = fixture("bootstrap-v1.0.json");
    assert!(decode_server_frame(frame).is_ok());
}

#[test]
fn application_routes_are_stable() {
    assert_eq!(CONTROL_PATH, "/api/v1/control");
    assert_eq!(LOGS_PATH, "/api/v1/logs");
    assert_eq!(EXEC_PATH, "/api/v1/exec");
}
```

- [ ] **Step 2: Verify protocol tests fail**

Run: `cargo test --locked -p k10s-protocol --test golden_protocol`

Expected: FAIL with unresolved protocol types.

- [ ] **Step 3: Implement IDs, envelopes, bootstrap payloads, and errors**

Use newtypes for `RequestId`, `SessionId`, `SubscriptionId`, `OperationId`, and `CorrelationId`. Parse the outer envelope as `{ kind, optional ids, payload: RawValue }`, then dispatch known payloads. Define `Hello`, `Welcome`, `Request`, `Response`, `CancelRequest`, `Subscribe`, `Subscribed`, `Unsubscribe`, `Complete`, `Ack`, `ResyncRequired`, `ErrorFrame`, `Ping`, `Pong`, and `ShutdownNotice`. Subscription payload is opaque in Plan 1 (`BootstrapStatus`) and extended with resource selectors in Plan 2; every server event carries a monotonic session sequence and `Ack` advances the resume cursor. `Hello` advertises major/minor plus capability strings; the server rejects incompatible majors and negotiates the lower minor/capability intersection. Requests carry an optional relative deadline; cancellation is idempotent. Add `#[serde(default)]` to compatible additions and ignore unknown object fields while rejecting unknown message kinds. Fix the three application route constants in `route.rs`. Model retryability as `Never | AfterReconnect | AfterRefresh | UserAction`.

- [ ] **Step 4: Run protocol tests and property round trips**

Run: `cargo test --locked -p k10s-protocol`

Expected: PASS; both v1.0 and v1.1 transcripts remain stable, the current decoder accepts the previous minor, and malformed/unknown frames return structured errors.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-protocol tests/fixtures/protocol
git commit -m "feat: define k10s websocket protocol"
```

### Task 3: Create the Backend Kernel and deterministic fake adapter

**Files:**
- Create: `crates/k10s-backend/src/port.rs`
- Create: `crates/k10s-backend/src/kernel.rs`
- Create: `crates/k10s-backend/src/fake.rs`
- Modify: `crates/k10s-backend/src/lib.rs`
- Create: `crates/k10s-backend/tests/kernel_bootstrap.rs`

- [ ] **Step 1: Write the failing kernel behavior test**

```rust
#[tokio::test]
async fn bootstrap_hides_credentials_and_reports_fake_contexts() {
    let kernel = BackendKernel::new(FakeKubernetes::standard());
    let result = kernel.query(Query::Bootstrap).await.unwrap();
    assert_eq!(result.context_names(), ["dev-local", "prod-readonly"]);
    assert!(!result.serialized().contains("token"));
}
```

- [ ] **Step 2: Verify the test fails**

Run: `cargo test --locked -p k10s-backend --test kernel_bootstrap`

Expected: FAIL because `BackendKernel` and the port are absent.

- [ ] **Step 3: Implement the smallest deep kernel interface**

Define one internal behavior-level `KubernetesAccess` port now, with `query`, `execute`, and `subscribe` methods over backend-owned request/result enums. Only bootstrap query and the opaque bootstrap-status subscription are implemented in this task; unsupported variants return typed capability errors. All later fake and kube work must extend this same port rather than adding side doors. `BackendKernel::{query,execute,subscribe}` is the sole protocol-facing interface, owns mapping to normalized protocol payloads, and enforces deadlines/cancellation. Validation and stream-ticket issuance are queries; `execute(Command)` always returns `OperationId`; dedicated stream redemption enters the kernel-owned Stream Hub through `subscribe`. Fake data never escapes as fixture types. Add `server_instance_id` on construction.

- [ ] **Step 4: Verify behavior through the kernel interface**

Run: `cargo test --locked -p k10s-backend`

Expected: PASS; tests do not reach into fake-adapter internal collections.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-backend
git commit -m "feat: add backend kernel and fake adapter"
```

### Task 4: Expose authenticated control WebSocket requests

**Files:**
- Create: `crates/k10s-server/src/config.rs`
- Create: `crates/k10s-server/src/auth.rs`
- Create: `crates/k10s-server/src/control.rs`
- Create: `crates/k10s-server/src/outbound.rs`
- Create: `crates/k10s-server/src/lifecycle.rs`
- Modify: `crates/k10s-server/src/lib.rs`
- Create: `crates/k10s-server/tests/control_socket.rs`

- [ ] **Step 1: Write failing loopback integration tests**

Cover: only `CONTROL_PATH` upgrades; logs/exec paths exist but return `NotImplemented` in this plan; unauthenticated first frame closes; wrong token closes; correct `Hello` returns negotiated `Welcome`; bootstrap preserves `RequestId`; cancellation/deadline works; oversized individual frames are rejected; an oversized message split across individually valid fragments is rejected before full payload assembly; bounded output overload closes explicitly.

```rust
#[tokio::test]
async fn authenticated_bootstrap_round_trips_request_id() {
    let server = TestServer::spawn("secret").await;
    let mut ws = connect(server.control_url()).await;
    ws.send(hello("secret")).await.unwrap();
    assert!(matches!(recv(&mut ws).await, ServerFrame::Welcome(_)));
    ws.send(bootstrap_request(7)).await.unwrap();
    assert_eq!(recv(&mut ws).await.request_id(), Some(RequestId::from_u128(7)));
}
```

- [ ] **Step 2: Verify the server tests fail**

Run: `cargo test --locked -p k10s-server --test control_socket`

Expected: FAIL because no router or lifecycle exists.

- [ ] **Step 3: Implement bounded authentication and dispatch**

Bind through `tokio::net::TcpListener`; configure hello timeout, separate maximum frame and assembled-message sizes on Axum's WebSocket upgrade, maximum authenticated/unauthenticated connections, and bounded per-connection queues. The assembled-message limit must apply across fragmentation before JSON payload allocation/dispatch. Split each accepted socket into read/write tasks joined by a child `CancellationToken`. Add the permanent bounded priority scheduler: P0 authentication/shutdown/terminal-operation/resync signals and P1 request responses/subscription lifecycle/connection/permission status are never silently discarded; P2 resource deltas may coalesce by resource identity while preserving detectable revision gaps. When P0/P1 pressure persists or the fixed reserve is exhausted, close with an explicit overload reason. Dispatch only through `BackendKernel`; bootstrap query and bootstrap-status subscription are the only supported behaviors in this plan. Emit tracing spans with session/request/correlation IDs and queue pressure, never token or payload bodies.

- [ ] **Step 4: Run integration tests and Clippy**

Run: `cargo test --locked -p k10s-server --test control_socket && cargo clippy --locked -p k10s-server --all-targets -- -D warnings`

Expected: PASS with no unbounded channel construction.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-server
git commit -m "feat: serve authenticated control websocket"
```

### Task 5: Implement the shared protocol-client state machine

**Files:**
- Create: `crates/k10s-ui/src/client/mod.rs`
- Create: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/src/client/transport.rs`
- Modify: `crates/k10s-ui/src/lib.rs`
- Create: `crates/k10s-ui/tests/client_state.rs`
- Create: `crates/k10s-ui/tests/client_transport.rs`

- [ ] **Step 1: Write failing client-state tests**

Test `Disconnected -> Authenticating -> Ready`, request correlation, unknown response IDs, cancellation/deadlines, bounded inbox overflow, full-jitter retry scheduling, and reconnect preserving local UI state while reissuing live subscriptions after `ResyncRequired`. Add a transport burst test that leaves the UI completely undrained, injects more events than the configured capacity, and proves the callback closes at the exact bound without any intermediate receiver queue. Explicitly test terminal states: authentication rejection returns to the web gate without retry, incompatible protocol major stops with an upgrade-required error, and user/application explicit close remains closed until a new connect command.

```rust
#[test]
fn bootstrap_response_completes_only_matching_request() {
    let mut client = ClientState::ready_for_test();
    let pending = client.begin(Query::Bootstrap).unwrap();
    client.apply(ServerFrame::response(pending.id(), BootstrapResponse::fixture())).unwrap();
    assert!(matches!(client.take(pending), Some(QueryResult::Bootstrap(_))));
}
```

- [ ] **Step 2: Verify client tests fail**

Run: `cargo test --locked -p k10s-ui --test client_state`

Expected: FAIL with missing client state.

- [ ] **Step 3: Implement pure state plus target transport adapters**

Keep request maps, inbox bounds, sequence/Ack tracking, reconnect/backoff, and resubscription logic in pure Rust. Retry only transient transport loss and retryable server errors. Authentication rejection, incompatible major version, and explicit close are terminal and cancel pending retry timers. Put `ewebsock` behind a private transport module selected by target, but use only `ws_connect` with an event callback that calls `try_send` directly on the bounded inbox and returns `ControlFlow::Break` on overflow; do not call `connect`, `connect_with_wakeup`, or construct `WsReceiver`, because those introduce an unbounded intermediate channel. The browser and native paths use this same bounded callback contract, send the same `Hello` JSON, and never attach credentials to the URL. Exercise the Plan 1 bootstrap-status subscription to prove the baseline recovery contract: after reconnect or `ResyncRequired`, preserve local UI state, invalidate server-issued state, re-bootstrap, and reissue subscriptions. Plan 2 extends the subscription selector without changing this state machine. Plan 5 may optimize recovery with bounded journal replay but cannot change the correctness contract.

- [ ] **Step 4: Verify native and WASM compilation**

Run: `cargo test --locked -p k10s-ui && cargo check --locked -p k10s-ui --target wasm32-unknown-unknown`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui
git commit -m "feat: add shared websocket client state"
```

### Task 6: Embed the server in the desktop application

**Files:**
- Create: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/lib.rs`
- Modify: `apps/k10s-desktop/src/main.rs`
- Create: `apps/k10s-desktop/tests/embedded_lifecycle.rs`

- [ ] **Step 1: Write a failing embedded-lifecycle test**

Assert random loopback bind, readiness delivery, bootstrap over the exact `CONTROL_PATH`, a 32-byte cryptographically random URL-safe launch token, different tokens across consecutive launches, and port closure after cancellation.

- [ ] **Step 2: Verify it fails**

Run: `cargo test --locked -p k10s-desktop --test embedded_lifecycle`

Expected: FAIL because the launcher does not start a server.

- [ ] **Step 3: Implement desktop startup and minimal UI**

Start the server on a dedicated thread, generate the launch token from the OS CSPRNG, bind only `127.0.0.1:0`, and return `EmbeddedServerHandle { url, token, shutdown }` through a one-shot channel. Construct `K10sApp` only after readiness. Render `Connecting`, the server instance ID, and fake context names; never call the kernel directly.

- [ ] **Step 4: Verify lifecycle and application build**

Run: `cargo test --locked -p k10s-desktop --test embedded_lifecycle && cargo build --locked -p k10s-desktop`

Expected: PASS; the listener is closed after handle shutdown.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui apps/k10s-desktop
git commit -m "feat: embed k10s server in desktop app"
```

### Task 7: Add the standalone server and web connection gate

**Files:**
- Modify: `apps/k10s-server/src/main.rs`
- Create: `crates/k10s-server/src/probes.rs`
- Create: `crates/k10s-server/tests/probes.rs`
- Modify: `apps/k10s-web/src/lib.rs`
- Create: `crates/k10s-ui/src/connection.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Create: `crates/k10s-ui/tests/connection_gate.rs`
- Create: `web/index.html`
- Create: `Trunk.toml`
- Create: `package.json`
- Create: `package-lock.json`
- Create: `playwright.config.ts`
- Create: `tests/browser/foundation.spec.ts`

- [ ] **Step 1: Write failing connection-gate and browser smoke tests**

Assert that a fresh web app shows the gate, wrong-token error returns to it, successful authentication clears the input buffer, and the token is absent from serializable persisted settings. Unit-test URL derivation: `http:` maps to `ws:`, `https:` maps to `wss:`, scheme/authority are preserved, and path is replaced—not joined—with the root-level `CONTROL_PATH` (`https://host/prefix/` becomes `wss://host/api/v1/control`). Add probe tests: `/healthz` is 200 while the process listener/event loop is alive; `/readyz` is 503 during backend initialization, 200 only after backend initialization and request acceptance, and 503 with a safe body after initialization failure or once draining begins. First create the Playwright spec and run `npm ci && npx playwright test tests/browser/foundation.spec.ts --project=chromium`; expect FAIL because no built/served web application exists. The smoke opens the server-hosted built app, authenticates, and sees fake contexts.

- [ ] **Step 2: Verify tests fail**

Run: `cargo test --locked -p k10s-ui --test connection_gate`

Expected: FAIL with missing gate state.

- [ ] **Step 3: Implement same-origin web startup**

Pin Trunk 0.21.14 in CI and configure `Trunk.toml` with `locked = true`; `trunk build --release` performs wasm-bindgen generation and fingerprints JS/WASM/assets into `dist/`. The server reads a token from an explicit secret source, refuses non-loopback startup without it, embeds or serves that exact `dist/` tree, exposes `/healthz` and `/readyz`, and mounts `CONTROL_PATH`. Model readiness as `Starting | Ready | InitializationFailed | Draining` with the status codes tested above; probe bodies contain no kubeconfig path or credentials. The web entry derives only the socket scheme and authority from `window.location`, then replaces the path with root-level `CONTROL_PATH`; it never forces WSS on HTTP development pages. The gate retains the token only in a non-serialized field and zeroes the input string after connection. Pin `@playwright/test` 1.62.0 in `package-lock.json`; configure Chromium foundation smoke with a `webServer` command that starts `k10s-server` against the built `dist/`.

- [ ] **Step 4: Verify server, WASM, and secret behavior**

Run:

```bash
cargo test --locked -p k10s-ui --test connection_gate
cargo test --locked -p k10s-server
trunk build --release
npm ci
npx playwright install chromium
npx playwright test tests/browser/foundation.spec.ts --project=chromium
cargo build --locked -p k10s-server
```

Expected: PASS; non-loopback/no-token configuration test returns an error before bind.

- [ ] **Step 5: Commit**

```bash
git add apps/k10s-server apps/k10s-web crates/k10s-ui web Trunk.toml package.json package-lock.json playwright.config.ts tests/browser/foundation.spec.ts
git commit -m "feat: add standalone web runtime"
```

### Task 8: Add shutdown, tracing, CI, and foundation documentation

**Files:**
- Modify: `crates/k10s-server/src/lifecycle.rs`
- Modify: `apps/k10s-desktop/src/main.rs`
- Modify: `apps/k10s-server/src/main.rs`
- Create: `.github/workflows/ci.yml`
- Modify: `README.md`
- Create: `crates/k10s-server/tests/shutdown.rs`

- [ ] **Step 1: Write failing graceful-shutdown and redaction tests**

Assert the exact order: set `/readyz` to 503/Draining; stop accepting application connections; send `ShutdownNotice`; reject new mutations while allowing status reads; cancel watches/logs; terminate Exec; drain tasks to deadline; close listener. `/healthz` stays 200 until the listener is closed. Also assert absence of access token in captured tracing output.

- [ ] **Step 2: Verify tests fail**

Run: `cargo test --locked -p k10s-server --test shutdown`

Expected: FAIL until shutdown ordering is implemented.

- [ ] **Step 3: Implement root cancellation and structured tracing**

Track connection tasks with `TaskTracker` and encode the approved order as an explicit lifecycle state machine: mark not-ready, stop accepting application connections, send notice and flip the mutation gate, cancel watches/logs, terminate Exec, then drain with a deadline. Emit safe correlation fields. Add CI jobs for fmt, Clippy, native tests, and WASM check. Document native/web development commands, both probes, and the current fake-only scope.

- [ ] **Step 4: Run the foundation gate**

Run:

```bash
cargo fmt --all -- --check
trunk build --release
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo test --locked --workspace
cargo check --locked -p k10s-web --target wasm32-unknown-unknown
npm ci
npx playwright test tests/browser/foundation.spec.ts --project=chromium
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add .github README.md crates/k10s-server apps
git commit -m "test: verify runtime foundation"
```

## Plan 1 verification gate

- Native UI retrieves fake contexts only through the loopback WebSocket.
- Web UI requires token entry, then retrieves the same bootstrap response.
- Protocol crate builds without platform dependencies.
- Backend tests cross `BackendKernel`; no UI test reads fake collections.
- Authentication on the fixed route, terminal-versus-retryable disconnects, request correlation, compatibility, cancellation/deadlines, end-to-end bounded client transport, fragmented-message limits, bounded scheduling, reconnect/full-resync, probe transitions, and graceful shutdown are tested.
- `Cargo.lock`, Trunk output, and a real Chromium bootstrap smoke are part of the foundation gate.
- All workspace tests, Clippy, and WASM checks pass.
