# Cluster API Traffic Monitor Implementation Plan

> **For Codex:** Implement this plan task-by-task in the current workspace.

**Goal:** Measure traffic between each k10s server-side Kubernetes client and its API server, stream live samples to connected clients, and show the selected context's traffic beside the context picker.

**Architecture:** A Tower layer around each real `kube::Client` counts request bytes, streamed response bytes, requests, and in-flight requests in per-context atomic counters. A backend traffic subscription samples those counters once per second; the control protocol forwards typed samples over the existing WebSocket subscription path, and the shared egui UI keeps a bounded history for a compact context-bar display.

**Tech Stack:** Rust, kube-rs, Tower/http-body, Tokio broadcast subscriptions, serde protocol types, Axum WebSocket, egui/eframe.

---

### Task 1: Define the traffic protocol contract

**Files:**
- Create: `crates/k10s-protocol/src/traffic.rs`
- Modify: `crates/k10s-protocol/src/lib.rs`
- Modify: `crates/k10s-protocol/src/subscription.rs`

1. Add serialization tests for a context-scoped traffic selector and sample.
2. Add `TrafficWatchSpec`, `TrafficSample`, and `TRAFFIC_EVENT_UPDATED`.
3. Add `SubscriptionSelector::Traffic` and export the new contract.
4. Run `cargo test -p k10s-protocol` and expect all tests to pass.

### Task 2: Count Kubernetes transport traffic

**Files:**
- Create: `crates/k10s-backend/src/kube/traffic.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Modify: `crates/k10s-backend/Cargo.toml`

1. Test request-size accounting, streamed response accounting, request completion, and rate snapshots.
2. Implement per-context atomic counters and an HTTP body wrapper that counts every data frame as it is consumed.
3. Install the layer at the shared kube client construction seam without recording headers, URLs, or payload content.
4. Run the focused backend traffic tests and expect all tests to pass.

### Task 3: Stream traffic samples through the backend and server

**Files:**
- Modify: `crates/k10s-backend/src/port.rs`
- Modify: `crates/k10s-backend/src/kernel.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Modify: `crates/k10s-backend/src/fake.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Test: `crates/k10s-server/tests/subscription_loopback.rs`

1. Add a failing loopback test that subscribes to one context and receives a typed traffic update.
2. Extend backend subscription/event variants and provide deterministic zero samples in fake mode.
3. Add the real one-second sampler and server event mapping using the existing bounded outbound scheduler.
4. Run backend and server subscription tests and expect all tests to pass.

### Task 4: Retain traffic state in the shared client

**Files:**
- Modify: `crates/k10s-ui/src/client/state.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Test: `crates/k10s-ui/tests/client_state.rs`

1. Test traffic event decoding, context isolation, and bounded history retention.
2. Subscribe after bootstrap and resubscribe when the selected context changes.
3. Store the latest sample plus sixty recent points and clear stale context presentation on switch.
4. Run client-state tests and expect all tests to pass.

### Task 5: Render the context traffic monitor

**Files:**
- Modify: `crates/k10s-ui/src/ui/top_bar.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`

1. Add UI tests for live, idle, unavailable, and compact labels.
2. Render download/upload rates and a bounded two-line sparkline to the right of the context picker.
3. Add an accessible tooltip with totals, request count, active requests, and measurement scope.
4. Verify compact widths degrade to text-only without overlap.

### Task 6: Verify the complete change

1. Run `cargo fmt --all -- --check`.
2. Run focused protocol/backend/server/UI tests.
3. Run `cargo check --locked --workspace`.
4. Run `cargo check --locked -p k10s-web --target wasm32-unknown-unknown`.
5. If the browser harness is available, build the web app and capture a UI screenshot for visual inspection.
