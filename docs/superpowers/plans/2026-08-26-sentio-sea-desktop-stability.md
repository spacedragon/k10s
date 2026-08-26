# Desktop Large-Cluster Connection Stability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the desktop client reach a usable, stable state on `sentio-sea` by eliminating duplicate/legacy discovery work, subscribing only to visible namespace-scoped resources, safely transferring large snapshots, and ending unsupported Overview loads locally.

**Architecture:** Add one shared discovery generation per Kubernetes context and prefer kube-rs aggregated discovery with a narrowly classified legacy fallback. Derive canonical resource subscriptions from open workspace windows using an explicit namespace-scope model. Keep transport memory bounded while serializing complete initial snapshots per control session, and retain request-scoped infrastructure failures as a UI-readable panel state rather than escalating them to the connection.

**Tech Stack:** Rust 2024, kube-rs 4.2, Tokio, futures-util, Tower test services, Axum/WebSockets, Serde/serde_json, egui/eframe, cargo test/Clippy.

---

**Design:** [Desktop Large-Cluster Connection Stability Design](../specs/2026-08-26-sentio-sea-desktop-stability-design.md)

## Implementation rules

- Work in task order. Tasks 1-2 establish the backend catalog contract; Tasks 3-4 depend on the namespace model; Task 5 depends on the final subscription shape.
- For every behavior change: add the focused test, run it and observe the intended failure, implement the smallest passing change, then rerun the focused and affected suites.
- Use `cargo test --locked` and `cargo clippy --locked` throughout.
- Do not widen a namespace implicitly. `ContextDefault` always resolves to the selected context's namespace or `default`; only `AllNamespaces` resolves to protocol `None`.
- Do not retry aggregated discovery through legacy discovery for authentication, authorization, throttling, server, transport, TLS, proxy, service, timeout, or cancellation failures.
- Do not increase the 1 MiB frame or 4 MiB message limits and do not add raw Kubernetes objects, API response bodies, kubeconfig data, or credentials to logs.
- Preserve unrelated worktree changes and make one small commit after each green task.

## File map

- `crates/k10s-backend/src/testkit.rs`: Accept-aware recorded discovery responses and request-header observations.
- `crates/k10s-backend/src/kube/discovery.rs`: aggregated-first discovery, usable-core validation, fallback classification, normalization, and safe tracing.
- `crates/k10s-backend/src/kube/mod.rs`: per-context shared discovery generations and cache publication.
- `crates/k10s-backend/tests/discovery.rs`: aggregated/fallback and concurrent-generation contracts.
- `crates/k10s-ui/src/workspace/{mod,resource,service,snapshot}.rs`: explicit namespace scope and v1-to-v2 persistence migration.
- `crates/k10s-ui/tests/{workspace_state,workspace_snapshot}.rs`: scope behavior, context-switch resolution, and migration.
- `apps/k10s-desktop/src/lib.rs`: migration provenance and debounced rewrite of v1 state files.
- `crates/k10s-ui/src/app.rs`: desired-subscription reconciliation, ref-counted canonical identities, inbox bound, and local infrastructure failure state.
- `crates/k10s-ui/src/ui/{mod,window,overview,infrastructure,resource_window,service_window}.rs`: scope controls and infrastructure-unavailable rendering.
- `crates/k10s-ui/tests/{ui_resource_windows,ui_services,ui_infrastructure}.rs`: explicit scope and unavailable-state rendering.
- `crates/k10s-ui/tests/client_state.rs`: request failure retention if introduced at the shared client boundary.
- `crates/k10s-server/src/{config,control}.rs`: production page default and per-session initial-snapshot serialization.
- `crates/k10s-server/tests/{budget_config,subscription_loopback,fake_capacity}.rs`: defaults, contiguous snapshot lifecycle, cancellation, and 4,300-row capacity.
- `apps/k10s-desktop/tests/large_cluster_connection.rs`: real `K10sApp`/`BoundedInbox` large-snapshot loopback.
- `crates/k10s-server/tests/live_context.rs`: ignored opt-in production adapter plus authenticated control-WebSocket smoke.

### Task 1: Prefer aggregated discovery with a classified legacy fallback

**Files:**

- Modify: `crates/k10s-backend/src/testkit.rs`
- Modify: `crates/k10s-backend/src/kube/discovery.rs`
- Modify: `crates/k10s-backend/tests/discovery.rs`

- [ ] **Step 1: Add failing Accept-aware testkit coverage.** Record the `Accept` header for each path in `RecordedState`, expose `request_accepts(path)`, and allow canned responses keyed by `METHOD path` plus an exact Accept substring. Add a unit/integration assertion that the recorded service distinguishes aggregated `/apis` traffic from a later legacy `/apis` request.
- [ ] **Step 2: Add aggregated fixture documents.** Add `APIGroupDiscoveryList` v2 bodies for `/apis` and `/api` that describe the existing core, apps, batch, storage, apiextensions, and `k10s.example.com` resources, including list/watch verbs and `/scale` subresources. Keep the current legacy fixtures available for fallback tests.
- [ ] **Step 3: Add failing test `supported_cluster_uses_two_aggregated_requests`.** Query `ResourceTypes`, assert Pod/Deployment/Gadget normalization remains unchanged, assert exactly one `/apis` and one `/api` hit, assert both requests advertise `apidiscovery.k8s.io`, and assert no `/api/v1` or group-version endpoint was requested.
- [ ] **Step 4: Add failing fallback table tests.** Cover HTTP 404, 406, and 415; a malformed aggregated shape producing `SerdeError`; an aggregated v2 document that produces kube-rs `DiscoveryError`; and successful legacy `/api` + `/apis` bodies that deserialize as default-empty v2. Each case must run legacy discovery exactly once and return the normalized catalog.
- [ ] **Step 5: Add failing no-fallback table tests.** Cover HTTP 401, 403, 429, and 500 plus `RecordedApiServer::set_transport_error`. Assert the returned `BackendError` stays sanitized and legacy group-version endpoints receive zero hits. Preserve the existing raw-status redaction assertion.
- [ ] **Step 6: Run the red tests:**

```bash
cargo test --locked -p k10s-backend --test discovery supported_cluster_uses_two_aggregated_requests -- --nocapture
cargo test --locked -p k10s-backend --test discovery aggregated_fallback -- --nocapture
cargo test --locked -p k10s-backend --test discovery aggregated_failure_does_not_fallback -- --nocapture
```

Expected: request-count/header assertions fail because `discover_resource_types` still calls legacy `Discovery::run()`.

- [ ] **Step 7: Implement aggregated-first discovery.** Split discovery execution from catalog normalization. Call `Discovery::new(client.clone()).run_aggregated()` first, validate that the resulting discovery contains a non-empty core group/version with at least one usable list resource, and otherwise classify the original `kube::Error` before deciding whether to call `run()`.
- [ ] **Step 8: Keep fallback classification exhaustive and private.** A helper such as `should_fallback_from_aggregated(&kube::Error) -> bool` returns true only for HTTP 404/406/415, `SerdeError`, and `Discovery`. The empty-core compatibility signal is a separate successful-result branch. All other variants return the existing sanitized error immediately.
- [ ] **Step 9: Add safe discovery tracing.** Record context, mode (`aggregated`/`legacy`), elapsed duration, and normalized outcome only. Do not format the raw kube error or response.
- [ ] **Step 10: Run `cargo test --locked -p k10s-backend --test discovery` and `cargo clippy --locked -p k10s-backend --all-targets -- -D warnings`; expect PASS.**
- [ ] **Step 11: Commit:**

```bash
git add crates/k10s-backend/src/testkit.rs crates/k10s-backend/src/kube/discovery.rs crates/k10s-backend/tests/discovery.rs
git commit -m "perf: prefer aggregated kubernetes discovery"
```

### Task 2: Coalesce catalog discovery by context and generation

**Files:**

- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Modify: `crates/k10s-backend/tests/discovery.rs`

- [ ] **Step 1: Add failing `concurrent_cold_queries_share_one_discovery_generation`.** Use a barrier to start eight `Query::ResourceTypes` futures against one recorded context, await them together, assert equal results, and assert `/apis == 1` and `/api == 1`.
- [ ] **Step 2: Add failing `different_contexts_discover_independently`.** Stall context A's `/apis`, prove context B completes before A is released, then assert one discovery generation per context.
- [ ] **Step 3: Add failing `all_waiters_share_one_failed_generation`.** Arrange a deterministic aggregated 500 response, release eight barriered callers together, and assert every caller receives the same cloned `BackendError` while `/apis == 1`. After all first-generation futures complete, change the response to success, make one later call, and assert `/apis == 2` and success. No first-generation waiter may start generation two.
- [ ] **Step 4: Add failing `forced_refresh_joins_running_generation_and_bypasses_fresh_cache`.** Start a cold `ResourceTypes` call, concurrently exercise the context-switch validation path, and assert one running generation. Then populate a fresh cache and switch again; assert the forced path starts exactly one new generation and replaces the cached catalog.
- [ ] **Step 5: Run the red tests:**

```bash
cargo test --locked -p k10s-backend --test discovery concurrent_cold_queries -- --nocapture
cargo test --locked -p k10s-backend --test discovery all_waiters_share_one_failed_generation -- --nocapture
cargo test --locked -p k10s-backend --test discovery forced_refresh_joins_running_generation -- --nocapture
```

Expected: same-context hit counts exceed one.

- [ ] **Step 6: Add cloneable flight state to `KubeAdapter`.** Introduce a per-context registry behind a short-held standard mutex. Each entry contains a monotonically increasing generation and a `futures_util::future::Shared` boxed future whose output is `Result<ResourceTypesData, BackendError>`. Initialize it in both `from_kubeconfig` and `with_cluster_clients`; include no client/credential details in `Debug`.
- [ ] **Step 7: Implement one internal resolver with cache policy.** Use an enum such as `CatalogPolicy::{UseFreshCache, ForceLive}`. Validate context first, take the normal fresh-cache fast path only for `UseFreshCache`, then join/install a flight, await it outside all locks, publish successful data once, and remove the entry only when its generation still matches.
- [ ] **Step 8: Preserve failed-generation semantics.** Clone the same `Result` out of the shared future for all joiners. Remove a failed generation only after it resolves; a caller that already cloned the future remains bound to that failure, while a caller arriving after removal installs the next generation.
- [ ] **Step 9: Route both call sites through the resolver.** `catalog_for` uses `UseFreshCache`; switch validation uses `ForceLive`. Remove the old direct `discover_catalog` race. Keep context cache invalidation and retirement behavior unchanged.
- [ ] **Step 10: Add safe tracing for generation number, context, whether the caller joined, duration, and success/failure category.**
- [ ] **Step 11: Run `cargo test --locked -p k10s-backend --test discovery`, `cargo test --locked -p k10s-backend --test context_switch`, and the full backend suite; expect PASS.**
- [ ] **Step 12: Commit:**

```bash
git add crates/k10s-backend/src/kube/mod.rs crates/k10s-backend/tests/discovery.rs
git commit -m "fix: coalesce kubernetes catalog discovery"
```

### Task 3: Make namespace scope explicit and migrate workspace snapshots

**Files:**

- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Modify: `crates/k10s-ui/src/workspace/resource.rs`
- Modify: `crates/k10s-ui/src/workspace/service.rs`
- Modify: `crates/k10s-ui/src/workspace/snapshot.rs`
- Modify: `crates/k10s-ui/tests/workspace_state.rs`
- Modify: `crates/k10s-ui/tests/workspace_snapshot.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Modify: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Modify: `crates/k10s-ui/tests/ui_services.rs`
- Modify: `apps/k10s-desktop/src/lib.rs`

- [ ] **Step 1: Add failing scope model tests.** Assert new resource and Service windows start with `NamespaceScope::ContextDefault`; `SetNamespaceScope` can choose `Namespace("team-a")` and `AllNamespaces`; duplicate same-kind windows keep independent scopes.
- [ ] **Step 2: Add failing context-resolution tests.** Add/target a pure helper `NamespaceScope::resolve(context_namespace)` and assert `ContextDefault` maps `Some("sea") -> Some("sea")` and `None -> Some("default")`; explicit namespace is preserved; `AllNamespaces -> None`. After `CommitContextSwitch`, only `ContextDefault` resolves against the destination namespace.
- [ ] **Step 3: Add failing navigation-guard tests.** Changing scope on a window with dirty YAML or a connected shell must yield `WorkspaceEvent::Blocked` and leave scope, selection, and detail unchanged. `Cancel` keeps the old scope; resolving every blocker commits the pending scope change once, clears the now-stale selection/detail, and disconnects/releases the guarded state. Changing to the already-selected scope is a no-op.
- [ ] **Step 4: Add failing v2 round-trip tests.** Set `SNAPSHOT_VERSION` expectation to 2 and lock the exact representation: `{"kind":"context_default"}`, `{"kind":"namespace","value":"prod"}`, and `{"kind":"all_namespaces"}` using `#[serde(tag = "kind", content = "value", rename_all = "snake_case")]`. Prove all three variants restore unchanged.
- [ ] **Step 5: Add failing v1 migration tests from literal JSON.** A version-1 view with `"namespace":"prod"` must become `Namespace("prod")`; `"namespace":null` must become `ContextDefault`; geometry/search/filter/sort/split/custom kind survive; unknown versions remain rejected. Explicitly assert legacy `null` never becomes `AllNamespaces`.
- [ ] **Step 6: Add a failing desktop state-store migration test.** Write literal v1 JSON to a temp state path, launch/restore or call the same load path, and assert the decoded snapshot reports `migrated_from == Some(1)`. After the normal debounce/tick, read the file back and assert `version == 2`, the explicit scope wire shape is present, and all other settings survived. Also assert an already-v2 steady-state file is not rewritten.
- [ ] **Step 7: Run the red tests:**

```bash
cargo test --locked -p k10s-ui --test workspace_state namespace_scope -- --nocapture
cargo test --locked -p k10s-ui --test workspace_snapshot namespace -- --nocapture
cargo test --locked -p k10s-desktop state_store_rewrites_v1_namespace_scope -- --nocapture
```

Expected: `NamespaceScope`/`SetNamespaceScope` are unresolved and version-1 input is rejected.

- [ ] **Step 8: Implement the model and guarded transition.** Export a serializable, ordered `NamespaceScope { ContextDefault, Namespace(String), AllNamespaces }`; replace `Option<String>` in both window state structs and replace `WorkspaceCommand::SetNamespace`. Route `SetNamespaceScope` through a dedicated `set_namespace_scope` navigation method: collect the target window's blockers, park the command when guarded, and only on guard-clear commit the new scope plus clear selection/detail. Keep default construction at `ContextDefault`.
- [ ] **Step 9: Implement versioned deserialization without weakening unknown-version rejection.** Deserialize a raw versioned snapshot envelope into a `LoadedWorkspaceSnapshot { snapshot, migrated_from: Option<u32> }` (or equivalent provenance-bearing result), normalize only v1 into v2, and have `snapshot()` always write version 2. Unknown versions return no load result. Do not reinterpret `None` elsewhere as all namespaces.
- [ ] **Step 10: Preserve migration provenance through `StateStore`.** `StateStore::load` must return both the normalized snapshot and migration flag. On launch, restore the normalized snapshot but do not call `mark_loaded` as if that v2 value were already on disk when `migrated_from.is_some()`; leave `last_saved` different/empty so the existing debounced tick rewrites v2. Keep the no-write fast path for files originally loaded as v2.
- [ ] **Step 11: Update namespace controls.** Render `Context default (<resolved>)`, explicit namespace choices/text, and `All namespaces` distinctly for namespaced resources and Services; cluster-scoped custom resources hide/ignore the scope control. Queue only the guarded `SetNamespaceScope` command.
- [ ] **Step 12: Run `cargo test --locked -p k10s-ui --test workspace_state --test workspace_snapshot --test ui_resource_windows --test ui_services` and `cargo test --locked -p k10s-desktop`; expect PASS.**
- [ ] **Step 13: Commit:**

```bash
git add crates/k10s-ui/src/workspace crates/k10s-ui/src/ui/resource_window.rs crates/k10s-ui/src/ui/service_window.rs crates/k10s-ui/tests/workspace_state.rs crates/k10s-ui/tests/workspace_snapshot.rs crates/k10s-ui/tests/ui_resource_windows.rs crates/k10s-ui/tests/ui_services.rs apps/k10s-desktop/src/lib.rs
git commit -m "feat: persist explicit namespace scopes"
```

### Task 4: Reconcile subscriptions from open workspace windows

**Files:**

- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs` if feed lookup needs a window/key seam
- Modify: `crates/k10s-ui/src/ui/service_window.rs` if feed lookup needs a window/key seam
- Modify: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Modify: `crates/k10s-ui/tests/ui_services.rs`

- [ ] **Step 1: Replace eager-bootstrap expectations with failing demand tests in `app.rs`.** An Overview-only bootstrap must emit `bootstrap` and `infrastructure.get`, but no resource subscribe and no `resource.types` request. Opening Pods must add exactly one Pod subscription using the context namespace from Bootstrap; opening Services adds exactly one Service subscription; opening Custom Resources with no selected GVK requests `resource.types` but starts no resource watch.
- [ ] **Step 1a: Add a focused launcher projection assertion.** Unopened workload/Service launcher entries remain unknown/not-loaded (rather than displaying fabricated zero counts) and this projection must not create hidden subscriptions.
- [ ] **Step 2: Add failing canonical identity/ref-count tests.** Two Pod windows with the same scope share one subscription; closing one sends no unsubscribe; closing the last sends one unsubscribe. Two Pod windows with `Namespace("a")` and `Namespace("b")` create two subscriptions. Changing one scope replaces only its reference.
- [ ] **Step 3: Add failing custom-resource identity tests.** Two Custom Resource windows selecting different canonical `group/version/kind` values subscribe independently; duplicate GVK+scope windows share. A cluster-scoped descriptor emits protocol namespace `None` without treating it as the user's `AllNamespaces` choice.
- [ ] **Step 4: Add failing context-switch/reconnect tests.** Reconciliation rebuilds the desired set on the new context; `ContextDefault` resolves the new context namespace; explicit namespace and all-namespaces scopes are preserved. Closed windows never return during reconnect.
- [ ] **Step 5: Run the red tests:**

```bash
cargo test --locked -p k10s-ui --lib workspace_driven -- --nocapture
cargo test --locked -p k10s-ui --lib canonical_subscription -- --nocapture
cargo test --locked -p k10s-ui --lib reconnect_rebuilds_desired -- --nocapture
```

Expected: bootstrap emits all built-in subscriptions and `resource.types`, and same-kind state cannot represent multiple scope-specific streams.

- [ ] **Step 6: Introduce canonical keys and retained entries.** Use ordered value types similar to:

```rust
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct ResourceSubscriptionKey {
    context: String,
    gvk: GroupVersionKind,
    namespace_scope: NamespaceScope,
}

#[derive(Debug)]
struct RetainedResourceSubscription {
    live: LiveSubscription,
    windows: BTreeSet<WindowId>,
}
```

Keep window-to-key projection deterministic so feed lookup does not collapse two same-kind windows with different scopes. If `ResourceFeed` currently keys lists only by `WorkloadKind`, migrate it to window ID or canonical key and update the focused rendering tests.
- [ ] **Step 7: Replace `ensure_resource_streams` with desired-set reconciliation.** Walk only open workspace list windows, resolve builtin/custom GVK, resolve namespace scope against the selected Bootstrap context, diff desired versus retained keys, subscribe new keys, update reference sets, and unsubscribe removed keys. Run reconciliation after workspace commands, bootstrap, context switch, and reconnect recovery.
- [ ] **Step 8: Make type discovery demand-driven.** Request/retain `resource.types` only while at least one Custom Resources window needs the picker or a selected descriptor. Cancel/clear stale-context requests. Do not use the catalog to preload closed launcher entries.
- [ ] **Step 9: Keep subscription limits and recovery semantics intact.** The shared client remains the desired-subscription owner across transport loss; reconciliation changes desired identities only when workspace/context changes.
- [ ] **Step 10: Run `cargo test --locked -p k10s-ui --lib`, the four affected UI integration suites, and `cargo check --locked -p k10s-web --target wasm32-unknown-unknown`; expect PASS.**
- [ ] **Step 11: Commit:**

```bash
git add crates/k10s-ui/src/app.rs crates/k10s-ui/src/ui/resource_window.rs crates/k10s-ui/src/ui/service_window.rs crates/k10s-ui/tests/ui_resource_windows.rs crates/k10s-ui/tests/ui_services.rs
git commit -m "fix: subscribe only visible workspace resources"
```

### Task 5: Serialize initial snapshots and raise bounded production defaults

**Files:**

- Modify: `crates/k10s-server/src/config.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-server/tests/budget_config.rs`
- Modify: `crates/k10s-server/tests/subscription_loopback.rs`
- Modify: `crates/k10s-server/tests/fake_capacity.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/client/transport.rs` only if a named production default belongs there
- Create: `apps/k10s-desktop/tests/large_cluster_connection.rs`

- [ ] **Step 1: Add failing production-default assertions.** Assert `ServerConfig::default().snapshot_rows_per_chunk == 128` and the real app connection uses a named inbox capacity of 256. Keep both values explicit and documented.
- [ ] **Step 2: Add failing `concurrent_initial_snapshots_are_serialized_per_session`.** Subscribe two resources whose snapshots are delayed/interleaved at the backend seam. On the wire assert each `snapshotBegin` is followed only by chunks/end for the same subscription before the other begin appears. Sequence numbers remain contiguous.
- [ ] **Step 3: Add failing cancellation test.** Cancel the subscription holding the snapshot permit after begin/chunks, assert no `snapshotEnd` for that incomplete snapshot, then assert another subscription acquires the permit and completes. The session and scheduler remain healthy.
- [ ] **Step 4: Add a non-ignored real-desktop-inbox 4,300-row loopback test.** In `apps/k10s-desktop/tests/large_cluster_connection.rs`, start `k10s_server::spawn_loopback` with `FakeKubernetes::with_capacity(...)` large enough to expose at least 4,300 Pods and otherwise untouched `ServerConfig::default()`. Connect through public `K10sApp::connect` so `RealConnectionFactory`, ewebsock, and the production `BoundedInbox` are used. Poll until Ready, open Pods, select explicit `AllNamespaces` through the same public workspace/app seam, continue polling while the UI drains/ACKs events, and assert: the socket never transitions to Connecting/Failed, the inbox never causes reconnect, every expected Pod row is visible, and completion meets a generous deterministic timeout. Add a separate server-loopback assertion for the exact `rows.div_ceil(128)` chunk count; do not substitute a raw tungstenite-to-`ClientState` pump for the desktop inbox test.
- [ ] **Step 5: Run the red tests:**

```bash
cargo test --locked -p k10s-server --test budget_config snapshot_rows -- --nocapture
cargo test --locked -p k10s-server --test subscription_loopback concurrent_initial_snapshots -- --nocapture
cargo test --locked -p k10s-server --test subscription_loopback cancelled_snapshot_releases -- --nocapture
cargo test --locked -p k10s-server --test fake_capacity default_4300 -- --nocapture
cargo test --locked -p k10s-desktop --test large_cluster_connection -- --nocapture
```

Expected: defaults are 16/64 and concurrent forwarders can interleave snapshot lifecycles.

- [ ] **Step 6: Create one semaphore per authenticated control session.** Allocate `Arc<tokio::sync::Semaphore>::new(1)` beside the session's watch-forwarder state. Clone it into every resource forwarder; do not use a process-global semaphore.
- [ ] **Step 7: Hold an owned permit for the complete snapshot lifecycle.** Acquire cancellation-aware before `snapshotBegin`, hold through all chunks and `snapshotEnd`, and drop it on every return. Thread both session and generation cancellation into acquisition and sending so a cancelled partial snapshot emits no end. Infrastructure updates/deltas outside initial resource snapshots must not hold this permit.
- [ ] **Step 8: Add safe snapshot tracing.** Record subscription ID, row count, chunk count, elapsed duration, and completed/cancelled outcome, never row content.
- [ ] **Step 9: Change defaults to 128 rows/page and 256 inbox events.** Keep outbound scheduler capacity 64 unless the test proves a separate bounded change is necessary; do not alter frame/message byte limits.
- [ ] **Step 10: Run `cargo test --locked -p k10s-server --test budget_config --test subscription_loopback --test fake_capacity`, `cargo test --locked -p k10s-desktop --test large_cluster_connection`, `cargo test --locked -p k10s-ui`, and server/UI/desktop Clippy; expect PASS.**
- [ ] **Step 11: Commit:**

```bash
git add crates/k10s-server/src/config.rs crates/k10s-server/src/control.rs crates/k10s-server/tests/budget_config.rs crates/k10s-server/tests/subscription_loopback.rs crates/k10s-server/tests/fake_capacity.rs crates/k10s-ui/src/app.rs crates/k10s-ui/src/client/transport.rs apps/k10s-desktop/tests/large_cluster_connection.rs
git commit -m "fix: stabilize large initial snapshots"
```

### Task 6: Project unsupported infrastructure as a panel-local state

**Files:**

- Modify: `crates/k10s-ui/src/client/state.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/window.rs`
- Modify: `crates/k10s-ui/src/ui/overview.rs`
- Modify: `crates/k10s-ui/src/ui/infrastructure.rs`
- Modify: `crates/k10s-ui/tests/client_state.rs`
- Modify: `crates/k10s-ui/tests/ui_infrastructure.rs`

- [ ] **Step 1: Add failing client/app test for an unsupported request error.** After Ready/bootstrap, deliver a request-scoped `ErrorFrame` with `ErrorCode::UnsupportedMessage` for `infrastructure.get`. Assert the request finishes, the control client stays `Ready`, `AppView` stays `Ready`, and the app retains a safe infrastructure-unavailable state rather than restarting or keeping a pending request.
- [ ] **Step 2: Add failing subscription-scoped variant.** Reject the infrastructure subscription as unsupported; assert it is removed or marked unavailable without affecting resource subscriptions or the control connection. Refresh may retry the panel request explicitly but must not create a per-frame retry loop.
- [ ] **Step 3: Add failing egui test.** Render the unavailable state and assert `Cluster overview is not available in this build`, a `Refresh overview` button, no progress indicator, and no `Loading cluster overview`. If Nodes/Storage windows are open, show the same safe capability-unavailable family instead of indefinite inventory spinners.
- [ ] **Step 4: Run the red tests:**

```bash
cargo test --locked -p k10s-ui --lib unsupported_infrastructure -- --nocapture
cargo test --locked -p k10s-ui --test ui_infrastructure unavailable -- --nocapture
```

Expected: `ClientState::apply_at` returns the scoped server error and drops correlation without a retained panel outcome; rendering keeps the spinner.

- [ ] **Step 5: Retain typed request failures at the narrowest useful boundary.** Add a bounded completed-error map/API keyed by `RequestId` (or an equivalent app-local extraction path) so `finish_infrastructure_request` can distinguish Unsupported from transport/session loss. Remove errors when taken/cancelled/rebuilt just like successful completions; do not stringify arbitrary server details into UI state.
- [ ] **Step 6: Add an explicit UI model.** Use a state such as `InfrastructureLoad::{Loading, Available(response), Unavailable { message }}` or a separate panel status passed alongside the cached response. Preserve a previous successful response for stale transport display, but an initial Unsupported response must end loading.
- [ ] **Step 7: Keep failure scope local.** Only Unsupported request/subscription errors map to capability unavailable. Transport loss follows existing reconnect behavior; request-scoped 401/conflict/internal failures use the existing safe retry/error policy and never turn into a fake response.
- [ ] **Step 8: Wire Refresh.** The panel button calls the existing explicit `refresh_infrastructure`; unavailable state becomes Loading for that attempt. Prevent automatic retry on every render frame.
- [ ] **Step 9: Run `cargo test --locked -p k10s-ui --lib --test client_state --test ui_infrastructure --test ui_resilience`; expect PASS.**
- [ ] **Step 10: Commit:**

```bash
git add crates/k10s-ui/src/client/state.rs crates/k10s-ui/src/app.rs crates/k10s-ui/src/ui/mod.rs crates/k10s-ui/src/ui/window.rs crates/k10s-ui/src/ui/overview.rs crates/k10s-ui/src/ui/infrastructure.rs crates/k10s-ui/tests/client_state.rs crates/k10s-ui/tests/ui_infrastructure.rs
git commit -m "fix: end unsupported overview loading locally"
```

### Task 7: Add opt-in live smoke and run release gates

**Files:**

- Create: `crates/k10s-server/tests/live_context.rs`
- Modify: `docs/superpowers/specs/2026-08-26-sentio-sea-desktop-stability-design.md` only if implementation discoveries require an approved correction

- [ ] **Step 1: Add an ignored, credential-free authenticated control-path live test.** In the server integration target, read only `K10S_LIVE_CONTEXT`; skip/fail with a clear instruction when absent. Build `KubeAdapter::from_kubeconfig(None)`, wrap it in `BackendKernel`, and start `spawn_loopback(ServerConfig::default() with a generated test access token)`. Connect a real WebSocket, authenticate with Hello, request Bootstrap, switch to `K10S_LIVE_CONTEXT` when needed, query `resource.types`, resolve the selected Bootstrap context namespace or `default`, subscribe to namespace-scoped Pods, ACK/reassemble the complete snapshot, and keep pumping/acking the authenticated control connection for 60 seconds. Fail on close, reconnect-required, sequence gap, resync storm, or session error. Never print the access token, kubeconfig, raw object manifests, or environment contents.
- [ ] **Step 2: Run the deterministic full gates from a clean test process:**

```bash
cargo fmt --all -- --check
cargo test --locked --workspace
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo check --locked -p k10s-web --target wasm32-unknown-unknown
```

Expected: all PASS. If the WASM target is unavailable locally, report that exact environmental limitation and run the native workspace gates; do not claim the WASM gate passed.

- [ ] **Step 3: Run desktop-focused integration tests:**

```bash
cargo test --locked -p k10s-desktop --test kube_factory --test embedded_lifecycle
```

Expected: PASS.

- [ ] **Step 4: Run the live smoke against the reported context:**

```bash
K10S_LIVE_CONTEXT=sentio-sea cargo test --locked -p k10s-server --test live_context -- --ignored --nocapture
```

Expected: aggregated catalog succeeds, the namespace-scoped Pod snapshot completes, and the 60-second stability window ends without reconnect/error. This is read-only cluster traffic.

- [ ] **Step 5: Launch the desktop development binary against the normal kubeconfig and manually verify:** Overview shows unavailable rather than loading; opening Pods uses the resolved default namespace; opening All namespaces completes without connection failure; closing the window unsubscribes. Do not kill or alter any pre-existing user-owned packaged app process.
- [ ] **Step 6: Inspect `git diff --check`, `git status --short`, and the task commits.** Confirm no temporary probes, credentials, generated kubeconfig, or unrelated files remain.
- [ ] **Step 7: Commit the smoke test:**

```bash
git add crates/k10s-server/tests/live_context.rs
git commit -m "test: add opt-in live context stability smoke"
```

## Completion evidence

Before declaring the issue fixed, record fresh evidence for all of the following:

- Eight cold same-context catalog calls yield one aggregated `/apis` and one `/api` request.
- Unsupported aggregated discovery falls back once; auth/transport failures never double traffic.
- Overview-only bootstrap opens no Kubernetes resource watches and makes no resource-types request.
- Pod default scope for `sentio-sea` is the kubeconfig context namespace (or `default`), not all namespaces.
- Explicit All namespaces applies a complete 4,300-row snapshot with the socket still connected.
- Unsupported infrastructure ends the spinner while the top-level app remains Ready.
- Formatting, workspace tests, workspace Clippy, desktop integration tests, and the opt-in `sentio-sea` smoke pass freshly.
