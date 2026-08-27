# Resource Details and Namespace Combobox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Pod and Deployment details return without waiting for owner traversal, lazily load related rows, and replace free-form namespace filtering with a searchable combobox backed by the complete Namespace watch.

**Architecture:** Keep primary detail, events, and related traversal as distinct lifecycle concerns: primary detail retains bounded event enrichment, while controller-related rows use a new identity-echoing `resource.relations` query and UI cache. Reuse the existing list/watch machinery for one demand-driven core/v1 Namespace subscription, but expose its complete-snapshot lifecycle to UI as a typed catalog state. Preserve old wire payloads and workspace snapshots through defaulted fields and boundary-time migration.

**Tech Stack:** Rust 2024, Tokio, kube-rs, serde, k10s typed WebSocket protocol, egui/egui_kittest, Cargo integration tests.

**Design:** `docs/superpowers/specs/2026-08-27-resource-details-and-namespace-combobox-design.md`

---

### Task 0: Capture the implementation base

- [ ] **Step 1: Record the pre-implementation commit**

Run: `git rev-parse HEAD`

Expected: save the result as `IMPLEMENTATION_START_SHA` in the execution notes before writing any Task 1 test; later verification must use this value instead of a fixed commit count.

## File Structure

- `crates/k10s-protocol/src/resource.rs`: wire-compatible events condition and relation response types.
- `crates/k10s-protocol/src/lib.rs`: public exports and request-kind constant.
- `crates/k10s-protocol/tests/resource_contract.rs`: serde compatibility and identity echo contracts.
- `crates/k10s-backend/src/kernel.rs`: stop eager relation composition; map independent relation results.
- `crates/k10s-backend/src/port.rs`: carry adapter-domain Event availability on detail records.
- `crates/k10s-backend/src/kube/events.rs`: bound dual Event API enrichment and report availability.
- `crates/k10s-backend/tests/resource_details.rs`: blocked events/relations behavior tests.
- `crates/k10s-server/src/control.rs`: parse `resource.relations` and serialize the typed response.
- `crates/k10s-server/tests/detail_loopback.rs`: end-to-end detail/relations compatibility.
- `crates/k10s-ui/src/client/state.rs`: encode/decode the new query and expose complete Namespace snapshots.
- `crates/k10s-ui/src/app.rs`: detail/relations request registries, generation checks, Namespace subscription demand and feed projection.
- `crates/k10s-ui/src/workspace/resource.rs`: all-namespaces defaults and legacy scope normalization helper.
- `crates/k10s-ui/src/workspace/snapshot.rs`: normalize legacy `ContextDefault` before workspace construction.
- `crates/k10s-ui/src/ui/resource_window.rs`: typed Namespace catalog state and searchable combobox UI.
- `crates/k10s-ui/src/ui/service_window.rs`: use the same authoritative Namespace combobox for Services.
- `crates/k10s-ui/src/ui/mod.rs`: drainable resource-network action queue for primary, relation, and catalog retries.
- `crates/k10s-ui/src/ui/detail/mod.rs`: use independent relation state on the existing controller `Pods` tab and show event availability.
- `crates/k10s-ui/tests/{client_state,workspace_snapshot,workspace_state,ui_resource_windows,ui_details}.rs`: focused client, migration, catalog, combobox, and lazy-tab regressions.

### Task 1: Add wire-compatible detail enrichment contracts

**Files:**
- Modify: `crates/k10s-protocol/src/resource.rs`
- Modify: `crates/k10s-protocol/src/lib.rs`
- Test: `crates/k10s-protocol/tests/resource_contract.rs`

- [ ] **Step 1: Write failing serde contract tests**

Add separate tests proving: a legacy decode fixture without `eventsCondition` or `related` decodes as `EventsCondition::Available` plus an empty related list; a legacy payload with eagerly populated `related` still decodes; a current encode fixture contains `eventsCondition: "available"` and `related: []`; `ResourceRelationsResponse` round-trips the complete identity; and its request kind is exactly `resource.relations`.

- [ ] **Step 2: Run the protocol tests and verify RED**

Run: `cargo test -p k10s-protocol --test resource_contract`

Expected: FAIL because `EventsCondition`, `ResourceRelationsResponse`, and `REQUEST_RESOURCE_RELATIONS` do not exist and `related` is still required.

- [ ] **Step 3: Add the minimal protocol types**

Implement:

```rust
pub const REQUEST_RESOURCE_RELATIONS: &str = "resource.relations";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EventsCondition {
    #[default]
    Available,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceRelationsResponse {
    pub identity: ResourceIdentity,
    pub revision: BackendRevision,
    pub groups: Vec<RelatedGroup>,
}
```

Add `#[serde(default)]` to `ResourceDetailResponse.related` and to the new `events_condition` field. Export the new symbols from `lib.rs`.

- [ ] **Step 4: Run the protocol tests and verify GREEN**

Run: `cargo test -p k10s-protocol --test resource_contract && cargo test -p k10s-protocol --test golden_protocol`

Expected: PASS. The legacy decode fixture remains byte-for-byte unchanged; the current encode/golden fixture is deliberately updated to contain `eventsCondition` and an empty `related` list.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-protocol/src/resource.rs crates/k10s-protocol/src/lib.rs crates/k10s-protocol/tests/resource_contract.rs crates/k10s-protocol/tests/fixtures
git commit -m "feat(protocol): split resource relation responses"
```

### Task 2: Return primary details independently and bound Event enrichment

**Files:**
- Modify: `crates/k10s-backend/src/kernel.rs`
- Modify: `crates/k10s-backend/src/port.rs`
- Modify: `crates/k10s-backend/src/fake.rs`
- Modify: `crates/k10s-backend/src/runtime/cache.rs`
- Modify: `crates/k10s-backend/src/watch.rs`
- Modify: `crates/k10s-backend/src/kube/owners.rs`
- Modify: `crates/k10s-backend/src/kube/events.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Test: `crates/k10s-backend/tests/resource_details.rs`

- [ ] **Step 1: Write failing backend timing and mapping tests**

Add a test adapter whose `ResourceDetail` succeeds while `ResourceRelations` never completes, and assert `Kernel::query(ResourceDetail)` completes under a short Tokio timeout with `related.is_empty()`. Add recorded-service tests where both Event APIs block or fail and assert the detail completes after the configured test budget with `events_condition == Unavailable` and no events.

- [ ] **Step 2: Run the focused backend tests and verify RED**

Run: `cargo test -p k10s-backend --features testkit --test resource_details`

Expected: FAIL because kernel detail awaits relations and event availability is not represented.

- [ ] **Step 3: Remove eager relation composition**

Add adapter-domain `RecordEventsCondition::{Available, Unavailable}` and `ResourceRecord.events_condition` in `port.rs`. Update literals in `fake.rs`, `runtime/cache.rs`, and `watch.rs`; ordinary list/watch/cache records deliberately use `Available` because the condition is only interpreted on authoritative detail records. Change `ResourceDetailResult::new` to accept only `ResourceRecord`, map its event condition, and serialize `related: []`. Remove `detail_reference` capture and the obsolete `adapter_relations` helper.

Add `revision: u64` to backend `RelatedData`. Update `kube/owners.rs` so the real adapter stores the same `watches.next_revision()` already assigned to every returned related row; update `fake.rs` so the fake adapter allocates its normal monotonic query revision and stamps both `RelatedData` and returned rows with it. Update all constructors/tests instead of deriving a revision from the reference. Then add `ResourceRelationsResult`, `KernelQueryResult::ResourceRelations`, and its `serialized`/typed payload match arms. Replace the current rejection of adapter `QueryResult::ResourceRelations` with mapping that echoes the exact reference identity, authoritative relation revision, and groups into `ResourceRelationsResponse`.

- [ ] **Step 4: Bound Event reads without leaking errors**

Give `events_for` a total deadline parameter or a testable wrapper. Run the core/v1 and events.k8s.io/v1 reads under one one-second production budget; any timeout/forbidden/unavailable outcome returns `(Vec::new(), RecordEventsCondition::Unavailable)`. Successful reads preserve existing dedupe/sort behavior and return `Available`; `resource_detail` writes both values into the record.

- [ ] **Step 5: Run backend tests and verify GREEN**

Run: `cargo test -p k10s-backend --features testkit --test resource_details && cargo test -p k10s-backend --features testkit --test kube_contract`

Expected: PASS; primary detail no longer invokes relations.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-backend/src/kernel.rs crates/k10s-backend/src/port.rs crates/k10s-backend/src/fake.rs crates/k10s-backend/src/runtime/cache.rs crates/k10s-backend/src/watch.rs crates/k10s-backend/src/kube/events.rs crates/k10s-backend/src/kube/mod.rs crates/k10s-backend/src/kube/owners.rs crates/k10s-backend/tests/resource_details.rs
git commit -m "fix(backend): return resource details before relations"
```

### Task 3: Route independent relations through server and client

**Files:**
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Test: `crates/k10s-server/tests/detail_loopback.rs`
- Test: `crates/k10s-ui/tests/client_state.rs`

- [ ] **Step 1: Write failing server/client request tests**

Assert `Query::ResourceRelations(identity)` emits request kind `resource.relations` with `ResourceRefRequest`, decodes `ResourceRelationsResponse`, and the server parses it into backend `Query::ResourceRelations`. In client-state tests, inject a request-scoped `unsupportedMessage` against that exact pending request and assert `take_failure()` returns the typed safe failure without invalidating primary detail. In server loopback, assert the new server accepts the new request and also continues emitting legacy-compatible detail fields.

- [ ] **Step 2: Run focused tests and verify RED**

Run: `cargo test -p k10s-ui --test client_state resource_relations && cargo test -p k10s-server --test detail_loopback resource_relations`

Expected: FAIL because the UI query/result and control parser route do not exist.

- [ ] **Step 3: Implement the minimal query/result route**

Add `Query::ResourceRelations(ResourceIdentity)` and `QueryResult::ResourceRelations(Box<ResourceRelationsResponse>)` to client state, encode/decode them beside `ResourceDetail`, and retain request failures for this query just as infrastructure failures are retained so `take_failure()` can consume a typed `UnsupportedMessage`. Add the exact request-kind parser arm in server control and the `KernelQueryResult::ResourceRelations` response branch beside `ResourceDetail` so the server emits its typed payload.

- [ ] **Step 4: Run focused and compatibility tests**

Run: `cargo test -p k10s-ui --test client_state && cargo test -p k10s-server --test detail_loopback --test kube_detail_loopback`

Expected: PASS, including new-client/unsupported-old-server behavior.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-server/src/control.rs crates/k10s-server/tests/detail_loopback.rs crates/k10s-ui/src/client/state.rs crates/k10s-ui/tests/client_state.rs
git commit -m "feat: route lazy resource relations"
```

### Task 4: Add identity- and generation-safe relation state to the application

**Files:**
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Test: `crates/k10s-ui/tests/ui_details.rs`
- Test: `crates/k10s-ui/tests/ui_resilience.rs`

- [ ] **Step 1: Write failing lazy-load UI/application tests**

Cover: primary Deployment detail renders before relations; a primary-detail server failure renders a bounded failure rather than perpetual Loading; selecting the existing `Pods` tab queues exactly one relation request; loading, empty, loaded, and safe failed states render independently; Retry replaces one failed request; and responses with another UID, connection generation, or retired context are discarded. Use an injectable/test clock to verify the 30-second stale refresh without sleeping. Assert leaving the tab retains the request; selection change retires its visible binding; context retirement and transport loss clear primary details, relation caches, and pending state before recovery reissues current demand.

- [ ] **Step 2: Run UI tests and verify RED**

Run: `cargo test -p k10s-ui --test ui_details --test ui_resilience`

Expected: FAIL because related rows still come from `ResourceDetailResponse.related` and no independent lifecycle exists.

- [ ] **Step 3: Add focused relation cache/request state**

In `app.rs`, replace the split primary maps with an explicit per-identity `PrimaryDetailState::{Loading, Loaded(ResourceDetailResponse), Failed(SafeUiError)}` plus pending request metadata. A request-scoped failure moves to Failed; retry/selection/context/transport transitions clear or replace it deliberately. Project this state through `ResourceFeed`, and make `detail/mod.rs` render the safe Failed state with Retry instead of treating every absent response as Loading.

Add a separate per-identity relation state carrying `NotRequested | Loading | Loaded { response, loaded_at_ms, refreshing } | Failed { safe_error }`, plus pending request ID and connection generation. Trigger demand only for a selected controller detail whose active tab is `DetailTab::Pods`. Validate request ID, echoed identity, and generation before cache insertion. Leaving the tab retains in-flight work; selection change retires cache visibility and cancels when no other window pins that identity; context retirement and transport loss clear all primary/relation cache and pending state.

Define `ResourceAction::{RetryPrimary(ResourceIdentity), RetryRelations(ResourceIdentity), RetryNamespaceCatalog}` in `ui/mod.rs`, store it in a shell-owned queue, and expose `UiShell::drain_resource_actions()`. Detail Retry buttons enqueue the exact pinned identity rather than a `WorkspaceCommand`. After each shell render, `app.rs` drains actions and performs one Failed→Loading replacement request; repeated frames without another click enqueue nothing.

- [ ] **Step 4: Project and render relation state**

Extend `ResourceFeed` with read-only primary and relation states keyed by identity. Change the general detail body to distinguish Loading/Loaded/Failed and queue Retry; change `DetailTab::Pods` to render its own loading/stale/failed/retry states and independent groups; ignore legacy `view.related`. Show `EventsCondition::Unavailable` as a safe Events-tab unavailable message.

- [ ] **Step 5: Run UI tests and verify GREEN**

Run: `cargo test -p k10s-ui --test ui_details --test ui_resilience --test ui_resource_windows`

Expected: PASS with no duplicate outbound requests across repeated frames.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src/app.rs crates/k10s-ui/src/ui/mod.rs crates/k10s-ui/src/ui/resource_window.rs crates/k10s-ui/src/ui/detail/mod.rs crates/k10s-ui/tests/ui_details.rs crates/k10s-ui/tests/ui_resilience.rs
git commit -m "feat(ui): load related resources lazily"
```

### Task 5: Make all-namespaces the canonical workspace default

**Files:**
- Modify: `crates/k10s-ui/src/workspace/resource.rs`
- Modify: `crates/k10s-ui/src/workspace/service.rs`
- Modify: `crates/k10s-ui/src/workspace/snapshot.rs`
- Test: `crates/k10s-ui/tests/workspace_state.rs`
- Test: `crates/k10s-ui/tests/workspace_snapshot.rs`

- [ ] **Step 1: Write failing default and migration tests**

Assert new resource and Service windows use `AllNamespaces`; snapshot restoration converts legacy `ContextDefault` to `AllNamespaces` before creating workspace state; explicit namespace scopes remain unchanged.

- [ ] **Step 2: Run workspace tests and verify RED**

Run: `cargo test -p k10s-ui --test workspace_state --test workspace_snapshot`

Expected: FAIL because defaults currently use `ContextDefault`.

- [ ] **Step 3: Implement boundary normalization**

Set both state defaults to `AllNamespaces`. Keep `ContextDefault` deserializable, but map it to `AllNamespaces` while converting `WorkspaceSnapshot` into live state, before any subscription reconciliation.

- [ ] **Step 4: Run workspace tests and verify GREEN**

Run: `cargo test -p k10s-ui --test workspace_state --test workspace_snapshot`

Expected: PASS; update assertions that intentionally encoded the old default.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui/src/workspace/resource.rs crates/k10s-ui/src/workspace/service.rs crates/k10s-ui/src/workspace/snapshot.rs crates/k10s-ui/tests/workspace_state.rs crates/k10s-ui/tests/workspace_snapshot.rs
git commit -m "fix(workspace): default namespace scope to all"
```

### Task 6: Add the complete demand-driven Namespace catalog

**Files:**
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Test: `crates/k10s-ui/tests/client_state.rs`
- Test: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`

- [ ] **Step 1: Write failing catalog lifecycle tests**

Cover demand from Pods/Deployments/Services and selected namespaced custom GVKs; no demand from cluster-scoped or unselected custom GVKs; one shared core/v1 Namespace subscription; partial snapshot pages remain Loading; snapshot end yields sorted/deduplicated Ready names; watch deltas update Ready; resync and transport loss clear names and return to Loading; forbidden rejection becomes Unavailable; recovery resubscribes once and repopulates only after a complete snapshot.

- [ ] **Step 2: Run catalog tests and verify RED**

Run: `cargo test -p k10s-ui --test client_state namespace_catalog && cargo test -p k10s-ui --test ui_resource_windows namespace_catalog && cargo test -p k10s-ui --test ui_services namespace_catalog`

Expected: FAIL because no Namespace subscription/feed state exists.

- [ ] **Step 3: Reconcile Namespace demand**

In the existing subscription reconciliation path, calculate whether any open Service or discovery-declared namespaced resource window demands the catalog. Add a dedicated `namespace_subscription: Option<(String, LiveSubscription)>` to `K10sApp`; do not insert it in `window_subscriptions` or give it fake window owners. Add `namespace_catalog_load: Loading | Unavailable(SafeUiError)` beside it. Include add/remove in subscription preflight capacity, unsubscribe when final demand disappears or context changes, and rely on retained client desired-subscription recovery after reconnect.

Match subscription-scoped errors by the Namespace subscription ID, call `retire_rejected_subscription`, retain only the protocol safe message/code in `SafeUiError`, and transition to Unavailable. Passive reconciliation must not recreate a rejected subscription every frame. The Unavailable combobox area renders Retry, which enqueues `ResourceAction::RetryNamespaceCatalog`; `app.rs` drains it, clears the rejection guard, and creates exactly one replacement subscription. Context change, demand falling to zero then returning, or reconnect generation change also clear the guard. On transport loss/resync, clear names and error state and expose Loading until a completed snapshot/live state arrives; assembling pages are never exposed.

- [ ] **Step 4: Project typed feed lifecycle**

Add a small typed and non-raw UI error plus catalog state:

```rust
pub struct SafeUiError {
    pub message: String,
}

pub enum NamespaceCatalogState {
    NotDemanded,
    Loading,
    Ready(Vec<String>),
    Unavailable(SafeUiError),
}
```

Construct `SafeUiError` only from the protocol's `safe_message` (never raw Kubernetes status/details). Populate `ResourceFeed.namespace_catalog` from subscription/request state. Sort and deduplicate by namespace identity name; test that raw backend details never render.

- [ ] **Step 5: Run catalog tests and verify GREEN**

Run: `cargo test -p k10s-ui --test client_state --test ui_resource_windows --test ui_services`

Expected: PASS; the catalog subscription retires when its final demander closes, and one Retry click creates exactly one replacement subscription across repeated frames.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src/app.rs crates/k10s-ui/src/ui/mod.rs crates/k10s-ui/src/ui/resource_window.rs crates/k10s-ui/src/ui/service_window.rs crates/k10s-ui/tests/client_state.rs crates/k10s-ui/tests/ui_resource_windows.rs crates/k10s-ui/tests/ui_services.rs
git commit -m "feat(ui): watch complete namespace catalog"
```

### Task 7: Replace namespace TextEdit with a searchable combobox

**Files:**
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Test: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`

- [ ] **Step 1: Write failing egui interaction tests**

For both workload and Service windows, assert there is no `TextInput` labelled `Namespace filter`; a combobox button opens a popup with a search `TextInput`; search narrows only authoritative Ready options; selecting applies `Namespace(name)`; Clear applies `AllNamespaces`; Loading/Unavailable disables selection; and each window retains independent popup search scratch.

Add tests for a restored/deleted selected namespace: keep its scoped workspace state, display “namespace no longer exists,” and never queue `AllNamespaces` until Clear is clicked.

- [ ] **Step 2: Run UI tests and verify RED**

Run: `cargo test -p k10s-ui --test ui_resource_windows namespace_combobox && cargo test -p k10s-ui --test ui_services namespace_combobox`

Expected: FAIL because the current namespace control is a free-form `TextEdit`.

- [ ] **Step 3: Implement the minimal combobox**

Extract one small `namespace_combobox` renderer and per-window scratch map owned by the shell UI state so both `resource_window.rs` and `service_window.rs` use identical behavior. Use `egui::ComboBox`/popup content with a search edit and filtered buttons. Closed text is the explicit namespace or `All namespaces`. Clear explicitly queues `SetNamespaceScope(window_id, AllNamespaces)`. Never synthesize arbitrary namespaces from search input. Remove every Service `ContextDefault` label/action and free-form editor.

- [ ] **Step 4: Run UI tests and verify GREEN**

Run: `cargo test -p k10s-ui --test ui_resource_windows --test ui_services`

Expected: PASS at normal and compact viewport sizes, including cluster-scoped absence.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui/src/ui/resource_window.rs crates/k10s-ui/src/ui/service_window.rs crates/k10s-ui/tests/ui_resource_windows.rs crates/k10s-ui/tests/ui_services.rs
git commit -m "fix(ui): use searchable namespace combobox"
```

### Task 8: Full regression and acceptance verification

**Files:**
- Modify only files required by failures directly caused by Tasks 1–7.

- [ ] **Step 1: Format and run static checks**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --all-features -- -D warnings`

Expected: PASS with no warnings.

- [ ] **Step 2: Run the workspace test suite**

Run: `cargo test --workspace --all-features`

Expected: PASS.

- [ ] **Step 3: Run browser acceptance tests**

Run: `npx playwright test`

Expected: PASS for `playwright.config.ts` and `tests/browser/*.spec.ts`.

- [ ] **Step 4: Inspect the final diff and commits**

Run: `git status --short && git diff "$IMPLEMENTATION_START_SHA"..HEAD --check && git log --oneline "$IMPLEMENTATION_START_SHA"..HEAD`

Expected: clean worktree, no whitespace errors, and focused commits corresponding to the plan tasks, regardless of whether fixture-only commits were needed.

- [ ] **Step 5: Commit any verification-only fixture updates**

```bash
git add <only directly affected snapshots or fixtures>
git commit -m "test: update detail and namespace acceptance fixtures"
```
