# k10s Kubernetes Read Path Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace fake read behavior with a real kube-rs adapter supporting kubeconfig contexts, discovery, demand-driven list/watch caches, details, events, owner traversal, RBAC projections, and Resource Metrics API data.

**Architecture:** The kube-rs adapter remains internal to `k10s-backend` and satisfies the existing Kubernetes seam. `ClusterRuntime` normalizes typed built-ins and dynamic resources into the already-shipped protocol. Recoverable watchers feed atomic caches; UI and server code remain unchanged except for configuration that chooses fake or real mode.

**Tech Stack:** kube 4.2.0, compatible k8s-openapi, Tokio, existing Backend Kernel and WebSocket protocol, ephemeral kind for E2E.

---

## File map

- `crates/k10s-backend/src/kube/{mod,config,discovery,read,watch,normalize,metrics,permissions}.rs`: real adapter.
- `crates/k10s-backend/src/runtime/{mod,context,cluster,cache,supervisor}.rs`: context and watch ownership.
- `crates/k10s-backend/tests/kube_contract.rs`: shared fake/real behavior contract.
- `tests/kind/*`: manifests and E2E helpers.

All new behavior extends the Plan 1 `KubernetesAccess` port and is reachable only through `BackendKernel`, server, and shared client. `kube_contract.rs` runs the same behavior suite against `FakeKubernetes` and `KubeAdapter`. All Cargo commands use `--locked`.

### Task 1: Load kubeconfig contexts without leaking credentials

**Files:** create `kube/{mod,config}.rs`, create `runtime/{mod,context}.rs`, modify backend `{lib,port,kernel}.rs`, create `crates/k10s-backend/tests/kube_contract.rs`, modify `crates/k10s-server/src/config.rs`, modify `apps/k10s-server/src/main.rs`, modify `apps/k10s-desktop/src/main.rs`, create `crates/k10s-server/tests/backend_mode.rs`, create `apps/k10s-desktop/tests/kube_factory.rs`, test `tests/context_registry.rs`.

- [ ] Write failing tests for context enumeration, explicit kubeconfig path, current context, missing file, invalid exec plugin, safe serialized summaries containing no tokens/cert data, and runtime selection `BackendMode::Fake | Kube { kubeconfig }` in standalone and embedded entry points.
- [ ] Run `cargo test --locked -p k10s-backend --test context_registry`; expect missing real adapter.
- [ ] Implement `KubeAdapter::from_kubeconfig`, prepare-then-commit `ContextRegistry`, and a backend factory selected by validated server/desktop configuration. Plan 3 defaults normal launches to `Kube`; tests and an explicit `--fake` development flag select the fake. Keep kube types internal and normalize errors.
- [ ] Run `cargo test --locked -p k10s-backend --test context_registry && cargo test --locked -p k10s-backend --test kube_contract bootstrap && cargo test --locked -p k10s-server --test backend_mode && cargo test --locked -p k10s-desktop --test kube_factory`; expect both adapters to return the same protocol shape, the desktop launcher to construct Kube mode without a direct kernel shortcut, and `--fake` to be explicit.
- [ ] Commit `feat: load kubernetes contexts`.

### Task 2: Implement discovery and dynamic resource catalog

**Files:** create `kube/discovery.rs`, modify `runtime/cluster.rs`, modify backend `{port,kernel}.rs`, modify server control/client state, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/discovery.rs` and `crates/k10s-server/tests/kube_discovery_loopback.rs`.

- [ ] Write failing tests for built-in GVK/scope, CRD search by kind/plural/group/version, scale subresource capability, unavailable GVK, and discovery refresh.
- [ ] Run the focused test; expect failure.
- [ ] Use kube discovery to create normalized `ApiResourceDescriptor`; cache by context with refresh/invalidation and no raw discovery types in protocol payloads.
- [ ] Run discovery tests, the real control-socket loopback, and the shared fake/real contract against a recorded Tower Kubernetes service; expect PASS.
- [ ] Commit `feat: discover kubernetes resources`.

### Task 3: Build supervised demand-driven watchers and atomic caches

**Files:** create `kube/watch.rs`, create `runtime/{cluster,cache,supervisor}.rs`, modify backend `{watch,port,kernel}.rs`, modify server control/client state, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/watch_runtime.rs` and `crates/k10s-server/tests/kube_watch_loopback.rs`.

- [ ] Write failing tests for first-subscriber start, shared subscriber, lingered final unsubscribe, `Init/InitApply/InitDone`, apply/delete, restart, stale cache during relist, and atomic replacement.
- [ ] Run `cargo test --locked -p k10s-backend --test watch_runtime`; expect missing runtime.
- [ ] Implement one supervised watcher per `(context, GVK, scope)`, child cancellation tokens, normalized summary cache, opaque Kubernetes resourceVersion, and monotonic BackendRevision.
- [ ] Run watch tests with scripted watcher streams plus the loopback reconnect/full-resync test; verify bounded snapshot chunks, P2 same-resource coalescing, lossless P1 subscription lifecycle, no half-initialized snapshot, and task exit after linger.
- [ ] Commit `feat: add kubernetes watch runtime`.

### Task 4: Normalize built-in and dynamic resource lists

**Files:** create `kube/normalize.rs`, create `kube/read.rs`, modify `runtime/cache.rs`, modify backend `{port,kernel}.rs`, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/resource_normalization.rs`.

- [ ] Write failing table-driven tests for Deployments, Pods, StatefulSets, DaemonSets, Jobs, CronJobs, Nodes, PVCs, PVs, StorageClasses, and cluster/namespaced DynamicObjects.
- [ ] Run focused tests; expect missing normalizers.
- [ ] Implement typed built-in normalizers and a standard-metadata dynamic normalizer. Store list view models only; fetch YAML/detail on demand. Preserve quantities and timestamps without lossy guessing.
- [ ] Run normalization and protocol golden tests; expect PASS.
- [ ] Commit `feat: normalize kubernetes resource lists`.

### Task 5: Implement details, related pods, YAML reads, and events

**Files:** modify `kube/read.rs`, create `kube/owners.rs`, create `kube/events.rs`, modify backend `{port,kernel}.rs`, modify server/client control state, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/resource_details.rs` and `crates/k10s-server/tests/kube_detail_loopback.rs`.

- [ ] Write failing tests for exact identity get, resource-gone, Deployment→ReplicaSet→Pod controller UID traversal, tailored detail fields, newest-first events, and YAML bound to UID/resourceVersion.
- [ ] Run focused tests; expect failures.
- [ ] Implement on-demand reads; never resolve ownership by labels or reused names. Normalize Event API variants into the existing protocol.
- [ ] Run fake/real adapter contract tests with recorded responses; expect identical observable behavior.
- [ ] Commit `feat: read kubernetes resource details`.

### Task 6: Add Resource Metrics API polling and partial coverage

**Files:** create `kube/metrics.rs`, modify `runtime/cluster.rs`, modify backend `{port,kernel}.rs`, modify server control, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/metrics_collector.rs`.

- [ ] Write failing tests for discovered/absent/forbidden Metrics API, full and partial NodeMetrics, stale timestamps, PodMetrics, poll start/linger/stop, and pod capacity from core Node allocatable.
- [ ] Run focused tests; expect missing collector.
- [ ] Poll `metrics.k8s.io/v1beta1` only with active consumers; cache timestamp/window and coverage. Never infer usage from requests/capacity or map missing data to zero.
- [ ] Run collector plus UI infrastructure tests; expect PASS for available/partial/unavailable projections.
- [ ] Commit `feat: collect kubernetes resource metrics`.

### Task 7: Project permissions and context-switch read behavior

**Files:** create `kube/permissions.rs`, modify `runtime/context.rs`, modify backend `{port,kernel}.rs`, modify server/client control state, modify `crates/k10s-backend/tests/kube_contract.rs`, test `tests/context_switch.rs`.

- [ ] Write failing tests for SelfSubjectAccessReview projection, forbidden review fallback, destination prepare failure preserving current context, selection clearing, unavailable GVK, and previous runtime retirement.
- [ ] Run focused tests; expect failures.
- [ ] Implement capability projection as advisory metadata and prepare-then-commit context switching. Do not bypass Kubernetes authorization on later operations.
- [ ] Run context and UI guard tests; expect PASS.
- [ ] Commit `feat: add kubernetes context switching`.

### Task 8: Verify the read path against an ephemeral cluster

**Files:** create `tests/kind/{cluster.sh,fixtures.yaml,metrics-fixture.yaml}`, create `crates/k10s-backend/tests/kind_read_path.rs`, create `crates/k10s-server/tests/kind_server_read_path.rs`, modify CI and README.

- [ ] Write ignored E2E tests for contexts, discovery, built-ins, CRD, watch apply/delete, owner traversal, events, forbidden namespace, metrics absent/partial, and watch restart.
- [ ] Run unit suite first, then `cargo test --locked -p k10s-backend --test kind_read_path -- --ignored --nocapture`; expect initial E2E failure until harness exists.
- [ ] Implement a deterministic kind harness with explicit cleanup, fixture namespaces, CRD, least-privilege service account, and optional fake metrics API responses.
- [ ] Launch `k10s-server --kubeconfig ...`, connect through the real control WebSocket, and run the full E2E. Assert that configured Kube data—not fake fixture names—served contexts, discovery, list, detail, related pods, events, YAML, metrics, RBAC, deletion, and watch recovery. Execute `cargo test --locked -p k10s-backend --test kind_read_path -- --ignored --nocapture`, `cargo test --locked -p k10s-server --test kind_server_read_path -- --ignored --nocapture`, and `cargo test --locked -p k10s-desktop --test kube_factory`, then run the documented native/web smoke.
- [ ] Commit `test: verify kubernetes read path`.

## Plan 3 verification gate

- Switching configuration from fake to real adapter requires no UI/protocol changes.
- Real lists and details update through live watchers and normalized caches.
- Kubernetes resourceVersion remains opaque and backend-only.
- Relist is atomic and old data remains visibly stale until replacement.
- Metrics handle absent, forbidden, partial, and stale data without false zeroes.
- kind E2E verifies built-ins, CRDs, ownership, events, RBAC, and watch recovery.
- Standalone and embedded entry points select the Kube adapter through validated configuration; no manual kernel construction exists outside the backend factory.
