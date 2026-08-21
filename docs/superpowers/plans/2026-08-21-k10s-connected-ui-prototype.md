# k10s Connected UI Prototype Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the complete approved egui console on native and web while every list, detail, mutation, log, shell, metric, and failure state travels through the real WebSocket protocol and deterministic fake Kubernetes adapter.

**Architecture:** UI state remains local to `k10s-ui`; authoritative snapshots, validation tickets, operation states, and streams live in `BackendKernel`. Protocol payloads use normalized view models. The fake adapter deterministically advances only when commanded by tests, making every approved UI state reproducible without cluster infrastructure.

**Tech Stack:** The Plan 1 workspace; egui/eframe/egui_extras 0.36.1; egui_kittest 0.36.1; Serde JSON control frames; binary log/exec frames.

---

## File map

- `crates/k10s-protocol/src/{resource,subscription,operation,stream,metrics}.rs`: UI-facing payloads.
- `crates/k10s-backend/src/{catalog,watch,operation,stream,fake/*}.rs`: normalized fake behavior.
- `crates/k10s-ui/src/workspace/*`: window, guard, split, and editor state.
- `crates/k10s-ui/src/ui/*`: shell, resource windows, details, tools, and dialogs.
- `crates/k10s-ui/tests/*`: pure state and AccessKit tests.
- `crates/k10s-ui/tests/snapshots/*`: approved visual states.

Every task that introduces a data-bearing behavior must change and test the entire path in the same commit: normalized protocol payload → `KubernetesAccess` behavior → `BackendKernel::{query,execute,subscribe}` → fake adapter → Axum route → shared client state → UI projection. Validation and stream-ticket issuance are queries, commands return `OperationId`, and dedicated stream redemption uses the kernel-owned Stream Hub behind `subscribe`. Each such task adds a loopback integration test under `crates/k10s-server/tests/`; a direct UI-to-fake import is a test failure. All Cargo commands below include `--locked`.

### Task 1: Define normalized resource and subscription contracts

**Files:**
- Create: `crates/k10s-protocol/src/resource.rs`
- Create: `crates/k10s-protocol/src/subscription.rs`
- Create: `crates/k10s-protocol/src/metrics.rs`
- Modify: `crates/k10s-protocol/src/lib.rs`
- Create: `crates/k10s-protocol/tests/resource_contract.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-server/tests/resource_loopback.rs`

- [ ] **Step 1: Write failing contract tests** for stable `ResourceIdentity { context, gvk, namespace, name, uid }`, namespaced/cluster scope, every designed workload kind, `SnapshotBegin/Chunk/End`, monotonic `BackendRevision`, metrics `Available/Partial/Unavailable`, and resource-gone events.
- [ ] **Step 2: Run `cargo test --locked -p k10s-protocol --test resource_contract`** and expect unresolved types.
- [ ] **Step 3: Implement the exact payloads** with normalized list rows, detail sections, events, capabilities, timestamps, and no kube-rs types. Extend the existing port/kernel/fake path and control/client dispatch; add fake contexts, built-ins, CRDs, metrics, owner references, and deterministic timestamps behind the adapter.
- [ ] **Step 4: Run `cargo test --locked -p k10s-protocol && cargo test --locked -p k10s-backend && cargo test --locked -p k10s-server --test resource_loopback`** and expect PASS through a real socket.
- [ ] **Step 5: Commit:** `git commit -am "feat: define resource subscription contracts"` after adding new files.

### Task 2: Implement workspace, window registry, and navigation guards

**Files:**
- Create: `crates/k10s-ui/src/workspace/{mod,window,resource,detail,guard}.rs`
- Modify: `crates/k10s-ui/src/lib.rs`
- Create: `crates/k10s-ui/tests/workspace_state.rs`

- [ ] **Step 1: Write failing pure tests** for Overview-only startup, singleton focus, multiple workload instances, MRU, pinned dedicated details, independent filters/splits, dirty YAML guards, connected-shell guards, and context-switch cancellation.
- [ ] **Step 2: Run `cargo test --locked -p k10s-ui --test workspace_state`** and expect missing state types.
- [ ] **Step 3: Implement command-driven state** with stable `WindowId`, queued `WorkspaceCommand`, one writable YAML owner per identity, and `PendingNavigation` that commits only after every blocker resolves.
- [ ] **Step 4: Run `cargo test --locked -p k10s-ui --test workspace_state`** and expect PASS without egui initialization.
- [ ] **Step 5: Commit:** add workspace files and commit `feat: add k10s workspace state`.

### Task 3: Build the top bar, launcher, and window canvas

**Files:**
- Create: `crates/k10s-ui/src/ui/{mod,theme,top_bar,launcher,window}.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Create: `crates/k10s-ui/tests/ui_shell.rs`

- [ ] **Step 1: Write failing egui_kittest tests** for menus, connection state, context selector, fixed launcher, workload expansion, highlight/count/plus behavior, absence of checkbox roles, singleton behavior, and staggered windows.
- [ ] **Step 2: Run `cargo test --locked -p k10s-ui --test ui_shell`** and expect missing labels.
- [ ] **Step 3: Render the approved shell** with stable egui IDs and accessibility labels. Apply queued workspace commands after rendering to avoid borrow conflicts. Use default dark visuals with only approved status colors and density.
- [ ] **Step 4: Run shell tests and `cargo clippy --locked -p k10s-ui --all-targets -- -D warnings`**; expect PASS.
- [ ] **Step 5: Commit:** `feat: build k10s workspace shell`.

### Task 4: Implement Overview, Nodes, Storage, and metrics states

**Files:**
- Create: `crates/k10s-ui/src/ui/{overview,infrastructure}.rs`
- Create: `crates/k10s-backend/src/catalog.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-protocol/src/{resource,metrics}.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/tests/ui_infrastructure.rs`
- Create: `crates/k10s-server/tests/infrastructure_loopback.rs`

- [ ] **Step 1: Write failing tests** for totals, attention rows, node/storage columns, progress bars with text, `Available/Partial/Unavailable`, last-updated timestamps, and no missing metric rendered as zero.
- [ ] **Step 2: Run `cargo test --locked -p k10s-ui --test ui_infrastructure`** and expect missing content.
- [ ] **Step 3: Add protocol queries and backend projections** through the existing port/kernel/control/client chain, then render Overview/Nodes/Storage exclusively from responses. Fake metrics must cover full, partial, forbidden, and stale cases. Feed telemetry through the already bounded P2/coalescing scheduler.
- [ ] **Step 4: Run UI/backend/loopback tests**; expect PASS on native plus `cargo check --locked -p k10s-ui --target wasm32-unknown-unknown`.
- [ ] **Step 5: Commit:** `feat: add overview nodes and storage views`.

### Task 5: Implement workload lists, CRD picker, and split details

**Files:**
- Create: `crates/k10s-ui/src/ui/{resource_window,resource_table,split}.rs`
- Create: `crates/k10s-backend/src/watch.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-protocol/src/subscription.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Create: `crates/k10s-server/tests/subscription_loopback.rs`

- [ ] **Step 1: Write failing tests** for all seven workload kinds, independent namespace/filter/sort, cluster-scoped CRD behavior, searchable GVK picker, 640×420 minimum, 120/180 pane minima, hide/restore, selection, and snapshot resync.
- [ ] **Step 2: Run the focused tests** and expect missing subscription and UI behavior.
- [ ] **Step 3: Implement fake subscriptions end-to-end** through the existing port/kernel/control/client chain: first subscriber starts a fake watch, subscribers receive bounded chunked snapshots and P2 resource deltas coalesced only by resource identity, while P1 subscription lifecycle frames remain lossless. UI applies only contiguous revisions, and list/detail scroll areas use stable distinct IDs. Force a socket drop in the loopback test and prove the Plan 1 baseline reconnect performs a full bootstrap/resubscribe/resync while preserving windows and filters.
- [ ] **Step 4: Run `cargo test --locked -p k10s-ui --test ui_resource_windows` plus `cargo test --locked -p k10s-server --test subscription_loopback`**; expect PASS.
- [ ] **Step 5: Commit:** `feat: add connected workload windows`.

### Task 6: Implement kind-specific details and hybrid popouts

**Files:**
- Create: `crates/k10s-ui/src/ui/detail/{mod,overview,related,events}.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-protocol/src/resource.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/tests/ui_details.rs`
- Create: `crates/k10s-server/tests/detail_loopback.rs`

- [ ] **Step 1: Write failing contract tests** for exact tabs/actions per kind, identity header, Deployment→ReplicaSet→Pod controller-UID traversal, single-click integrated detail, double-click/context-menu popout, pinned identity, and independent tabs.
- [ ] **Step 2: Run `cargo test --locked -p k10s-ui --test ui_details`** and expect failures.
- [ ] **Step 3: Implement detail queries and rendering** through the full loopback path. Owner traversal belongs to Backend Kernel; UI receives resolved related rows. Dedicated windows clone stable identity and never read later integrated selection.
- [ ] **Step 4: Run detail, workspace, protocol, and `detail_loopback` tests**; expect PASS.
- [ ] **Step 5: Commit:** `feat: add workload detail views`.

### Task 7: Implement guarded YAML editing through fake validation tickets

**Files:**
- Create: `crates/k10s-protocol/src/operation.rs`
- Create: `crates/k10s-backend/src/operation.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/src/ui/tools/{mod,yaml}.rs`
- Create: `crates/k10s-ui/tests/yaml_workflow.rs`
- Create: `crates/k10s-server/tests/validation_loopback.rs`

- [ ] **Step 1: Write failing tests** for read-only default, single writer, edit/review/diff, buffer hash, target identity/resourceVersion binding, fake schema/dry-run results, disruptive warning, conflict preservation, invalidation, and Apply gating.
- [ ] **Step 2: Run `cargo test --locked -p k10s-ui --test yaml_workflow`** and expect missing ticket state.
- [ ] **Step 3: Implement validation and ticket issuance through `BackendKernel::query`** and the full UI state machine. The fake adapter returns deterministic validation errors/conflicts; UI never fabricates authoritative success. Applying the validated ticket is a separate command returning `OperationId`.
- [ ] **Step 4: Run UI/backend/loopback operation tests**; expect PASS, including reconnect preserving the dirty buffer while invalidating stale tickets.
- [ ] **Step 5: Commit:** `feat: add guarded yaml workflow`.

### Task 8: Implement Logs and Exec stream sockets with fake sessions

**Files:**
- Create: `crates/k10s-protocol/src/stream.rs`
- Create: `crates/k10s-backend/src/stream.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Create: `crates/k10s-server/src/{logs,exec}.rs`
- Modify: `crates/k10s-server/src/{config,lib}.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/src/ui/tools/{logs,shell}.rs`
- Create: `crates/k10s-ui/tests/stream_tools.rs`
- Create: `crates/k10s-server/tests/stream_sockets.rs`

- [ ] **Step 1: Write failing tests** for the exact `LOGS_PATH`/`EXEC_PATH`, missing/wrong/correct token in the mandatory first `Hello` frame before ticket redemption, separate frame and assembled-message limits on both routes, fragmented oversized `Hello` on both routes, fragmented oversized Exec input and resize/control messages, single-use bound tickets, selected container, tail/since/follow/pause/find, truncation, explicit shell connect, TTY stdin/merged-output/resize/exit, RBAC/missing-binary errors, and terminal disconnect on socket loss. If a non-TTY exec fixture is retained, test its stdout/stderr separation as a distinct mode.
- [ ] **Step 2: Run focused UI/server tests** and expect missing stream routes.
- [ ] **Step 3: Implement dedicated bounded sockets** by issuing tickets through `BackendKernel::query` and redeeming them in the kernel-owned Stream Hub behind `BackendKernel::subscribe`. Guard each Logs and Exec upgrade with separate individual-frame and assembled-message limits that apply across fragmentation before authentication or payload dispatch, plus the Plan 1 connection semaphore, bounded per-stream queues, rate budgets, and explicit overload closure. Authenticate `Hello` before accepting the single-use ticket. Use JSON handshake/status frames and versioned binary payload headers. Fake streams advance on explicit test ticks only; no command or process executes.
- [ ] **Step 4: Run stream tests and `cargo check --locked -p k10s-ui --target wasm32-unknown-unknown`**; expect PASS and no token in URLs.
- [ ] **Step 5: Commit:** `feat: add connected log and shell tools`.

### Task 9: Implement fake mutations, dialogs, and operation updates

**Files:**
- Modify: `crates/k10s-backend/src/operation.rs`
- Modify: `crates/k10s-backend/src/{port,kernel,fake}.rs`
- Modify: `crates/k10s-protocol/src/operation.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Create: `crates/k10s-ui/src/ui/dialogs.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Create: `crates/k10s-ui/tests/operation_dialogs.rs`
- Create: `crates/k10s-server/tests/operation_loopback.rs`

- [ ] **Step 1: Write failing tests** for the complete action matrix, exact scope identity, typed delete, propagation modes, disabled reasons, idempotency, duplicate prevention, progress, success/failure/unknown, retry eligibility, refresh-before-retry, and querying every nonterminal `OperationId` after a forced control reconnect.
- [ ] **Step 2: Run `cargo test --locked -p k10s-ui --test operation_dialogs`** and expect missing dialogs.
- [ ] **Step 3: Implement operation commands and deterministic fake advancement** through `BackendKernel::execute`. Submission returns `OperationId`; terminal state arrives through the bounded P0 reserve and is traced with operation/correlation IDs. On reconnect/full resync, query every nonterminal operation by `OperationId` and reuse the bounded idempotency record before allowing a retry. Mutate only backend fake state, causing normal resource deltas.
- [ ] **Step 4: Run operation, subscription, real-socket recovery, and UI tests**; expect PASS.
- [ ] **Step 5: Commit:** `feat: add workload operation workflows`.

### Task 10: Complete resilient states, accessibility, and snapshots

**Files:**
- Modify: `crates/k10s-ui/src/ui/*`
- Create: `crates/k10s-ui/tests/ui_resilience.rs`
- Create: `crates/k10s-ui/tests/ui_snapshots.rs`
- Create: `crates/k10s-ui/tests/snapshots/*`
- Create: `crates/k10s-backend/benches/fake_scale.rs`
- Create: `crates/k10s-ui/benches/ui_capacity.rs`
- Create: `crates/k10s-server/tests/fake_capacity.rs`
- Modify: `README.md`

- [ ] **Step 1: Write failing tests** for loading, empty, filtered-empty, stale, forbidden, conflict, gone, unavailable GVK after context switch, disconnected logs, active-shell guards, status without color, focus order, and minimum-size non-overlap.
- [ ] **Step 2: Run focused tests** and capture expected missing-state failures.
- [ ] **Step 3: Implement remaining projections and stable snapshots** for the approved screen set. Add accessible names for icon-only controls and preserve independent scroll regions. Generate a deterministic 50,000-object/1,000-node fake dataset, prove bounded snapshot chunking and stable memory, and record a non-regression frame-time baseline for 10,000 visible/filterable rows; Plan 5 repeats this against real runtime pressure.
- [ ] **Step 4: Run the Plan 2 gate:** `cargo fmt --all -- --check && cargo clippy --locked --workspace --all-targets -- -D warnings && cargo test --locked --workspace && cargo check --locked -p k10s-web --target wasm32-unknown-unknown && cargo bench --locked -p k10s-backend --bench fake_scale -- --test && cargo bench --locked -p k10s-ui --bench ui_capacity -- --test`. The UI benchmark renders/filter-scrolls the 50,000-object model at fixed viewport/density and fails the recorded frame-time/allocation ceiling.
- [ ] **Step 5: Commit:** `test: verify connected ui prototype`.

## Plan 2 verification gate

- Every approved UI workflow exists on native and web.
- Every data-bearing screen obtains state through protocol requests/subscriptions.
- Fake data is inaccessible to UI code.
- Logs/Exec use dedicated real sockets; operations use real control events.
- All loading/error/stale/guard states are testable deterministically.
- AccessKit and visual snapshots cover the approved screen set and minimum window size.
- The deterministic 50,000-object fake capacity and UI frame baseline pass before real Kubernetes work starts.
