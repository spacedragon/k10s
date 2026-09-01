# Detail Shared Frame and Pod Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship PR A with the frozen shared Detail frame/protocol API and the redesigned Pod Detail window matching `docs/designs/09-detail-redesign.html`.

**Architecture:** Add backward-compatible typed Pod, Deployment, and ReplicaSet wire slots plus explicit restart authority, then freeze the shared frame API before building the Pod-specific presentation and body. The frame owns fixed chrome and exactly one bounded vertical body scroll area; Pod rendering consumes a typed presentation input assembled at the UI boundary.

**Tech Stack:** Rust, egui/eframe, serde protocol types, kube/k8s-openapi normalization, Cargo tests, Clippy, WASM.

---

## File structure and ownership

- Modify `crates/k10s-protocol/src/resource.rs`: capability and projection wire contracts.
- Modify `crates/k10s-protocol/src/metrics.rs`: named container metrics samples.
- Modify `crates/k10s-backend/src/port.rs`: internal projection equivalents.
- Modify `crates/k10s-backend/src/kernel.rs`: wire mapping and capabilities.
- Modify `crates/k10s-backend/src/kube/normalize.rs`: Pod normalization only.
- Modify `crates/k10s-backend/src/kube/metrics.rs`: preserve container samples.
- Create `crates/k10s-ui/src/ui/detail/frame.rs`: fixed chrome, layout, scroll ownership, shared controls.
- Create `crates/k10s-ui/src/ui/detail/presentation.rs`: frozen input and shared projections.
- Create `crates/k10s-ui/src/ui/detail/pod.rs`: frozen compiling stub before the
  shared freeze; Task 5 replaces only its internals with Pod rendering.
- Create `crates/k10s-ui/src/ui/detail/deployment.rs`: frozen Deployment stub
  exposing the final renderer signature and unavailable fallback; PR B replaces
  only its internals.
- Modify `crates/k10s-ui/src/ui/detail/mod.rs`: thin kind router and frozen shared API.
- Modify `crates/k10s-ui/src/workspace/mod.rs` and `crates/k10s-ui/src/workspace/window.rs`: identity title and transient per-window expansion state.
- Modify `crates/k10s-ui/src/ui/taskbar.rs`: consistent pinned Detail label.
- Modify focused protocol/backend/UI tests listed below.
- Create `crates/k10s-ui/tests/ui_pod_details.rs`: Pod-only tests and baselines;
  after the freeze, Pod work does not touch shared test files.

### Task 1: Freeze backward-compatible protocol slots

**Files:**
- Modify: `crates/k10s-protocol/src/resource.rs`
- Modify: `crates/k10s-protocol/src/metrics.rs`
- Modify: `crates/k10s-protocol/tests/resource_contract.rs`
- Modify: `crates/k10s-protocol/tests/port_forward_contract.rs`

- [ ] Add failing serde round-trip tests for `can_restart`, Pod/Deployment/ReplicaSet projections, and named container metrics; assert old JSON omitting new fields decodes with safe defaults.
- [ ] Run `cargo test -p k10s-protocol --test resource_contract --test port_forward_contract`; expect missing fields/variants failures.
- [ ] Add `#[serde(default)] pub can_restart: bool`, typed projection structs, enum variants, and `ContainerMetrics { name, metrics }` with a defaulted vector on `ResourceMetricsResponse`.
- [ ] Re-run the two tests; expect PASS.
- [ ] Commit with `feat(protocol): add detail projection contracts`.

### Task 2: Freeze backend projection and capability mapping

**Files:**
- Modify: `crates/k10s-backend/src/port.rs`
- Modify: `crates/k10s-backend/src/kernel.rs`
- Test: `crates/k10s-backend/tests/resource_normalization.rs`
- Test: `crates/k10s-backend/tests/resource_details.rs`

- [ ] Add failing tests proving all new internal variants map exhaustively and only supported workload GVKs advertise restart.
- [ ] Run `cargo test -p k10s-backend --test resource_normalization --test resource_details`; expect failures.
- [ ] Add internal Pod/Deployment/ReplicaSet projection types, wire mapping arms, and explicit `can_restart` assignment beside existing workload capability rules.
- [ ] Re-run focused tests; expect PASS.
- [ ] Commit with `feat(backend): map typed detail projections`.

### Task 3: Preserve authoritative Pod and container data

**Files:**
- Modify: `crates/k10s-backend/src/kube/normalize.rs`
- Modify: `crates/k10s-backend/src/kube/metrics.rs`
- Test: `crates/k10s-backend/tests/resource_normalization.rs`
- Test: `crates/k10s-backend/tests/resource_details.rs`

- [ ] Add fixtures/tests for healthy and CrashLoopBackOff Pods covering phase, readiness, restarts, waiting/terminated reason, exit code, conditions, placement, network, metadata, and absent fields.
- [ ] Add metrics tests that preserve exact container names and reject/invalidate unavailable samples.
- [ ] Run the two focused backend tests; expect projection assertions to fail.
- [ ] Normalize only Kubernetes metadata/spec/status and metrics API fields into the typed contracts; never parse display summaries.
- [ ] Re-run focused tests; expect PASS.
- [ ] Commit with `feat(backend): normalize pod detail data`.

### Task 4: Build and freeze the shared frame API

**Files:**
- Create: `crates/k10s-ui/src/ui/detail/frame.rs`
- Create: `crates/k10s-ui/src/ui/detail/presentation.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Modify: `crates/k10s-ui/src/workspace/window.rs`
- Modify: `crates/k10s-ui/src/ui/taskbar.rs`
- Test: `crates/k10s-ui/tests/ui_details.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`

- [ ] Add failing frame/layout tests for fixed title/vital/tab/body/footer order, one body scroll ID per window/tab, no duplicate action row, and stable Pod/Deployment window plus taskbar identity through loading/stale/failed/gone states; run `cargo test --locked -p k10s-ui --test ui_details frame_`; expect failures.
- [ ] Implement finite rect allocation and the single vertical scroll owner; rerun the focused `frame_` tests; expect PASS.
- [ ] Add failing command-authority tests for Copy name, exact overflow contents, explicit mutation buttons, disabled stale authority, and focus-safe shortcuts; run the focused `detail_commands_` tests; expect failures.
- [ ] Implement shared command composition; rerun `detail_commands_`; expect PASS.
- [ ] Add failing compatibility tests for Service Ports and generic Overview Status/Age/freshness; run `cargo test --locked -p k10s-ui --test ui_services` plus focused `generic_detail_`; expect failures.
- [ ] Implement generic vitals and preserve specialized bodies; rerun compatibility tests; expect PASS.
- [ ] Add failing accessibility/state tests for stable labels and per-window collapsed expansions; run focused `detail_accessibility_`; expect failures.
- [ ] Implement transient expansion state and labels; rerun the accessibility tests; expect PASS.
- [ ] Introduce `DetailPresentationInput` containing pinned identity, primary detail, exact metrics, relation state, and freshness/gone/mutation authority.
- [ ] Register both Pod and Deployment renderer extension points in `detail/mod.rs` before the freeze; create compiling Pod and Deployment stubs with their final signatures that show `Structured details unavailable` until their internals are filled.
- [ ] Run `cargo test --locked -p k10s-ui --test ui_details --test ui_services`; expect PASS.
- [ ] Commit with `feat(ui): freeze shared detail frame API`; record this commit SHA as `DETAIL_FRAME_FREEZE` for PR B.

### Task 5: Implement Pod presentation and Overview test-first

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/pod.rs`
- Create: `crates/k10s-ui/tests/ui_pod_details.rs`

- [ ] Add failing pure projection tests for healthy/failing/missing data, exact metrics mismatch, owner verification, deterministic labels, and event limitations; run `cargo test --locked -p k10s-ui --test ui_pod_details projection_`; expect failures.
- [ ] Implement pure `PodDetailProjection::from_input` with `—` for every unavailable field and no manifest/summary parsing.
- [ ] Re-run `projection_`; expect PASS.
- [ ] Add failing responsive tests at widths 759/760 for ordering, collapsed metadata, and exact vital overflow; run focused `pod_layout_`; expect failures.
- [ ] Render vitals and wide/narrow section layout; rerun `pod_layout_`; expect PASS.
- [ ] Add failing interaction/accessibility tests for tabs, footers, owner navigation, labels, and loading/failed/gone states; run focused `pod_interaction_`; expect failures.
- [ ] Implement the remaining containers, conditions, events, owner, placement, network, labels, annotations, identity, and exact container metrics UI.
- [ ] Keep Logs/Shell/YAML/Events stores and stream lifecycle unchanged; missing typed projection shows `Structured details unavailable` only in Overview.
- [ ] Run `cargo test --locked -p k10s-ui --test ui_pod_details`; expect PASS.
- [ ] Confirm `git diff "$DETAIL_FRAME_FREEZE" -- crates/k10s-ui/src/ui/detail/mod.rs crates/k10s-ui/src/ui/detail/frame.rs crates/k10s-ui/src/ui/detail/presentation.rs crates/k10s-ui/tests/ui_details.rs crates/k10s-ui/tests/ui_services.rs` is empty.
- [ ] Commit with `feat(ui): redesign pod detail overview`.

### Task 6: Verify PR A and prepare the branch

**Files:**
- Modify only Pod-owned files/tests if verification exposes a defect. Any shared
  frame/API/shared-test correction requires a new `DETAIL_FRAME_FREEZE` commit,
  explicit PR B coordination, and rebase before either branch continues.

- [ ] Run `cargo fmt --all -- --check`; expect PASS.
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`; expect PASS.
- [ ] Run `cargo test -p k10s-protocol -p k10s-backend -p k10s-ui`; expect PASS.
- [ ] Run the repository's existing WASM/browser check commands discovered from CI; expect PASS.
- [ ] Launch the native desktop app and visually verify Pod, Service, and generic Detail at wide/narrow/minimum sizes against `docs/designs/09-detail-redesign.html`.
- [ ] Commit any verification-only fixes atomically, push PR A, and ensure `DETAIL_FRAME_FREEZE` is reachable on its branch before PR B rebases.
