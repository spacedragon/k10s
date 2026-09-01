# Deployment Detail Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship stacked PR B with the redesigned Deployment Detail using PR A's frozen frame and wire APIs without modifying shared chrome.

**Architecture:** Fork from PR A's recorded `DETAIL_FRAME_FREEZE`, populate the already-defined Deployment and ReplicaSet projections, then replace only the frozen Deployment stub's internals using `DetailPresentationInput`. The shared router and frame remain untouched.

**Tech Stack:** Rust, egui/eframe, kube/k8s-openapi normalization, typed k10s protocol projections, Cargo tests, Clippy, WASM.

---

## File structure and ownership

- Modify `crates/k10s-backend/src/kube/normalize.rs` or create focused `crates/k10s-backend/src/kube/deployment_projection.rs`: Deployment/ReplicaSet normalization only.
- Modify `crates/k10s-ui/src/ui/detail/deployment.rs`: replace the frozen stub's
  internals without changing its public signature.
- Create `crates/k10s-ui/tests/ui_deployment_details.rs`: exclusive Deployment
  tests and uniquely named baselines; never edit shared UI test/snapshot files.
- The frozen router and compiling stub already own the Deployment extension
  point; integration modifies only the stub internals and not `detail/mod.rs`.
- Do not modify `frame.rs`, `presentation.rs`, workspace title/state, protocol enums, metrics, or shared tests; report API defects to PR A.

### Task 1: Create the stacked branch at the frozen API

- [ ] Verify `git show "$DETAIL_FRAME_FREEZE" --stat` contains protocol slots and shared frame/input API.
- [ ] Create PR B's branch from that exact commit and configure its initial base as PR A's branch.
- [ ] Run `cargo test --locked -p k10s-protocol` and then `cargo test --locked -p k10s-ui --test ui_details`; expect both frozen baselines to pass.

### Task 2: Normalize Deployment and ReplicaSet projections

**Files:**
- Create: `crates/k10s-backend/src/kube/deployment_projection.rs`
- Modify: `crates/k10s-backend/src/kube/normalize.rs`
- Test: `crates/k10s-backend/tests/resource_normalization.rs`
- Test: `crates/k10s-backend/tests/resource_details.rs`

- [ ] Add failing fixtures/tests for complete/progressing/failed Deployment status, strategy, replica counts, template, selector, metadata, and missing fields.
- [ ] Add failing ReplicaSet tests for authoritative revision annotation, replica/ready counts, image, and created time; rows without revision must remain valid but be omitted from history by the UI.
- [ ] Run the focused backend tests; expect missing projection failures.
- [ ] Populate only the pre-frozen internal/wire variants from Kubernetes metadata/spec/status.
- [ ] Re-run focused tests; expect PASS.
- [ ] Commit with `feat(backend): normalize deployment detail data`.

### Task 3: Implement pure Deployment presentation

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/deployment.rs`
- Create: `crates/k10s-ui/tests/ui_deployment_details.rs`

- [ ] Add failing pure projection tests for rollout vitals, condition failure, typed ReplicaSet history, template/management/labels/identity, and `—`; run focused `deployment_projection_`; expect failures.
- [ ] Implement `DeploymentDetailProjection::from_input` using only the frozen typed input; never parse manifest, summary, or generic detail rows.
- [ ] Re-run `deployment_projection_`; expect PASS.
- [ ] Add failing relation tests for loading, failed, stale, retry, and exact identity; run focused `deployment_relations_`; expect failures.
- [ ] Implement related Pod/history state projection; rerun `deployment_relations_`; expect PASS.
- [ ] Commit with `feat(ui): project deployment detail data`.

### Task 4: Render Deployment Overview and actions

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/deployment.rs`
- Test: `crates/k10s-ui/tests/ui_deployment_details.rs`
- Test: `crates/k10s-ui/tests/ui_deployment_details.rs` (uniquely named baselines)

- [ ] Add a passing characterization test for `Deployment · namespace / name` in window and taskbar through loading/stale/failed/gone states; this verifies PR A's frozen contract before Deployment body work continues.
- [ ] Add failing wide/narrow/minimum-height layout tests asserting fixed chrome/footer and one vertical owner; run focused `deployment_layout_`; expect failures.
- [ ] Render Pods, read-only history, events, template, management, labels, annotations, and identity; rerun `deployment_layout_`; expect PASS.
- [ ] Add failing command-authority tests for Scale, explicit Restart, Delete, Copy, overflow, shortcuts, and no rollback/delete shortcut; run focused `deployment_commands_`; expect failures.
- [ ] Compose actions through frozen helpers; rerun `deployment_commands_`; expect PASS.
- [ ] Add failing accessibility tests for text-plus-shape state and expand/collapse labels; run focused `deployment_accessibility_`; expect failures.
- [ ] Implement accessibility details and run the entire `ui_deployment_details` target; expect PASS.
- [ ] Commit with `feat(ui): redesign deployment detail overview`.

### Task 5: Verify frozen-boundary integration

- [ ] Run the frozen router test proving exact Deployment GVK selects the extension while other controllers stay generic; expect PASS after the module is present.
- [ ] Confirm `git diff "$DETAIL_FRAME_FREEZE" -- crates/k10s-ui/src/ui/detail/mod.rs crates/k10s-ui/src/ui/detail/frame.rs crates/k10s-ui/src/ui/detail/presentation.rs crates/k10s-ui/tests/ui_details.rs crates/k10s-ui/tests/ui_snapshots.rs` is empty.
- [ ] If either check fails, stop and return the API defect to PR A; do not work around it.

### Task 6: Verify and publish stacked PR B

- [ ] Rebase onto the latest PR A branch only after confirming the freeze API did not change; if it changed, stop and coordinate a new freeze SHA.
- [ ] Run `cargo fmt --all -- --check`; expect PASS.
- [ ] Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`; expect PASS.
- [ ] Run `cargo test -p k10s-protocol -p k10s-backend -p k10s-ui`; expect PASS.
- [ ] Run existing WASM/browser CI commands; expect PASS.
- [ ] Launch native desktop and visually verify wide/narrow Deployment Detail against `docs/designs/09-detail-redesign.html`.
- [ ] Push PR B targeting PR A; after PR A merges, retarget/rebase PR B onto `main` and rerun all checks before merge.
