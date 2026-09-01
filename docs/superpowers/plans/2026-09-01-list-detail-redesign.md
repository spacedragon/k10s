# Shared List + Detail Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply `10-list-detail-redesign.html` across every resource list and integrated Detail view while preserving existing resource workflows and safety guards.

**Architecture:** Introduce a declarative responsive-table layer and refine the existing shared Detail frame instead of duplicating widgets per resource. Resource-specific modules provide columns, vitals, Overview content, and capability-driven actions; workspace commands remain the sole owner of guarded selection transitions.

**Tech Stack:** Rust, egui/eframe, AccessKit, k10s protocol projections, `egui_kittest`, Cargo tests, WASM.

---

## File structure and ownership

- Create `crates/k10s-ui/src/ui/responsive_table.rs`: column specifications, deterministic visibility calculation, middle-elision helper, shared selected-row interaction.
- Modify `crates/k10s-ui/src/ui/resource_table.rs`: migrate generic, Deployment, and Pod lists to responsive table specifications.
- Modify `crates/k10s-ui/src/ui/service_window.rs`: migrate Service list, remove legacy Detail visibility controls and full-width clear row.
- Modify `crates/k10s-ui/src/ui/resource_window.rs`: queue explicit select-versus-guarded-clear outcomes and render selection-derived integrated Detail.
- Modify `crates/k10s-ui/src/ui/split.rs`: collapse the Detail pane from selection state while preserving split/maximize behavior.
- Modify `crates/k10s-ui/src/ui/detail/frame.rs`: identity-row close control, chip styling, overflow popover, fixed chrome, and per-tab scroll ownership.
- Modify `crates/k10s-ui/src/ui/detail/presentation.rs`: deterministic vital priority and shared freshness/action projection.
- Modify `crates/k10s-ui/src/ui/detail/{deployment,pod,service,overview}.rs`: resource-specific Overview ordering and long-value treatment.
- Modify `crates/k10s-ui/src/workspace/{mod,resource,service,snapshot}.rs`: guarded toggle-off command path and legacy snapshot normalization.
- Extend focused UI/workspace tests; update only snapshots affected by intentional UI changes.
- Add `docs/designs/10-list-detail-redesign.html` as the visual reference.

### Task 1: Import the reference and characterize current behavior

**Files:**
- Create: `docs/designs/10-list-detail-redesign.html`
- Modify: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Modify: `crates/k10s-ui/tests/ui_services.rs`
- Modify: `crates/k10s-ui/tests/workspace_state.rs`

- [ ] **Step 1: Import the approved HTML byte-for-byte**

Use `apply_patch` to add the contents of `/Users/draco/Downloads/10-list-detail-redesign.html` at `docs/designs/10-list-detail-redesign.html`.

- [ ] **Step 2: Add failing selection contract tests**

Add `selected_row_click_queues_guarded_clear` and `detail_close_is_in_identity_row` to `ui_resource_windows`, plus `service_selected_row_click_queues_guarded_clear` and `service_selection_derives_detail_visibility` to `ui_services`. The close test must assert placement inside the identity row, not merely find the existing full-width button with the same label. Keep `blocked_toggle_preserves_selection` in `workspace_state`, where it directly exercises guarded `ClearSelection` and asserts selection/highlight source state remains intact when blocked or deferred.

- [ ] **Step 3: Run focused tests and confirm the intended failures**

Run `cargo test -p k10s-ui --test workspace_state blocked_toggle_preserves_selection`, `cargo test -p k10s-ui --test ui_resource_windows selected_row_click_queues_guarded_clear`, and `cargo test -p k10s-ui --test ui_services service_selected_row_click_queues_guarded_clear`.

Expected: FAIL because selected rows currently re-select, the close control is a full-width row, and Service still gates Detail on `detail_visible`.

- [ ] **Step 4: Commit the reference and red tests**

```bash
git add docs/designs/10-list-detail-redesign.html crates/k10s-ui/tests
git commit -m "test(ui): define shared list detail redesign contracts"
```

### Task 2: Implement guarded toggle-off and selection-derived Detail

**Files:**
- Modify: `crates/k10s-ui/src/ui/resource_table.rs`
- Create: `crates/k10s-ui/src/ui/responsive_table.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Modify: `crates/k10s-ui/src/ui/detail/frame.rs`
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Modify: `crates/k10s-ui/src/workspace/snapshot.rs`
- Test: `crates/k10s-ui/tests/workspace_state.rs`
- Test: `crates/k10s-ui/tests/workspace_snapshot.rs`
- Test: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`

- [ ] **Step 1: Make table outcomes explicit**

Create the shared interaction type below in `responsive_table.rs`, then replace the ambiguous `selected: Option<I>` outcome in both `resource_table.rs` and the independent table renderer inside `service_window.rs`:

```rust
pub(super) enum RowAction<I> {
    Select(I),
    ClearSelection,
}
```

The table receives the current identity; a click on the same identity emits `ClearSelection`, otherwise `Select`.

- [ ] **Step 2: Route both clear gestures through the guard**

Translate `RowAction::ClearSelection` and the new Detail identity-row `Clear selection` icon button into `WorkspaceCommand::ClearSelection(window_id)`. Task 2 owns creation of this close control and removal of the old rows; Task 4 may only restyle/regression-test it. Do not mutate `selection` or `detail` directly from UI code.

- [ ] **Step 3: Remove legacy visibility UI**

Remove Service `Hide details` / `Show details` and both full-width `Clear selection` render paths. Derive integrated Detail visibility from `state.detail.is_some()` for every list window.

- [ ] **Step 4: Normalize snapshot compatibility**

Continue deserializing `detail_visible`, but set `detail_visible = true` in both `from_resource` / `from_service` serialization and `into_resource` / `into_service` restoration. Update assertions that currently restore false. Preserve `split_ratio`; do not change the snapshot version. Keep `ToggleDetailPane` and the state field as compatibility-only for this schema version, but remove every runtime UI caller and make the command unable to hide a selected integrated Detail.

- [ ] **Step 5: Run focused state and UI tests**

Run `cargo test -p k10s-ui --test workspace_state --test workspace_snapshot --test ui_resource_windows --test ui_services`. Expected: PASS, including a guard-blocked toggle retaining selection and Detail state.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src crates/k10s-ui/tests
git commit -m "feat(ui): unify guarded detail selection clearing"
```

### Task 3: Build deterministic responsive list primitives

**Files:**
- Modify: `crates/k10s-ui/src/ui/responsive_table.rs`
- Modify: `crates/k10s-ui/src/ui/resource_table.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Test: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`

- [ ] **Step 1: Add failing pure layout tests**

Add private `#[cfg(test)]` unit tests beside the pure resolver in `responsive_table.rs`. Test the exact 1000- and 640-point fixtures from the design spec for Deployment, Pod, Service, and generic tables. Assert hide order, minimum Name/Namespace widths, reverse-order restoration, and the last-resort horizontal-scroll flag. Integration tests remain responsible for rendered headers, alignment, and tooltips.

- [ ] **Step 2: Add failing middle-elision tests**

Cover `ghcr.io/containers/kubernetes-mcp:v0.3.1` and assert the compact form retains `:v0.3.1`, while the helper returns the original value for tooltip/copy use.

- [ ] **Step 3: Run focused tests and confirm failure**

Run `cargo test -p k10s-ui responsive_table::tests`, `cargo test -p k10s-ui --test ui_resource_windows responsive_`, and `cargo test -p k10s-ui --test ui_services responsive_`.

Expected: FAIL because table specifications and elision helpers do not exist.

- [ ] **Step 4: Implement pure column resolution**

Add focused types equivalent to `ColumnSpec { key, min_width, elastic, hide_priority }` and `ResolvedColumns { visible, horizontal_scroll }`. Keep calculation independent from egui painting so it is deterministic under unit tests.

- [ ] **Step 5: Migrate generic/workload and Service tables**

Render only resolved columns, right-align numeric cells, retain Namespace/Name, use one stable horizontal `ScrollArea` only for the fallback case, and keep existing sort keys and row virtualization.

Generic/workload Namespace, Name, Status, and Age retain their existing sort mappings (`created` backs Age). Service retains sorting for Name, Namespace, Type, Cluster IP, Ports, and Age with its existing typed comparators; add regression assertions for every Service sort header. Only genuinely new workload headers—Ready, Image, Restarts, and Node—are non-sortable in this scope; their widgets expose no sort affordance and queue no unsupported sort key.

- [ ] **Step 6: Run focused tests**

Run these separately so the unit-test filter does not suppress integration cases:

```bash
cargo test -p k10s-ui responsive_table::tests
cargo test -p k10s-ui --test ui_resource_windows responsive_
cargo test -p k10s-ui --test ui_services responsive_
```

Expected: PASS with all columns at 1000 points and adapter-specific hiding at 640 points.

- [ ] **Step 7: Commit**

```bash
git add crates/k10s-ui/src/ui crates/k10s-ui/tests
git commit -m "feat(ui): add responsive resource list columns"
```

### Task 4: Refine the shared Detail frame

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/frame.rs`
- Modify: `crates/k10s-ui/src/ui/detail/presentation.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Modify: `crates/k10s-ui/src/ui/split.rs`
- Test: `crates/k10s-ui/tests/ui_details.rs`
- Test: `crates/k10s-ui/tests/ui_infrastructure.rs`

- [ ] **Step 1: Add failing frame tests**

At 1000 and 640 content points assert identity controls, a one-line vital strip, exact preserved-vital sets, `Show more` as a popover, fixed tabs/footer, and no full-width clear row. Add Esc tests proving focused editors/modals consume Esc first and an otherwise-unconsumed Esc queues guarded clear; dirty YAML and live Shell blockers must preserve selection and its selected-row highlight.

- [ ] **Step 2: Add per-tab scroll-owner tests**

Assert Overview and Events have the shared vertical owner, while Logs, Shell, YAML, and Ports render their existing viewport without a wrapping Detail `ScrollArea`. Also cover true empty and filtered-empty lists, no-selection height reclamation and hidden splitter, split-ratio restoration on reselection, dedicated Detail lifecycle with no integrated close, and Pop out plus Maximize/Restore regressions.

- [ ] **Step 3: Run focused tests and confirm failures**

Run `cargo test -p k10s-ui --test ui_details` and `cargo test -p k10s-ui --test ui_infrastructure detail_`.

Expected: FAIL where the current expansion adds inline vitals and every tab is wrapped by the same vertical scroller.

- [ ] **Step 4: Implement chip and overflow presentation**

Style each vital as a bounded chip with label/value hierarchy and tone/shape. Compute visible/overflow sets from resource priority and available width. Render overflow in a vertical menu/popover without increasing strip height.

- [ ] **Step 5: Move freshness into the identity row**

Render live/loading/reconnecting/stale/forbidden/failed/gone freshness beside identity and remove freshness from `show_vital_strip`. Test that it appears exactly once for every state and is never projected as a resource vital.

- [ ] **Step 6: Split body viewport ownership by tab**

Keep one finite body rect and fixed footer. Wrap Overview/Events in the shared vertical `ScrollArea`; pass Logs/Shell/YAML/Ports an unwrapped bounded child UI so their existing viewports remain the only scroll owner.

- [ ] **Step 7: Restyle close and implement Esc behavior**

Keep Task 2's accessible `Clear selection` in the integrated identity row and apply final compact styling. Queue guarded clear after higher-priority modal/editor Escape handling; dedicated Detail windows keep their existing close lifecycle.

- [ ] **Step 8: Run focused tests**

Run both targets above. Expected: PASS at wide, narrow, and minimum-height fixtures.

- [ ] **Step 9: Commit**

```bash
git add crates/k10s-ui/src/ui crates/k10s-ui/tests
git commit -m "feat(ui): refine shared detail frame"
```

### Task 5: Migrate resource-specific Detail content

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/deployment.rs`
- Modify: `crates/k10s-ui/src/ui/detail/pod.rs`
- Modify: `crates/k10s-ui/src/ui/detail/service.rs`
- Modify: `crates/k10s-ui/src/ui/detail/overview.rs`
- Modify: `crates/k10s-ui/src/ui/detail/presentation.rs`
- Test: `crates/k10s-ui/tests/ui_deployment_details.rs`
- Test: `crates/k10s-ui/tests/ui_pod_details.rs`
- Test: `crates/k10s-ui/tests/ui_services.rs`
- Test: `crates/k10s-ui/tests/ui_details.rs`

- [ ] **Step 1: Add failing responsive Overview tests**

For every adapter, assert a `1.35 : 1` two-column Overview at 1000 points and operational/configuration/identity ordering at 640 points. Assert empty sections collapse into concise copy.

- [ ] **Step 2: Add failing long-value tests**

Assert Image, selector, and annotation values use full-width two-line KV rows, preserve image tags under middle elision, expose the full tooltip, and provide a copy control.

- [ ] **Step 3: Add authority-matrix tests**

Cover live, loading/reconnecting, stale/failed with cache, forbidden, and gone states. Assert action availability is the intersection of freshness, capability, and mutation authority; missing typed projection changes only Overview.

- [ ] **Step 4: Run adapter tests and confirm failures**

Run `cargo test -p k10s-ui --test ui_deployment_details --test ui_pod_details --test ui_services --test ui_details`.

- [ ] **Step 5: Implement Deployment and Pod adapters**

Keep the existing typed projections. Reorder wide/narrow sections, collapse empty rollout/event blocks, and apply long-value components without reading manifest/summary display strings.

- [ ] **Step 6: Implement Service and generic adapters**

Preserve Ports and port-forward behavior. Provide Status/Age vitals and retain the existing generic body when structured projections are unavailable.

- [ ] **Step 7: Run all adapter tests**

Run `cargo test -p k10s-ui --test ui_deployment_details --test ui_pod_details --test ui_services --test ui_details`. Expected: PASS without regressions to YAML, Events, Logs, Shell, or port forwarding.

- [ ] **Step 8: Commit**

```bash
git add crates/k10s-ui/src/ui crates/k10s-ui/tests
git commit -m "feat(ui): migrate resource details to shared responsive layout"
```

### Task 6: Update snapshots and verify end to end

**Files:**
- Modify: `crates/k10s-ui/tests/snapshots/*.txt` only where the approved UI intentionally changes.
- Create: `crates/k10s-ui/tests/fixtures/list-detail-visual.yaml`
- Modify: `docs/screenshots/` only if the repository's visual-validation workflow produces new approved evidence.

- [ ] **Step 1: Run snapshot tests without automatic acceptance**

Run `cargo test -p k10s-ui --test ui_snapshots`. Expected: failures only for intentional List/Detail hierarchy, selected-row, and frame changes.

- [ ] **Step 2: Review and accept intentional snapshots**

Inspect every diff against `docs/designs/10-list-detail-redesign.html`; reject unrelated text, state, or action changes. Update only approved snapshot files.

- [ ] **Step 3: Run focused workflow regressions**

Run `cargo test -p k10s-ui --test yaml_workflow --test stream_tools --test port_forward_state --test operation_dialogs`. Expected: PASS.

- [ ] **Step 4: Run repository verification**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test -p k10s-ui
cargo check -p k10s-web --target wasm32-unknown-unknown
```

Expected: PASS.

- [ ] **Step 5: Create deterministic real-kind visual resources**

Add one namespaced fixture manifest containing namespace `k10s-list-detail-visual`, an `nginx:1.27.4` Deployment, its ClusterIP Service, and a ConfigMap used as the generic resource. Apply it to an explicit disposable context and wait for readiness:

```bash
kubectl --context kind-bunyip apply -f crates/k10s-ui/tests/fixtures/list-detail-visual.yaml
kubectl --context kind-bunyip -n k10s-list-detail-visual rollout status deployment/list-detail-demo --timeout=120s
kubectl --context kind-bunyip -n k10s-list-detail-visual get deployments,pods,services,configmaps
```

The manifest is idempotent and contains no Secret or external dependency beyond the pinned public image.

- [ ] **Step 6: Perform exact-width and native visual verification**

First add/run `egui_kittest` render cases backed by deterministic in-memory `ResourceListRow` and Detail projections for Deployment, Pod, Service, and ConfigMap. Set the harness content rect directly to 1000×700 and 640×700 egui points and save review PNGs; these tests are the reproducible authority for exact content widths.

Then launch `cargo run -p k10s-desktop` against `kind-bunyip`, open the four fixture resource kinds, and manually resize once above and once below the 760-point breakpoint (the visible two-column/single-column transition confirms the class; exact widths are already enforced by the harness). Compare both states with the HTML reference and deterministic PNGs. Verify middle-elided image tags, responsive columns, one-line vitals, popover overflow, fixed footer, and all three guarded clear gestures. Remove the disposable resources afterward with `kubectl --context kind-bunyip delete namespace k10s-list-detail-visual` and report that cleanup in the handoff.

- [ ] **Step 7: Commit verification artifacts**

```bash
git add crates/k10s-ui/tests docs/screenshots
git commit -m "test(ui): verify shared list detail redesign"
```
