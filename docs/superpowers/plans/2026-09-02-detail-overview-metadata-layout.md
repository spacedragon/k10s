# Detail Overview Metadata Layout Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Deployment Detail Overview sections match the approved dense layout without sparse metadata, weak section hierarchy, or undersized Pods text.

**Architecture:** Keep deployment-specific Pods layout in `detail/deployment.rs` and reusable metadata presentation primitives in `detail/overview.rs`. Drive every change through native `egui_kittest` geometry and accessibility assertions before updating snapshots.

**Tech Stack:** Rust, egui/eframe, egui_kittest/AccessKit, Cargo snapshots.

---

### Task 1: Restore dense Deployment Pods typography and section rules

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/deployment.rs`
- Modify: `crates/k10s-ui/src/ui/detail/overview.rs`
- Test: `crates/k10s-ui/tests/ui_deployment_details.rs`

- [ ] **Step 1: Write failing native layout tests**

Extend the wide and narrow deployment Overview tests to assert:

```rust
let pod_name = window.get_by_label("api-server-abc").rect();
assert!(pod_name.height() >= fixture.body_text_height);
assert!(!pod_name.intersects(window.get_by_label("1/1").rect()));
```

Capture `body_text_height` from the render closure with `ui.text_style_height(&egui::TextStyle::Body)`, rather than querying a kittest node for a context it does not expose.

Add a pure `configuration_section_sequence` seam and test the matrices: all sections present; Managed by absent; labels-only metadata; annotations-only metadata; and both metadata collections empty. Assert the sequence has no empty regions and therefore produces exactly `len - 1` separators. Test the separator helper directly in an `egui_kittest` unit harness by inspecting its returned `egui::Response::rect`, without inserting artificial accessibility nodes.

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
cargo test -p k10s-ui --test ui_deployment_details deployment_ -- --nocapture
cargo test -p k10s-ui detail_section_separator_spans_local_width
```

Expected: each command reports at least one executed test; FAIL on missing section geometry and/or Pods body-height/column geometry.

- [ ] **Step 3: Use the shared body row metric and resolved cell widths**

Calculate Pod rows with:

```rust
let row_height = ui
    .spacing()
    .interact_size
    .y
    .max(ui.text_style_height(&egui::TextStyle::Body));
```

Paint Pod cells with `TextStyle::Body`. Add a right-aligned resolved-cell path for Ready, Restarts, and Age. In the native test, query the matching `Role::TextRun` node (not the enclosing Label response, whose rectangle may cover the whole allocated cell) and assert its painted glyph rectangle is anchored to the resolved cell's right edge and does not intersect its neighbor. Retain horizontal scrolling whenever semantic minimum widths exceed the visible local width.

- [ ] **Step 4: Render section boundaries through one helper**

Add an `overview::section_separator(ui) -> egui::Response` helper that paints one full locally visible-width hairline and returns its geometry for direct unit testing. It must not add a synthetic accessibility node. Drive calls from the tested non-empty section sequence so there is exactly one separator between consecutive rendered regions, never before the first or after the last.

- [ ] **Step 5: Verify focused tests pass**

Run:

```bash
cargo test -p k10s-ui --test ui_deployment_details
cargo test -p k10s-ui detail_section_separator_spans_local_width
```

Expected: both commands report at least one executed test and PASS.

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src/ui/detail/deployment.rs crates/k10s-ui/src/ui/detail/overview.rs crates/k10s-ui/tests/ui_deployment_details.rs
git commit -m "fix(ui): restore detail overview section rhythm"
```

### Task 2: Wrap all label chips and collapse annotations by default

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/overview.rs`
- Modify: `crates/k10s-ui/src/ui/detail/deployment.rs`
- Modify: `crates/k10s-ui/src/ui/detail/pod.rs`
- Test: `crates/k10s-ui/tests/ui_deployment_details.rs`
- Test: `crates/k10s-ui/tests/ui_pod_details.rs`

- [ ] **Step 1: Write failing metadata behavior tests**

For both Deployment and Pod metadata, assert all label chips exist, `Show N more labels` does not exist, and constrained-width chip rectangles form at least two distinct `top()` rows without exceeding the metadata-column right edge. Assert zero metadata omits its region.

Assert `Annotations 3 ▾` starts collapsed with AccessKit expanded state false, click produces `Annotations 3 ▴` with expanded state true, long/unbroken key/value nodes stay within the local width and adjacent rows differ by at most Body line height plus standard spacing, then clicking again hides the rows.

- [ ] **Step 2: Run focused tests and verify RED**

Run:

```bash
cargo test -p k10s-ui --test ui_deployment_details metadata -- --nocapture
cargo test -p k10s-ui --test ui_pod_details metadata -- --nocapture
```

Expected: FAIL because labels are count-truncated and deployment annotations are eagerly expanded.

- [ ] **Step 3: Add shared metadata presentation helpers**

In `overview.rs`, add focused helpers:

```rust
pub(super) fn label_chips(ui: &mut egui::Ui, labels: &BTreeMap<String, String>) {
    ui.horizontal_wrapped(|ui| {
        for (key, value) in labels {
            metadata_chip(ui, key, value);
        }
    });
}
```

Add an annotation disclosure helper keyed by `WindowId` and resource identity. Use temporary egui state defaulting to false and a button labelled `Annotations N ▾/▴`. After assigning the stable button label, set its explicit AccessKit state with:

```rust
ui.ctx().accesskit_node_builder(response.id, |node| node.set_expanded(open));
```

Tests read `accesskit_node().is_expanded()`. Render compact two-column Body rows and elide long/unbroken visible strings while retaining full accessible labels and hover text.

- [ ] **Step 4: Replace Deployment and Pod duplicate metadata rendering**

Route both renderers through the shared label-chip and annotation-disclosure helpers. Remove fixed visible-label counts, `Show N more labels`, eager deployment annotation rows, and the old Pod `CollapsingHeader`. Preserve the existing outer narrow `Show/Hide {Kind} metadata` disclosure.

- [ ] **Step 5: Verify focused tests pass**

Run:

```bash
cargo test -p k10s-ui --test ui_deployment_details
cargo test -p k10s-ui --test ui_pod_details
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src/ui/detail/overview.rs crates/k10s-ui/src/ui/detail/deployment.rs crates/k10s-ui/src/ui/detail/pod.rs crates/k10s-ui/tests/ui_deployment_details.rs crates/k10s-ui/tests/ui_pod_details.rs
git commit -m "fix(ui): compact detail metadata presentation"
```

### Task 3: Refresh native baselines and verify the complete change

**Files:**
- Modify as generated: `crates/k10s-ui/tests/snapshots/deployment_detail_wide_overview.txt`
- Modify as generated: `crates/k10s-ui/tests/snapshots/deployment_detail_narrow_overview.txt`

- [ ] **Step 1: Run snapshots without update and inspect intentional differences**

Run: `cargo test -p k10s-ui --test ui_deployment_details`

Expected: only approved Detail Overview snapshot differences, if any.

- [ ] **Step 2: Refresh approved native snapshots**

Run:

```bash
K10S_UPDATE_DEPLOYMENT_SNAPSHOTS=1 cargo test -p k10s-ui --test ui_deployment_details
cargo test -p k10s-ui --test ui_deployment_details
```

Expected: the first command refreshes only the approved Deployment accessibility snapshots and the second passes without further changes.

- [ ] **Step 3: Run complete verification**

```bash
cargo fmt --check
cargo clippy -p k10s-ui --all-targets -- -D warnings
cargo test -p k10s-ui --tests
cargo test --workspace
git diff --check origin/main...HEAD
```

Expected: all commands pass with no warnings.

- [ ] **Step 4: Launch the real desktop environment**

Run: `cargo run -p k10s-desktop`

Expected: the app connects through the local kubeconfig and supports manual verification of a high-cardinality Deployment list and a real Deployment Detail Overview.

- [ ] **Step 5: Commit generated baselines**

```bash
git add crates/k10s-ui/tests/snapshots/deployment_detail_wide_overview.txt crates/k10s-ui/tests/snapshots/deployment_detail_narrow_overview.txt
git commit -m "test(ui): refresh detail overview baselines"
```
