# Overview Attention Scroll Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the Overview window bounded on large clusters by scrolling its Needs attention table inside the available window height.

**Architecture:** Preserve the existing Overview data flow and split rendering into fixed top content, a finite two-axis attention viewport, and a reserved metrics footer. Derive the attention height from the current window content budget; use an outer vertical scroll only when the existing 480×320 minimum cannot fit the fixed sections and a usable attention viewport.

**Tech Stack:** Rust, egui 0.36.1, egui_kittest 0.36.1, Cargo

---

## File Structure

- Modify `crates/k10s-ui/src/ui/overview.rs`: own Overview layout budgeting, two-axis attention scrolling, and compact fallback.
- Modify `crates/k10s-ui/tests/ui_infrastructure.rs`: exercise large real-cluster attention data, viewport bounds, scrolling, fixed content, and compact fallback.

No protocol, backend, workspace persistence, or other window modules change.

### Task 1: Bound and scroll the normal-size attention table

**Files:**
- Modify: `crates/k10s-ui/tests/ui_infrastructure.rs`
- Modify: `crates/k10s-ui/src/ui/overview.rs`

- [ ] **Step 1: Add a large-attention fixture helper**

Add a helper in `ui_infrastructure.rs` that replaces `response.attention` with at least 80 uniquely labeled rows, including a final row named `attention-row-079`. Keep the existing `full_response()` fixture unchanged for other tests.

```rust
fn fill_large_attention(response: &mut InfrastructureResponse) {
    response.attention = (0..80)
        .map(|index| AttentionRow {
            namespace: Some("prod".into()),
            kind: "Pod".into(),
            name: format!("attention-row-{index:03}"),
            status: "Failed".into(),
            reason: format!("synthetic failure {index:03}"),
        })
        .collect();
}
```

- [ ] **Step 2: Write the failing normal-viewport regression test**

Create `overview_attention_rows_scroll_inside_the_window` with a 1,280×800 harness. Before rendering, obtain `harness.state().shell.workspace().snapshot()`, change the Overview entry's public persisted geometry to position `[32.0, 24.0]` and size `[920.0, 620.0]`, then apply it through the supported `WorkspaceCommand::RestoreSnapshot(snapshot)` route. RestoreSnapshot advances the private layout revision and makes the renderer apply the explicit geometry without a test-only production API. Populate the large fixture, render several stabilization frames, and capture the Overview window rectangle plus the rectangles of the actual fixture summary label and `Workload health`.

Assert the Overview rectangle is contained by the shell canvas, `attention-row-079` is not initially visible, and the fixed labels are visible. Send repeated `scroll_down()` events from a visible node within the Needs attention panel, rerender after each event, then assert the late row becomes visible. Finally assert the Overview rectangle and fixed-label rectangles are unchanged within one logical pixel and the refresh/metrics labels remain queryable.

Target a visible descendant inside the attention scroll area—the `Name` heading or first data row, not the `Needs attention` title outside it. Use a bounded loop rather than a fixed scroll count:

Accessibility nodes borrow the current harness tree, so reacquire and drop them around every rerender:

```rust
for _ in 0..40 {
    if harness
        .get_by_role_and_label(Role::Window, "Overview")
        .query_by_label("attention-row-079")
        .is_some()
    {
        break;
    }
    {
        let overview = harness.get_by_role_and_label(Role::Window, "Overview");
        overview.get_by_label("attention-row-000").scroll_down();
    }
    harness.run();
}
let overview = harness.get_by_role_and_label(Role::Window, "Overview");
assert!(overview.query_by_label("attention-row-079").is_some());
```

- [ ] **Step 3: Run the focused test and verify the red state**

Run:

```bash
cargo test -p k10s-ui --test ui_infrastructure overview_attention_rows_scroll_inside_the_window -- --nocapture
```

Expected: FAIL because the current horizontal-only table expands vertically and the late row/window-bound assertions do not hold.

- [ ] **Step 4: Implement finite Overview layout budgeting**

In `overview.rs`, keep the fixed top sections unchanged. Use a finite child region matching the current Overview content rect. Measure/reserve the wrapping metrics footer in a bottom region, then subtract the fixed-top measured height and all inter-section spacing from the finite content height to obtain a total attention-panel budget.

Keep two distinct heights: `panel_height` is the complete finite framed panel; `inner_scroll_height` is what remains after subtracting the panel's top/bottom frame margins, the measured “Needs attention” heading and heading spacing, and scrollbar occupancy. Allocate `panel_height` first and pass only `inner_scroll_height` to `ScrollArea::max_height`. Nothing outside the inner scroll may be added after the total panel budget is allocated.

For this task's explicit 920×620 ordinary geometry, select the normal layout from that complete budget:

- fixed content + footer + gutters + 96 points must fit;
- use a 96-point minimum and let attention consume all remaining height;
- do not add compact branching or the outer fallback until Task 2's compact test has failed.

Pass the selected finite `panel_height` into `attention_panel`, calculate `inner_scroll_height` as described above, and use that inner value for the scroll widget. Frame, heading, spacing, and both scrollbar gutters count against `panel_height`; they must not increase the child region afterward.

Change `attention_panel` to accept `panel_height: f32`, retain its existing frame and empty state, derive `inner_scroll_height`, and use the existing stable ID with a two-axis area:

```rust
egui::ScrollArea::both()
    .id_salt("k10s.overview.attention.scroll")
    .max_height(inner_scroll_height)
    .auto_shrink([false, false])
    .show(ui, |ui| {
        // Existing striped grid and all rows, unchanged.
    });
```

Ensure the scroll area receives a finite width and height from the panel's available rect so horizontal and vertical scrollbar gutters remain inside the viewport. Do not truncate, paginate, reorder, or clone attention rows.

- [ ] **Step 5: Run the focused test and verify green**

Run the Step 3 command again.

Expected: PASS; the late row is reachable by scrolling and the Overview/fixed section rectangles remain stable.

- [ ] **Step 6: Run the existing Overview infrastructure test**

Run:

```bash
cargo test -p k10s-ui --test ui_infrastructure overview_renders_totals_capacity_health_attention_and_refresh_timestamp -- --nocapture
```

Expected: PASS with the existing labels, progress indicators, refresh action, and metrics footer intact.

- [ ] **Step 7: Commit the normal-size behavior**

```bash
git add crates/k10s-ui/src/ui/overview.rs crates/k10s-ui/tests/ui_infrastructure.rs
git commit -m "fix(ui): scroll large overview attention lists"
```

### Task 2: Keep compact Overview content reachable

**Files:**
- Modify: `crates/k10s-ui/tests/ui_infrastructure.rs`
- Modify: `crates/k10s-ui/src/ui/overview.rs`

- [ ] **Step 1: Write the failing compact-viewport test**

Create `compact_overview_keeps_large_attention_content_reachable` with a 720×420 shell harness, which leaves enough canvas width after the fixed launcher for the Overview window's 480-point minimum. Explicitly set the Overview geometry to position `[0.0, 0.0]` and size `[480.0, 320.0]` by modifying a public workspace snapshot and applying `WorkspaceCommand::RestoreSnapshot`, exactly as in Task 1. Use the same 80-row fixture.

Assert the Overview window remains inside the canvas and record its rectangle. Verify the top summary is initially visible while the metrics footer is clipped. Target a visible fixed-content descendant that belongs to the outer fallback, send scroll-down events in a bounded rerender loop, and verify `Needs attention` then the metrics footer become visible. When attention itself receives events, target `attention-row-000` and separately prove a late attention row becomes visible; do not treat inner-table movement as proof that the outer region moved. After each stage, assert the outer Overview rectangle is unchanged and previously clipped descendants changed viewport visibility.

- [ ] **Step 2: Run the compact test and verify the red state**

Run:

```bash
cargo test -p k10s-ui --test ui_infrastructure compact_overview_keeps_large_attention_content_reachable -- --nocapture
```

Expected: FAIL because no bounded compact fallback currently exists.

- [ ] **Step 3: Add the compact outer-scroll fallback**

In `overview.rs`, calculate whether the fixed top content, spacing, footer, and attention viewport fit the finite content height. Select the completed behavior from the approved budget:

- when fixed content, footer, framed attention chrome, scrollbar gutters, and a 96-point **inner interactive table viewport** fit, use Task 1's ordinary fixed-top layout and let attention consume the remaining finite height with a 96-point inner minimum;
- when 96 does not fit but the same non-table chrome plus a 48-point **inner interactive table viewport** does, use the same layout with the remaining finite height and a 48-point inner floor;
- otherwise wrap the complete Overview body in a bounded vertical `ScrollArea` tied to an Overview-specific compact ID.

Inside the outer compact fallback, do not derive the nested attention height from the outer scroll child's effectively unbounded `available_height()`. Allocate enough finite total panel height for its frame margins, heading/spacing, scrollbar gutters, and an explicit 48-point **inner interactive table viewport**. Pass only that 48-point inner value to the existing two-axis scroll area.

The fallback must use the current content rect as its maximum size and must never request a larger outer window. Keep the existing window minimum and geometry persistence unchanged.

- [ ] **Step 4: Run both new regression tests**

Run each valid single-filter command:

```bash
cargo test -p k10s-ui --test ui_infrastructure overview_attention_rows_scroll_inside_the_window -- --nocapture
cargo test -p k10s-ui --test ui_infrastructure compact_overview_keeps_large_attention_content_reachable -- --nocapture
```

Expected: both commands PASS.

- [ ] **Step 5: Run the complete infrastructure UI target**

Run:

```bash
cargo test -p k10s-ui --test ui_infrastructure -- --nocapture
```

Expected: all tests PASS with no warnings or snapshot regressions.

- [ ] **Step 6: Format and run the broader UI checks**

Run:

```bash
cargo fmt --check
cargo test -p k10s-ui --tests
cargo clippy -p k10s-ui --tests -- -D warnings
```

Expected: all commands exit 0 with no formatting diff, test failures, or Clippy warnings.

- [ ] **Step 7: Launch and visually verify representative desktop behavior**

Run:

```bash
cargo run -p k10s-desktop
```

Using a cluster or explicit fake fixture that produces enough attention rows, resize Overview at normal and compact sizes. Confirm that the normal layout keeps summary/health fixed while Needs attention scrolls vertically and horizontally, and that compact content remains reachable without the outer window leaving the canvas.

- [ ] **Step 8: Commit compact behavior and verification updates**

```bash
git add crates/k10s-ui/src/ui/overview.rs crates/k10s-ui/tests/ui_infrastructure.rs
git commit -m "fix(ui): keep compact overview content reachable"
```
