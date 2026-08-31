# Free Window Resizing Option Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a persisted, default-off View-menu toggle that switches every workspace window between the existing minimum-size policy and unrestricted bidirectional resizing.

**Architecture:** `WorkspaceState` owns the global boolean and mutates it through `WorkspaceCommand`. The top bar emits that command, the window renderer selects normal or free sizing from the authoritative state, and snapshot version 3 persists the value while v1/v2 migrate to `false`.

**Tech Stack:** Rust, egui/eframe 0.36, serde/serde_json, egui_kittest, Cargo tests.

---

## File Structure

- `crates/k10s-ui/src/workspace/mod.rs`: own the live preference, accessor, toggle command, and command dispatch.
- `crates/k10s-ui/src/workspace/snapshot.rs`: serialize snapshot v3 and migrate v1/v2 with free resizing disabled.
- `crates/k10s-ui/src/ui/top_bar.rs`: render the checked View-menu item and return a toggle action.
- `crates/k10s-ui/src/ui/mod.rs`: pass workspace state into the top bar/window canvas and queue the toggle command.
- `crates/k10s-ui/src/ui/window.rs`: define the two sizing policies and apply the selected policy to all windows.
- `crates/k10s-ui/tests/workspace_state.rs`: test the default and command transition.
- `crates/k10s-ui/tests/workspace_snapshot.rs`: test v3 persistence, v1/v2 migration, and malformed v3 rejection.
- `crates/k10s-ui/tests/ui_shell.rs`: test the checked menu item and command path.
- `crates/k10s-ui/tests/ui_resource_windows.rs`: test normal minima, free compact geometry, and undersized-canvas precedence.
- `crates/k10s-ui/tests/snapshots/*.txt`: update only intentional accessibility-tree changes.
- `apps/k10s-desktop/src/lib.rs`: update persistence fixtures/assertions for snapshot v3 where compilation or expectations require it.

### Task 0: Record the Dirty-Tree Baseline

**Files:**
- Inspect only: entire worktree

- [ ] **Step 1: Record the implementation base and dirty paths**

Run:

```bash
git rev-parse HEAD
git status --short
git diff -- crates/k10s-ui/src/ui/window.rs crates/k10s-ui/tests/ui_resource_windows.rs crates/k10s-ui/tests/snapshots
```

Save the commit id as `IMPLEMENTATION_BASE`. Confirm that `ui/window.rs`, `ui_resource_windows.rs`, and the regenerated snapshot files are the approved free-resize experiment from the preceding work. Treat every other dirty path as user-owned and out of scope.

- [ ] **Step 2: Establish commit safety rules**

For every task, use `git add -p` for a file that was dirty at the baseline. Never stage the entire snapshots directory. Before every commit run `git diff --cached --check` and inspect `git diff --cached`; unstage any unrelated hunk without altering the working copy.

### Task 1: Workspace Preference and Command

**Files:**
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Test: `crates/k10s-ui/tests/workspace_state.rs`

- [ ] **Step 1: Write failing workspace-state tests**

Add a focused test that asserts `WorkspaceState::new().free_window_resizing()` is false, applies `WorkspaceCommand::ToggleFreeWindowResizing`, asserts true, applies it again, and asserts false. Capture window ids/content before toggling and assert they are unchanged.

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cargo test -p k10s-ui --test workspace_state free_window_resizing -- --nocapture`

Expected: compilation failure because the accessor and command variant do not exist.

- [ ] **Step 3: Implement the minimal live state**

Add `free_window_resizing: bool` to `WorkspaceState`, initialize it to `false` in every constructor/restore literal, expose:

```rust
#[must_use]
pub fn free_window_resizing(&self) -> bool {
    self.free_window_resizing
}
```

Add `WorkspaceCommand::ToggleFreeWindowResizing` and dispatch it by negating the field and returning no events. It is a non-navigation command and must not affect windows or backend state.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `cargo test -p k10s-ui --test workspace_state free_window_resizing -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit the workspace state change**

```bash
git add crates/k10s-ui/src/workspace/mod.rs crates/k10s-ui/tests/workspace_state.rs
git diff --cached --check
git diff --cached
git commit -m "feat(ui): add free resize workspace preference"
```

### Task 2: Snapshot Version 3 Persistence and Migration

**Files:**
- Modify: `crates/k10s-ui/src/workspace/snapshot.rs`
- Modify: `crates/k10s-ui/tests/workspace_snapshot.rs`
- Modify if required: `apps/k10s-desktop/src/lib.rs`

- [ ] **Step 1: Write failing persistence and migration tests**

Add tests asserting:

- a current snapshot explicitly serializes `"free_window_resizing": false`;
- toggling the live preference before snapshotting round-trips as true;
- a v1 payload decodes to version 3, `migrated_from == Some(1)`, and false while preserving representative geometry and migrated view fields;
- a v2 payload decodes to version 3, `migrated_from == Some(2)`, and false while preserving representative geometry and view fields;
- a v3 payload missing `free_window_resizing` is rejected;
- `state_store_round_trips_free_resize_true` flushes a toggled-true snapshot and reloads true;
- `migrated_v1_state_is_rewritten_as_v3_after_debounce` and `migrated_v2_state_is_rewritten_as_v3_after_debounce` assert version 3, explicit false, and representative geometry/view preservation;
- the existing `clean_exit_persists_the_final_layout` test is extended to toggle the preference and assert the final flush writes true.

- [ ] **Step 2: Run the persistence tests and verify RED**

Run these independently so the expected first failure cannot skip desktop RED verification:

```bash
cargo test -p k10s-ui --test workspace_snapshot
cargo test -p k10s-desktop --lib
```

Expected: the new focused assertions fail because version 3 and the serialized field are absent; the desktop command must report a nonzero test count.

- [ ] **Step 3: Implement the versioned schema**

Bump `SNAPSHOT_VERSION` to 3. Add the boolean to `WorkspaceSnapshot` and a strict `V3Snapshot`. Decode version 1 and version 2 through their existing schema types, set the normalized boolean to false, and return migration provenance `Some(1)`/`Some(2)`. Decode version 3 strictly with the required boolean and `migrated_from: None`. Include the live preference in `WorkspaceState::snapshot()` and restore it in `from_snapshot()`.

Do not add `#[serde(default)]` to the v3 field: missing current-schema state must fail closed. Keep window/view migration and counter validation unchanged.

- [ ] **Step 4: Run the persistence tests and verify GREEN**

Run: `cargo test -p k10s-ui --test workspace_snapshot && cargo test -p k10s-desktop --lib`

Expected: PASS.

- [ ] **Step 5: Commit snapshot persistence**

```bash
git add crates/k10s-ui/src/workspace/snapshot.rs crates/k10s-ui/tests/workspace_snapshot.rs apps/k10s-desktop/src/lib.rs
git diff --cached --check
git diff --cached
git commit -m "feat(ui): persist free resize preference"
```

Only stage `apps/k10s-desktop/src/lib.rs` if it changed.

### Task 3: View Menu Toggle

**Files:**
- Modify: `crates/k10s-ui/src/ui/top_bar.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`

- [ ] **Step 1: Write a failing menu interaction test**

Extend the View-menu test or add a focused test that opens View, finds the checkable `Free window resizing` item, verifies it is initially unchecked, clicks it, advances the harness, asserts `workspace().free_window_resizing()` is true, reopens View, and verifies the item is checked.

- [ ] **Step 2: Run the UI test and verify RED**

Run: `cargo test -p k10s-ui --test ui_shell free_window_resizing -- --nocapture`

Expected: failure because the menu item is absent.

- [ ] **Step 3: Implement the top-bar action path**

Add `toggle_free_window_resizing: bool` to `TopBarAction`. Pass the authoritative preference into `top_bar::show`, render:

```rust
if ui
    .checkbox(&mut free_window_resizing, "Free window resizing")
    .clicked()
{
    toggle_free_window_resizing = true;
    ui.close();
}
```

Treat the local mutable value only as widget presentation state; queue `WorkspaceCommand::ToggleFreeWindowResizing` in `UiShell` when the returned action is true. Continue applying queued commands after rendering through the existing command loop.

- [ ] **Step 4: Run the UI test and verify GREEN**

Run: `cargo test -p k10s-ui --test ui_shell free_window_resizing -- --nocapture`

Expected: PASS.

- [ ] **Step 5: Commit the View-menu behavior**

```bash
git add crates/k10s-ui/src/ui/top_bar.rs crates/k10s-ui/src/ui/mod.rs crates/k10s-ui/tests/ui_shell.rs
git diff --cached --check
git diff --cached
git commit -m "feat(ui): toggle free resizing from View menu"
```

### Task 4: Conditional Window Sizing Policy

**Files:**
- Modify: `crates/k10s-ui/src/ui/window.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/tests/ui_resource_windows.rs`
- Modify: `crates/k10s-ui/tests/ui_resilience.rs`
- Modify: `crates/k10s-ui/tests/snapshots/*.txt`

- [ ] **Step 1: Write failing renderer tests for both policies**

Replace the unconditional compact-size regression with these exact test names and explicit policy cases:

- `normal_mode_enforces_workload_minimum`: requested 240 by 160 workload geometry renders at least 640 by 420;
- `free_mode_preserves_compact_workload_geometry`: the same requested geometry renders below 300 by 220;
- `normal_and_free_modes_apply_overview_size_policy`: Overview renders at least 480 by 320 in normal mode and becomes compact in free mode;
- `normal_and_free_modes_apply_detail_size_policy`: a dedicated Detail renders at least 640 by 420 in normal mode and becomes compact in free mode;
- `window_size_policy_handles_an_undersized_canvas`: normal mode preserves the class minimum while free mode fits the compact request.

Add an undersized-harness case confirming normal mode preserves the kind minimum even when larger than the canvas, while free mode fits the compact request. Retain the existing split-pane resilience coverage.

- [ ] **Step 2: Run the focused renderer tests and verify RED**

Run these independently so the expected first failure cannot skip resilience coverage:

```bash
cargo test -p k10s-ui --test ui_resource_windows
cargo test -p k10s-ui --test ui_resilience minimum_size -- --nocapture
```

Expected: normal-mode assertion fails because the current experimental renderer always uses zero minimum and window scrolling.

- [ ] **Step 3: Implement named conditional policies**

In `ui/window.rs`, define named constants for `[640.0, 420.0]` and `[480.0, 320.0]`. Pass `workspace.free_window_resizing()` through `show_canvas` to `show_window`.

For normal mode, apply the kind-specific `.min_size(...)`, do not enable the outer window scroll area, and restore the content `ui.set_min_size(min_size - Vec2::new(24.0, 48.0))`. For free mode, apply `.min_size(Vec2::ZERO)`, enable bidirectional window scrolling, and do not set the content minimum. Keep `.resizable(true)` and `.constrain_to(canvas)` in both branches.

Construct the `egui::Window` in a local variable before `.show(...)` so conditional builder calls remain readable and do not duplicate the body renderer.

- [ ] **Step 4: Run focused renderer tests and verify GREEN**

Run:

```bash
cargo test -p k10s-ui --test ui_resource_windows
cargo test -p k10s-ui --test ui_resilience minimum_size -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Regenerate and inspect accessibility snapshots**

Run: `K10S_UPDATE_SNAPSHOTS=1 cargo test -p k10s-ui --test ui_snapshots`

Inspect: `git diff -- crates/k10s-ui/tests/snapshots`

Expected: default-off snapshots return to normal-mode hierarchy, plus any intentional View-menu checkbox representation. Reject changes that remove labels, roles, or actions.

- [ ] **Step 6: Commit the renderer policy and snapshots**

```bash
git add -p crates/k10s-ui/src/ui/window.rs crates/k10s-ui/tests/ui_resource_windows.rs
git add crates/k10s-ui/src/ui/mod.rs
git add -p crates/k10s-ui/tests/ui_resilience.rs
git add -p crates/k10s-ui/tests/snapshots/*.txt
git diff --cached --check
git diff --cached
git commit -m "feat(ui): apply optional free window resizing"
```

Only stage `ui_resilience.rs` if it changed. Default-off regeneration should remove the experiment-only outer scroll-container drift from normal snapshots; stage only remaining intentional menu or policy changes and leave unrelated baseline drift untouched.

### Task 5: Full Verification and Desktop Smoke Test

**Files:**
- Verify all modified files

- [ ] **Step 1: Run formatting and complete test suites**

Run:

```bash
cargo fmt --check
cargo test -p k10s-ui
cargo test -p k10s-desktop
git diff --check
```

Expected: all commands exit 0 with no failing tests or whitespace errors.

- [ ] **Step 2: Review the final diff against the specification**

Run:

```bash
git status --short
git diff "$IMPLEMENTATION_BASE" --stat
git diff "$IMPLEMENTATION_BASE"
```

Compare final `git status --short` with the Task 0 baseline: preserve known user-owned or untracked paths, and investigate any newly introduced residue. Review the full diff, including tests and snapshots, rather than relying on the stat summary.

Confirm default off, exact menu label, immediate global application, normal minima, free compact resizing, v1/v2 migration, required v3 field, and no unrelated changes.

- [ ] **Step 3: Restart the desktop app for manual validation**

Stop the currently running `cargo run -p k10s-desktop` session by sending Ctrl-C and wait for its exit. Then run `cargo run -p k10s-desktop` in a new long-running terminal session.

The automated fresh-workspace test proves the initial unchecked state without touching the user's real state file. Manually verify the current persisted value is represented accurately; normal windows stop at their minima; enabling the item permits shrinking in both dimensions; disabling expands undersized windows to normal minima; and both true and false values survive a close/restart cycle.

- [ ] **Step 4: Record any final mechanical fix in one commit**

If verification required a correction, commit only that correction with a focused message. Otherwise do not create an empty commit.
