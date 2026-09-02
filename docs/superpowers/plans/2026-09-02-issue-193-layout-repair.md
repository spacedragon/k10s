# Issue #193 Integrated Layout Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the actual browser-integrated Deployment List + Detail view fit the approved 1000x700 wide and 640x700 narrow contracts without clipped controls.

**Architecture:** Preserve saved and manually resized window geometry. Derive first-open default geometry from the canvas, make toolbar/detail control visibility depend on each region's local width, and replace invisible horizontal scroll overflow with visible keyboard-accessible menus. Native geometry tests and browser screenshot snapshots cover both target viewports.

**Tech Stack:** Rust 2024, egui 0.36, egui_kittest, Playwright.

---

### Task 1: First-open wide Deployment geometry

**Files:**
- Modify: `crates/k10s-ui/src/ui/mod.rs:430-490`
- Modify: `crates/k10s-ui/src/ui/window.rs:1-120`
- Modify: `crates/k10s-ui/src/workspace/mod.rs:650-670`
- Test: `crates/k10s-ui/tests/ui_resource_windows.rs`

- [ ] **Step 1: Write the failing geometry test**

Add a test that opens its first Deployment window in a 1000x700 canvas, selects a row, and asserts its Detail body has at least 760 points; add a companion assertion that a persisted/manual geometry remains unchanged.

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test -p k10s-ui --test ui_resource_windows first_deployment_window_uses_wide_detail_geometry -- --exact`

Expected: FAIL because the current default window leaves the Detail body below 760 points.

- [ ] **Step 3: Implement the minimum geometry policy**

Add a UI-layer first-render geometry helper, where canvas bounds are available,
then apply its result only to first-created Deployment windows before egui
persists a rect. Keep `WindowGeom` serialization and existing persisted/manual
values unchanged; do not pass canvas dimensions into pure workspace commands.

- [ ] **Step 4: Run the focused test and commit**

Run: `cargo test -p k10s-ui --test ui_resource_windows first_deployment_window_uses_wide_detail_geometry -- --exact`

Commit: `git commit -am "fix(ui): size first deployment window for wide detail"`

### Task 2: Local-width toolbar and match-line overflow

**Files:**
- Modify: `crates/k10s-ui/src/ui/resource_window.rs:460-828`
- Test: `crates/k10s-ui/src/app.rs:9300-9520`

- [ ] **Step 1: Write failing render tests**

At narrow local list width, assert Search/Namespace/Status/Live are contained, `More list controls` is keyboard-queryable, and its menu exposes Columns, Refresh, sorting and age mode.

- [ ] **Step 2: Run tests to verify RED**

Run: `cargo test -p k10s-ui deployment_toolbar -- --nocapture`

Expected: FAIL because compactness is based on `ctx().content_rect()` and controls paint beyond the pane.

- [ ] **Step 3: Implement local layout budgeting**

Replace the global-width test with a local `ui.available_width()` budget. Render the primary controls first; put Columns/Refresh and match-line secondary controls behind a `More list controls` menu when their widths do not fit.

- [ ] **Step 4: Run focused tests and commit**

Run: `cargo test -p k10s-ui deployment_toolbar -- --nocapture`

Commit: `git commit -am "fix(ui): keep list controls within local width"`

### Task 3: Visible Detail vital/tab/action overflow

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/frame.rs:218-325`
- Test: `crates/k10s-ui/tests/ui_details.rs`

- [ ] **Step 1: Write failing narrow-frame tests**

Assert narrow frames preserve Rollout/Ready plus the existing `Show more`
vitals affordance, contain the active tab plus a visible `More detail tabs`
control, contain Scale/Delete plus `More detail actions`, and do not create
`AlwaysHidden` horizontal scroll areas.

- [ ] **Step 2: Run tests to verify RED**

Run: `cargo test -p k10s-ui --test ui_details compact_frame -- --nocapture`

Expected: FAIL because tabs/actions are currently clipped in hidden horizontal scroll areas.

- [ ] **Step 3: Implement menu overflow**

Replace every hidden horizontal scroll area in the vital, tab, and action rows
with width-budgeted primary controls and accessible menus/popovers. Preserve all
command dispatch semantics, active-tab label, Escape/outside close behaviour,
and reference action ordering when space permits.

- [ ] **Step 4: Run focused tests and commit**

Run: `cargo test -p k10s-ui --test ui_details compact_frame -- --nocapture`

Commit: `git commit -am "fix(ui): expose narrow detail overflow controls"`

### Task 4: Integrated native and browser visual regression coverage

**Files:**
- Modify: `crates/k10s-ui/tests/ui_deployment_details.rs:697-753`
- Modify: `tests/browser/issue193-visual.spec.ts`
- Create/modify: `tests/browser/__snapshots__/issue193-visual.spec.ts-snapshots/*`
- Modify: `docs/screenshots/issue-193/*`

- [ ] **Step 1: Write failing integrated geometry and image-snapshot tests**

Drive the real integrated Deployment selection path at 1000x700 and 640x700. Assert wide two columns/1.35 ratio vs narrow metadata disclosure and owning-rect containment. Replace write-only browser screenshots with `expect(page).toHaveScreenshot` baselines.

- [ ] **Step 2: Run tests to verify RED**

Run: `cargo test -p k10s-ui --test ui_deployment_details deployment_integrated_geometry -- --nocapture`

Expected: FAIL on the old layout because the integrated 1000x700 body is narrow and controls clip.

- [ ] **Step 3: Capture deterministic visual baselines**

Run the focused Playwright spec with `--update-snapshots`; retain documented before/after comparison assets separately from test baselines.

- [ ] **Step 4: Run focused suites and commit**

Run: `cargo test -p k10s-ui --test ui_deployment_details && pnpm exec playwright test tests/browser/issue193-visual.spec.ts`

Run: `git add tests/browser/__snapshots__ docs/screenshots/issue-193 tests/browser/issue193-visual.spec.ts crates/k10s-ui/tests/ui_deployment_details.rs && git diff --cached --check`

Commit: `git commit -m "test(ui): cover issue 193 integrated viewport layouts"`

### Task 5: Full verification and PR preparation

**Files:** no planned source changes.

- [ ] **Step 1: Format and lint**

Run: `cargo fmt --check && cargo clippy -p k10s-ui --all-targets -- -D warnings`

- [ ] **Step 2: Run complete validation**

Run: `cargo test -p k10s-ui && pnpm exec playwright test tests/browser/issue193-visual.spec.ts`

- [ ] **Step 3: Inspect final diff**

Run: `git diff main...HEAD --check && git status --short`

- [ ] **Step 4: Create PR**

Push `fix/issue-193-layout`, then create a PR that links #193 and lists the two target viewport test commands.
