# Shell Theme and Canvas Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Match the design-08 shell hierarchy and density across native and web while preserving accessibility and the 640x420 minimum viewport.

**Architecture:** Centralize the console palette, spacing, widget visuals, and shell frame helpers in `ui/theme.rs`. Keep the existing panel/window composition, adding only responsive top-bar layout, fixed shell proportions, canvas painting, and focused-window frame/title treatment. Verify geometry and semantic identity with egui tests and fixed Playwright screenshots.

**Tech Stack:** Rust, egui/eframe 0.36, egui_kittest, TypeScript, Playwright.

---

### Task 1: Theme tokens and shell geometry

**Files:**
- Modify: `crates/k10s-ui/src/ui/theme.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`

1. Add failing shell tests for token contrast, 196 px launcher geometry, 29 px taskbar, and non-overlapping top-bar controls at 640x420, 1280x800, and 1440x900.
2. Run `cargo test -p k10s-ui --test ui_shell` and confirm the new assertions fail.
3. Implement the tokenized dark theme, compact spacing, and exact shell panel sizes.
4. Run the shell tests and confirm they pass.

### Task 2: Responsive chrome and textured canvas

**Files:**
- Modify: `crates/k10s-ui/src/ui/top_bar.rs`
- Modify: `crates/k10s-ui/src/ui/launcher.rs`
- Modify: `crates/k10s-ui/src/ui/window.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`

1. Add assertions that focus has a textual semantic cue and remains synchronized with the raised window.
2. Implement compact responsive top-bar groups, launcher branding/hierarchy, dot-grid canvas depth, and focused border/title treatment without dimming inactive content.
3. Run `cargo test -p k10s-ui --test ui_shell` and the existing resilience suite.

### Task 3: Fixed native/browser visual coverage

**Files:**
- Modify: `tests/browser/layout.spec.ts`
- Modify: `tests/browser/layout.spec.ts-snapshots/compact-workspace-chromium-linux.png`
- Modify: `tests/browser/layout.spec.ts-snapshots/compact-workspace-chromium-darwin.png`
- Create: `tests/browser/layout.spec.ts-snapshots/shell-1280x800-chromium-linux.png`
- Create: `tests/browser/layout.spec.ts-snapshots/shell-1280x800-chromium-darwin.png`
- Create: `docs/screenshots/issue-152-shell-1280x800.png`

1. Extend Playwright coverage to fixed 640x420, 1280x800, and 1440x900 viewports and assert shell controls do not overlap.
2. Build the web frontend and update deterministic Chromium snapshots.
3. Capture the reviewed 1280x800 implementation image and commit it for the PR body.

### Task 4: Full verification and delivery

**Files:**
- Modify only files required by formatter or review findings.

1. Run `cargo fmt --all -- --check`, targeted UI tests, workspace tests, Clippy, WASM check, and Playwright layout tests.
2. Fetch `origin`, rebase onto latest `origin/main`, rerun relevant verification, commit, and push.
3. Open a non-draft PR with the committed screenshot embedded and `Closes #152`.
4. Monitor required CI and AgentConnect review; fix every finding, rerun checks, and push updates.
5. Merge only after every required check is complete and successful.
