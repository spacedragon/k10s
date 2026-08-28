# Resource Launcher Inventory Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete issue #168 with a searchable, keyboard-safe launcher whose complete taxonomy and inventory badges expose honest loading, zero, warning, and unavailable states.

**Architecture:** Keep the existing Kubernetes protocol and client seams: the backend continues to populate `InfrastructureResponse::launcher`, while the shared egui shell combines that response with `InfrastructureLoad` for presentation. Launcher rows remain workspace commands, so browser and desktop hosts use the same real state machine without fake-only actions.

**Tech Stack:** Rust 1.97.1, egui/eframe, kube-rs backend, Playwright Chromium, kind-bunyip.

---

### Task 1: Lock the launcher state contract with tests

**Files:**
- Modify: `crates/k10s-ui/tests/ui_shell.rs`
- Modify: `tests/browser/layout.spec.ts`

**Steps:**
1. Add deterministic shell tests for loading, zero, warning, and unavailable badge labels.
2. Add focus/collapse/filter assertions, including clearing a filter without mutating the saved collapsed state.
3. Run the focused tests and confirm they fail against the old launcher behavior.

### Task 2: Implement the shared launcher presentation

**Files:**
- Modify: `crates/k10s-ui/src/ui/launcher.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`

**Steps:**
1. Pass the existing `InfrastructureLoad` state into the launcher.
2. Centralize inventory badge rendering for loading, numeric zero/count, warning, and unavailable labels.
3. Keep filtering as a transient reveal so collapsed state and keyboard focus survive matching and clearing.
4. Run focused UI tests and update deterministic accessibility snapshots only when intentional.

### Task 3: Verify fixtures, real inventory, and visual evidence

**Files:**
- Create: `docs/screenshots/issue-168/before-1280x800.png`
- Create: `docs/screenshots/issue-168/after-1280x800.png`
- Modify: browser snapshots under `tests/browser/layout.spec.ts-snapshots/` if needed

**Steps:**
1. Capture the current branch at a fixed 1280x800 viewport before implementation.
2. Verify backend deterministic inventory tests and launcher UI/browser behavior.
3. Read the existing kind-bunyip API inventory and run only safe read-path validation.
4. Capture and inspect the fixed-viewport after image.

### Task 4: Quality gates and delivery

**Files:**
- Modify only files required by failures attributable to this issue.

**Steps:**
1. Run formatting, Clippy, relevant unit/UI/browser suites, WASM check, and desktop smoke.
2. Commit and push `spacedragon/issue-168`.
3. Open a non-draft PR with `Closes #168`, implementation/test evidence, and embedded before/after images.
4. Monitor required checks, CI review, review threads, requested changes, and mergeability; fix and push as needed.
5. Rebase on current `origin/main` if required, then squash merge only when all gates are green and no review remains unresolved.
