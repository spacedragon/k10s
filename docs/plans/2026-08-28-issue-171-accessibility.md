# Issue 171 Accessibility Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make destructive review and every per-window resilience state compact, explicit, keyboard-safe, and accessible.

**Architecture:** Extend the existing per-window presentation projection without changing protocol/client seams, render all states through the shared resource-window component, and reorganize the existing authoritative delete dialog. Deterministic egui fixtures provide AccessKit and pixel evidence at fixed viewports.

**Tech Stack:** Rust, egui/accesskit, egui_kittest, Playwright, Kubernetes kind smoke.

---

### Task 1: Lock the accessibility contracts with failing tests

**Files:**
- Modify: `crates/k10s-ui/tests/ui_resilience.rs`
- Modify: `crates/k10s-ui/tests/operation_dialogs.rs`
- Modify: `crates/k10s-ui/tests/ui_snapshots.rs`

1. Add assertions for live, stale, reconnecting, forbidden, failed, empty, filtered-empty, and unavailable recovery controls.
2. Add keyboard tests proving Enter outside the confirmation input cannot submit.
3. Add compact and standard fixed-viewport snapshot cases for states and confirmation.
4. Run focused tests and confirm the new expectations fail before implementation.

### Task 2: Implement the shared resilient-state presentation

**Files:**
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/theme.rs`

1. Extend `WindowFreshness` with explicit reconnecting and filtered-empty semantics plus recovery availability.
2. Render a shared high-contrast status card using iconography, text, guidance, and accessibly disabled recovery buttons.
3. Preserve per-window mutation guards and route enabled recovery through existing `ResourceAction` values.
4. Run resilience, service, application, and theme tests.

### Task 3: Implement compact ordered destructive review

**Files:**
- Modify: `crates/k10s-ui/src/ui/dialogs.rs`
- Modify: `crates/k10s-ui/tests/operation_dialogs.rs`

1. Render scope, impact, dry run, typed confirmation, and exact command as numbered compact sections.
2. Show every unmet precondition beside the disabled submit and preserve authoritative preflight invalidation.
3. Scope Enter submission to focused confirmation input while retaining click submission and one-shot draining.
4. Run operation-dialog and client seam tests.

### Task 4: Generate and inspect deterministic evidence

**Files:**
- Add: `crates/k10s-ui/tests/snapshots/issue_171_*.txt`
- Add: `docs/screenshots/issue-171/*.png`

1. Generate before images from the base revision at fixed compact and standard viewports.
2. Generate after images from the final implementation using the same fixtures and viewports.
3. Inspect every PNG for clipping, contrast, focus/disabled clarity, and state coverage.
4. Regenerate and verify AccessKit snapshots in normal non-update mode.

### Task 5: Full verification and delivery

**Files:**
- Modify as required by test or review findings.

1. Run formatting, Clippy, focused/unit/workspace tests, UI snapshots, Playwright browser tests, WASM build/check, and desktop smoke.
2. Safely read from `kind-bunyip` using the existing live smoke when applicable; do not mutate the shared cluster.
3. Commit, push `spacedragon/issue-171`, and create a non-draft PR with `Closes #171`, implementation/test evidence, and embedded before/after screenshots.
4. Monitor required checks, automated code review, comments, requested changes, and mergeability; resolve each item and rebase when necessary.
5. Squash merge only after all required checks and reviews pass with no unresolved threads, then report the PR URL, merge commit, and verification results.
