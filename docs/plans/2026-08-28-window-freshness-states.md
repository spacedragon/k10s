# Per-window Freshness States Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Give every resource-list window an independent, accessible lifecycle state without allowing one unhealthy subscription to obscure or disable healthy siblings.

**Architecture:** Add a `WindowFreshness` projection keyed by `WindowId` to `ResourceFeed`, populated from each subscription's lifecycle. Render the projection through one shared status component in workload and Services windows, and pass its mutation guard only to the owning integrated detail pane.

**Tech Stack:** Rust, egui/accesskit, egui_kittest snapshots.

---

### Task 1: Define and render the state contract

**Files:**
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`

1. Add deterministic live, stale/retrying, forbidden, failed, and ready-empty projections.
2. Render icon/shape plus text, last-sync/retry detail, copyable authorization guidance, and keyboard buttons.
3. Give authoritative empty and filtered-empty states distinct recovery actions.
4. Run `cargo test -p k10s-ui --test ui_resilience` and confirm the new assertions pass.

### Task 2: Wire independent runtime lifecycle and mutation guards

**Files:**
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`

1. Project connection/retry and subscription errors per window without removing cached rows.
2. Route retry/full-resync actions through the existing client recovery paths.
3. Disable only the owning stale window's mutation controls and expose the reason.
4. Add focused application tests proving a failed subscription leaves a healthy sibling live.

### Task 3: Deterministic fixtures, snapshots, and delivery

**Files:**
- Modify: `crates/k10s-ui/tests/ui_resilience.rs`
- Modify: `crates/k10s-ui/tests/ui_snapshots.rs`
- Create: `crates/k10s-ui/tests/snapshots/window_freshness_states.txt`

1. Render all lifecycle states side by side with fixed ages and retry values.
2. Verify accessible labels, enabled/disabled mutation controls, and recovery actions.
3. Run formatting, focused tests, and the full repository-prescribed test suite.
4. Capture the implementation, commit screenshots, open a closing PR, resolve CI/review findings, and merge only after all required checks succeed.
