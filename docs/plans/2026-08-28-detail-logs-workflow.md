# Detail Pane, Logs, Maximize, and Pop-out Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bring the integrated resource detail workflow, pod logs, keyboard controls, maximize/restore behavior, and dedicated pop-out window in line with issue #156.

**Architecture:** Extend the existing command-driven workspace state with reversible detail maximization and keep all rendering in the shared egui UI. Enrich the existing per-window log viewer state and toolbar without changing the stream protocol, deriving container/status defaults from authoritative detail/list data. Exercise behavior through pure workspace tests and egui accessibility-tree tests.

**Tech Stack:** Rust, egui/eframe, k10s workspace state machine, egui_kittest, Cargo.

---

### Task 1: Reversible pane maximization

**Files:**
- Modify: `crates/k10s-ui/src/workspace/resource.rs`
- Modify: `crates/k10s-ui/src/workspace/service.rs`
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Test: `crates/k10s-ui/tests/workspace_state.rs`

1. Add non-persisted prior-split state and maximize/restore commands.
2. Write state tests proving maximize remembers the split and restore returns to it.
3. Implement the minimal command handling and run the focused workspace tests.

### Task 2: Detail identity header and pane behavior

**Files:**
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Modify: `crates/k10s-ui/src/ui/resource_window.rs`
- Modify: `crates/k10s-ui/src/ui/service_window.rs`
- Test: `crates/k10s-ui/tests/ui_details.rs`
- Test: `crates/k10s-ui/tests/ui_resource_windows.rs`

1. Add kind/name, namespace/context, freshness, pop-out, maximize/restore affordances.
2. Add Enter and modified-Enter row behavior and documented `l/p/s/y/e` plus hierarchical Escape handling.
3. Auto-maximize Logs/Shell below the usable minimum while retaining explicit restore.
4. Verify stable header/tool layout for loading, empty, disconnected, and error content.

### Task 3: Complete logs toolbar and CrashLoopBackOff default

**Files:**
- Modify: `crates/k10s-ui/src/ui/tools/logs.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Test: `crates/k10s-ui/tests/stream_tools.rs`
- Test: `crates/k10s-ui/tests/ui_details.rs`

1. Add container selection, previous-log toggle, since-window choices, wrap, find, and export controls to stable viewer state.
2. Detect the fake CrashLoopBackOff row and default its log view to previous terminated-container logs with explanatory copy.
3. Test state transitions and accessibility labels.

### Task 4: Verification, screenshots, and delivery

**Files:**
- Update: `crates/k10s-ui/tests/snapshots/*.txt` only where intentional
- Add: `docs/screenshots/issue-156-*.png`

1. Run formatting, focused tests, full workspace tests, Clippy, WASM check, and browser smoke as applicable.
2. Launch the fake app and capture clear integrated/maximized logs and dedicated pop-out screenshots.
3. Fetch and rebase onto `origin/main`, rerun affected checks, commit, push, and open a non-draft PR closing #156 with embedded screenshots.
4. Monitor CI and AgentConnect review, address every finding, and merge only after all required checks succeed.
