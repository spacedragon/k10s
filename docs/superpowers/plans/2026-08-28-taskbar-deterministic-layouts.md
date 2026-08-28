# Taskbar and Deterministic Layouts Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Finish the instance-addressable taskbar, keyboard window controls, reversible deterministic layouts, and context-scoped persistence required by issue #155.

**Architecture:** Keep `WorkspaceState` as the sole window registry and put deterministic geometry transitions there. Render a fixed taskbar after the canvas and translate taskbar/keyboard input into existing or new workspace commands. Persist a versioned map of snapshots keyed by Kubernetes context in the native host, migrating the prior single-snapshot file.

**Tech Stack:** Rust, egui/eframe, serde JSON, egui_kittest, Cargo tests.

---

### Task 1: Deterministic registry layouts

**Files:**
- Modify: `crates/k10s-ui/src/workspace/window.rs`
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Test: `crates/k10s-ui/tests/workspace_state.rs`

1. Add failing tests for tile minima/overflow, reversible cascade, reversible focus, MRU cycling, and indexed activation.
2. Add pure geometry helpers and registry commands with one reversible layout checkpoint.
3. Run `cargo test -p k10s-ui --test workspace_state` and expect PASS.

### Task 2: Taskbar and keyboard controls

**Files:**
- Create: `crates/k10s-ui/src/ui/taskbar.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/window.rs`
- Test: `crates/k10s-ui/tests/ui_snapshots.rs`
- Create: `crates/k10s-ui/tests/snapshots/taskbar_layouts.txt`

1. Add a failing accessibility snapshot covering duplicate kinds, namespace/pinned identity, status text, active state, and compact overflow.
2. Render a bottom taskbar with Tile/Cascade/Focus controls and compact keyboard-reachable overflow.
3. Route `Alt+1..9`, `Ctrl+Tab`, and `Ctrl+W` to registry commands.
4. Regenerate the intentional snapshot and run the UI tests.

### Task 3: Context-scoped persistence

**Files:**
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `apps/k10s-desktop/src/lib.rs`
- Test: `apps/k10s-desktop/src/lib.rs`

1. Add failing tests that two contexts retain independent snapshots and that legacy state migrates.
2. Save/restore a versioned context-keyed snapshot collection and swap snapshots after a committed context switch.
3. Run `cargo test -p k10s-desktop` and expect PASS.

### Task 4: Verify and deliver

**Files:**
- Modify only snapshots or formatting touched by the implementation.

1. Run `cargo fmt --all -- --check`, focused tests, and the relevant workspace tests.
2. Launch the deterministic UI fixture, capture clear screenshots, inspect them, and commit them under `docs/screenshots/issue-155/`.
3. Fetch and rebase onto `origin/main`, repeat verification, commit, push, and open a non-draft PR closing #155 with embedded screenshots.
4. Monitor CI and AgentConnect review; fix every finding and rerun checks until all required checks pass.
5. Merge the PR only after no checks are pending or failing.
