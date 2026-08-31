# Keyboard-first Command Palette Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete issue #154 with a keyboard-first, accessible command palette that searches live Kubernetes projections and performs precise reuse-versus-new-window navigation.

**Architecture:** Add a focused palette module to the shared egui shell. It owns query/cursor state and pure ranking, renders grouped results from the existing `ResourceFeed` and context projections, and translates activation into existing workspace commands so navigation guards and z-order remain authoritative.

**Tech Stack:** Rust, egui 0.36, egui_kittest, existing k10s workspace model.

---

### Task 1: Pure palette search and ranking

**Files:**
- Create: `crates/k10s-ui/src/ui/command_palette.rs`

1. Write unit tests for exact/prefix/fuzzy ranking, `po`/`deploy`/`svc`/`ctx`/`ns` prefixes, resource metadata, and stable grouping.
2. Run `cargo test -p k10s-ui command_palette` and confirm the tests fail before implementation.
3. Implement the minimal palette state, candidates, ranking, and grouped result projection.
4. Re-run the focused tests and confirm they pass.

### Task 2: Keyboard and workspace activation behavior

**Files:**
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/top_bar.rs`
- Modify: `crates/k10s-ui/src/workspace/mod.rs`
- Test: `crates/k10s-ui/tests/ui_command_palette.rs`

1. Add failing egui tests for `:`/Ctrl+K ownership, editing conflicts, arrows/j/k, Enter/Escape, selection semantics, and modified Enter.
2. Add workspace helpers/commands only where needed to focus/reuse a matching target.
3. Render the modal palette with grouped results, metadata, active-row state, and keyboard footer.
4. Apply plain and modified activations through workspace commands and verify focused tests pass.

### Task 3: Resource actions and fake demonstration

**Files:**
- Modify: `crates/k10s-ui/src/ui/command_palette.rs`
- Modify: `crates/k10s-backend/src/fake.rs` only if the existing fake projection lacks the Design 08 CrashLoopBackOff row.
- Test: `crates/k10s-ui/tests/ui_command_palette.rs`

1. Add tests proving CrashLoopBackOff/restart-count search and Detail/Logs/Previous logs actions are visible for a matching Pod.
2. Implement action activation through dedicated/integrated detail tabs without bypassing guards.
3. Verify fake-mode data and focused UI tests.

### Task 4: Full verification and delivery

**Files:**
- Add: implementation screenshots under `docs/screenshots/`

1. Run formatting, clippy, workspace tests, WASM check, Trunk build, and the relevant Playwright smoke.
2. Launch fake mode, capture and inspect clear palette screenshots, then commit them.
3. Fetch and rebase onto latest `origin/main`; repeat relevant verification.
4. Commit, push, open a non-draft PR closing #154, and embed the committed screenshots.
5. Monitor all required checks and AgentConnect review; fix every finding and repeat until green.
6. Merge only after every required check succeeds.
