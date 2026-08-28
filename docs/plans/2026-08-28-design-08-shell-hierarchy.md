# Design 08 Shell Hierarchy Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Align the desktop shell top bar, multi-window depth, and instance taskbar with Design 08 at both supported fixed viewports.

**Architecture:** Keep `WorkspaceState` and the existing protocol/client seams unchanged. Move layout commands into the global top bar, project them as ordinary workspace commands, reserve the taskbar for addressable window instances, and express window focus with border, title treatment, and shadow depth. Use width-aware labels and controls so the same semantic actions remain keyboard reachable at 1280x800 and 640x420.

**Tech Stack:** Rust, egui/eframe, AccessKit via egui_kittest, TypeScript, Playwright.

---

### Task 1: Capture and specify the baseline

**Files:**
- Create: `docs/screenshots/issue-172/before-1280x800.png`
- Create: `docs/screenshots/issue-172/before-640x420.png`
- Modify: `tests/browser/layout.spec.ts`

1. Run the existing production-like browser fixture at 1280x800 and 640x420.
2. Save fixed-viewport baseline screenshots under the issue directory.
3. Add browser assertions for the global controls, task-instance distinction, and viewport fit.
4. Run the focused Playwright tests and confirm the new assertions fail before implementation.

### Task 2: Promote global window controls into the top bar

**Files:**
- Modify: `crates/k10s-ui/src/ui/top_bar.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/taskbar.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`
- Test: `crates/k10s-ui/tests/ui_resilience.rs`

1. Add a `Window` menu and first-class Tile, Cascade, and Focus actions to the top bar.
2. Render separate connection, version, and context controls with compact accessible labels.
3. Route top-bar actions to the existing `WorkspaceCommand` variants without protocol or client changes.
4. Remove layout commands from the taskbar so only task instances and overflow remain.
5. Add keyboard-navigation and compact-layout tests, then run the focused UI tests.

### Task 3: Strengthen multi-window hierarchy

**Files:**
- Modify: `crates/k10s-ui/src/ui/window.rs`
- Modify: `crates/k10s-ui/src/ui/theme.rs`
- Test: `crates/k10s-ui/tests/ui_shell.rs`

1. Add structural focus cues using border weight, title state, and shadow elevation.
2. Keep inactive and overlapping windows legible without relying on hue alone.
3. Verify focus transitions and overlapping-window rendering in deterministic egui tests.

### Task 4: Capture final evidence and verify

**Files:**
- Create: `docs/screenshots/issue-172/after-1280x800.png`
- Create: `docs/screenshots/issue-172/after-640x420.png`
- Modify only intentional browser snapshots affected by the shell.

1. Run formatting, Clippy, relevant unit tests, the workspace suite, WASM build/check, browser tests, and desktop smoke.
2. Run safe read-only validation against the existing `kind-bunyip` context if cluster validation is relevant.
3. Capture and inspect fixed-viewport after screenshots at 1280x800 and 640x420.
4. Commit, push `spacedragon/issue-172`, and open a non-draft PR with `Closes #172`, summary, test evidence, and directly embedded before/after images.
5. Monitor required checks, CI code review, review threads, requested changes, and mergeability; fix and push until clean, rebase `origin/main` if needed, then squash merge.
