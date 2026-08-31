# Pod Log Auto-Tail and Scroll Follow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically start a Pod log tail when its Logs tab opens and derive follow/autoscroll behavior from whether the log viewport is at the bottom.

**Architecture:** Keep stream lifecycle and retry eligibility in `LogsTool`/`LogsViews`, while keeping pixel scroll geometry in the egui renderer. The renderer performs a one-shot automatic open for an eligible disconnected view, uses egui's bottom-stick behavior while following, and projects actual bottom position back into `LogsTool`; the existing application ticket path remains unchanged.

**Tech Stack:** Rust, egui 0.36.1, egui_kittest, Cargo workspace tests

---

## File structure

- Modify `crates/k10s-ui/src/ui/tools/logs.rs`: connection-attempt eligibility, retry action, scroll-follow derivation, and log-pane rendering.
- Modify `crates/k10s-ui/tests/stream_tools.rs`: pure state-machine coverage for one-shot auto-connect and retry eligibility.
- Modify `crates/k10s-ui/tests/ui_details.rs`: rendered Logs-tab auto-connect/action assertions and toolbar regression coverage.
- Optionally modify focused in-module tests in `crates/k10s-ui/src/ui/tools/logs.rs` only if egui scroll output cannot be driven reliably from the integration harness; keep scroll geometry helpers private to this module.

### Task 1: One-shot automatic log connection

**Files:**
- Modify: `crates/k10s-ui/tests/stream_tools.rs`
- Modify: `crates/k10s-ui/src/ui/tools/logs.rs`

- [ ] **Step 1: Write failing state-machine tests**

Add tests proving a fresh disconnected tool can claim exactly one automatic attempt, remains ineligible while Connecting/Streaming, stays ineligible after `fail` or `connection_lost`, exposes retry eligibility for both failures, becomes eligible after a source-setting change, and becomes Connecting through an explicit retry method. Also prove every new/retried source resets follow to true. Use behavior-oriented methods such as:

```rust
assert!(tool.begin_auto_connect());
assert_eq!(tool.phase(), LogsPhase::Connecting);
assert!(!tool.begin_auto_connect());
tool.fail("ticket rejected");
assert!(!tool.begin_auto_connect());
assert!(tool.can_retry());
tool.retry();
assert_eq!(tool.phase(), LogsPhase::Connecting);
assert!(tool.follows());
```

- [ ] **Step 2: Run the focused test and verify RED**

Run: `cargo test -p k10s-ui --test stream_tools auto_connect -- --nocapture`

Expected: FAIL because the one-shot eligibility/retry API does not exist.

- [ ] **Step 3: Implement minimal lifecycle state**

In `LogsTool`, add a private attempt-eligibility flag initialized to true. Replace the renderer-facing use of `connect()` with a method that atomically checks eligibility, marks the attempt consumed, transitions to Connecting, and resets follow to true. Make `fail` and connection loss preserve ineligibility and expose retry eligibility (connection loss sets a safe disconnected error/status), make explicit retry start one new Connecting attempt and reset follow to true, and reset eligibility when container/Previous/Since changes create a new source. Preserve retained history on failures and existing clearing behavior on pod/container changes.

- [ ] **Step 4: Run focused state tests and verify GREEN**

Run: `cargo test -p k10s-ui --test stream_tools`

Expected: PASS.

- [ ] **Step 5: Commit the lifecycle slice**

```bash
git add crates/k10s-ui/src/ui/tools/logs.rs crates/k10s-ui/tests/stream_tools.rs
git commit -m "feat(ui): make log connection attempts one-shot"
```

### Task 2: Auto-connect on Logs-tab render with explicit retry

**Files:**
- Modify: `crates/k10s-ui/tests/ui_details.rs`
- Modify: `crates/k10s-ui/src/ui/tools/logs.rs`

- [ ] **Step 1: Write failing rendered-UI tests**

Extend the Pod detail harness to assert:

1. Before opening Logs, `drain_log_actions()` is empty.
2. The first render after clicking `Tab Logs` queues exactly one `LogsAction::OpenLogs` carrying the selected target and current Since/Previous values.
3. Further frames queue no duplicate action.
4. The old `Connect logs` and `Follow` controls are absent.
5. After projecting `fail` or `connection_lost`, the safe error/status and a `Retry logs` button are visible; clicking it queues one new action.

- [ ] **Step 2: Run the focused UI test and verify RED**

Run: `cargo test -p k10s-ui --test ui_details crashloop_logs_default_to_previous_with_complete_toolbar -- --nocapture`

Expected: FAIL because the tab still exposes `Connect logs` and does not auto-queue.

- [ ] **Step 3: Implement automatic queueing**

In `tools::logs::show`, after defaults/source reconciliation, atomically claim the automatic attempt when the view is eligible. Queue `OpenLogs` after the mutable view borrow ends, exactly as the current click path does. For any disconnected view whose automatic attempt was consumed (ticket/socket failure or control connection loss), render `Retry logs`; clicking it starts one new attempt and queues the same action immediately. Remove `Connect logs` and the manual `Follow` checkbox. Keep Connecting, Streaming, Pause/Resume, Since, Wrap, Find, Export, and error labels intact.

- [ ] **Step 4: Run UI and state tests and verify GREEN**

Run:

```bash
cargo test -p k10s-ui --test ui_details
cargo test -p k10s-ui --test stream_tools
```

Expected: PASS.

- [ ] **Step 5: Commit the UI connection slice**

```bash
git add crates/k10s-ui/src/ui/tools/logs.rs crates/k10s-ui/tests/ui_details.rs
git commit -m "feat(ui): tail pod logs when opening logs tab"
```

### Task 3: Scroll position controls follow

**Files:**
- Modify: `crates/k10s-ui/src/ui/tools/logs.rs`
- Test: `crates/k10s-ui/src/ui/tools/logs.rs` or `crates/k10s-ui/tests/ui_details.rs`

- [ ] **Step 1: Write failing bottom-detection tests**

Extract a small private helper whose inputs are egui scroll measurements and whose output is `at_bottom`. Test exact bottom, within a 2 logical-pixel tolerance, beyond tolerance, and content shorter than the viewport. Add a renderer/state test proving an upward position sets `follows()` false and returning to bottom sets it true.

Representative assertions:

```rust
assert!(is_at_bottom(100.0, 101.5));
assert!(!is_at_bottom(100.0, 103.0));
assert!(is_at_bottom(0.0, 0.0));
```

Use names matching the actual egui 0.36 `ScrollAreaOutput::state.offset.y` and `(content_size.y - inner_rect.height()).max(0.0)` semantics when implementing the helper.

- [ ] **Step 2: Run the focused tests and verify RED**

Run: `cargo test -p k10s-ui logs --lib -- --nocapture`

Expected: FAIL because bottom detection and scroll-position projection are absent.

- [ ] **Step 3: Implement geometry-driven follow**

Before rendering, capture whether the tool was following. Configure the vertical `ScrollArea` with `stick_to_bottom(was_following)` so appended lines remain visible only in follow mode. After `.show(...)` returns, calculate the clamped non-negative maximum vertical offset and compare the actual offset using the 2-pixel tolerance; update `view.set_follow(at_bottom)`. Do not call `scroll_to_cursor` unconditionally, and do not couple Pause to follow. When not following, allow egui to retain its existing offset while lines continue to append. The Task 1 transitions for a fresh, changed, or explicitly retried source ensure this task always starts those sessions at the bottom.

- [ ] **Step 4: Run focused and integration tests and verify GREEN**

Run:

```bash
cargo test -p k10s-ui logs --lib
cargo test -p k10s-ui --test stream_tools
cargo test -p k10s-ui --test ui_details
```

Expected: PASS.

- [ ] **Step 5: Commit the scroll-follow slice**

```bash
git add crates/k10s-ui/src/ui/tools/logs.rs crates/k10s-ui/tests/ui_details.rs
git commit -m "feat(ui): follow logs based on scroll position"
```

### Task 4: Regression verification

**Files:**
- Modify only if a regression test exposes an in-scope defect.

- [ ] **Step 1: Run formatting and static checks**

Run:

```bash
cargo fmt --all -- --check
cargo clippy -p k10s-ui --all-targets -- -D warnings
```

Expected: PASS with no warnings.

- [ ] **Step 2: Run the complete UI test suite**

Run: `cargo test -p k10s-ui`

Expected: PASS, including existing tail truncation, pause, find, export, connection-loss, snapshots, and stream-ticket tests.

- [ ] **Step 3: Inspect the final diff**

Run: `git diff HEAD~3 --check && git status --short`

Expected: no whitespace errors; only intended source/test changes and this plan/design documentation are present.

- [ ] **Step 4: Commit any final test-only adjustment**

If verification required an in-scope adjustment, commit it separately with a focused message; otherwise do not create an empty commit.
