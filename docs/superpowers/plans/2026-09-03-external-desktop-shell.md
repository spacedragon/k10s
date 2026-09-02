# External Desktop Shell Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the embedded exec terminal with a local-embedded-server-only Desktop button that opens a guarded `kubectl exec -it` session in the operating system terminal.

**Architecture:** Shared egui code exposes a capability-gated, structured external-shell action and never starts processes. The Desktop composition root owns a `KubectlLaunchDescriptor`, secure temporary-script renderer, and platform launcher. Active WebSocket exec support is removed; protocol-major-1 legacy decoding and an authenticated fail-closed tombstone remain for one compatibility window.

**Tech Stack:** Rust 1.97, egui/eframe 0.36.1, std process/filesystem APIs, platform-specific process extensions, existing k10s protocol/server/backend test harnesses.

---

## File structure

- Create `apps/k10s-desktop/src/external_shell.rs`: descriptor, validated target, script rendering, secure temporary storage, cleanup, and platform launcher facade.
- Create `apps/k10s-desktop/src/external_shell/{unix.rs,windows.rs}`: OS-specific secure file and process primitives.
- Create `apps/k10s-desktop/tests/external_shell.rs`: hermetic fake-kubectl and platform-independent launcher/script tests.
- Modify `apps/k10s-desktop/src/lib.rs`: build the descriptor with the embedded server, inject capability generation into `K10sApp`, drain launch requests after a frame, and surface typed errors.
- Modify `apps/k10s-desktop/Cargo.toml`: add only narrowly required filesystem/platform dependencies after proving std is insufficient.
- Modify `crates/k10s-ui/src/ui/mod.rs`: add external-shell availability/action queues and remove shell-session queues.
- Modify `crates/k10s-ui/src/ui/detail/{mod.rs,frame.rs,pod.rs}`: remove Shell tab and render `Open shell` with visible container selection.
- Modify `crates/k10s-ui/src/workspace/{detail.rs,guard.rs,snapshot.rs}`: remove connected-shell state and navigation blocking.
- Delete `crates/k10s-ui/src/ui/tools/shell.rs`; modify `crates/k10s-ui/src/ui/tools/mod.rs` and stream stores accordingly.
- Modify `crates/k10s-ui/src/app.rs` and `crates/k10s-ui/src/client/{state.rs,streams.rs}`: expose/drain structured requests and remove active exec transport.
- Modify `crates/k10s-backend/src/kube/{mod.rs,config.rs}` (or the discovered kube factory module): prepare one resolved kube configuration shared by embedded server creation and the external-shell descriptor.
- Modify focused UI tests under `crates/k10s-ui/tests/` and in-module tests for the new capability and removal behavior.
- Modify `crates/k10s-protocol/src/{lib.rs,route.rs,stream.rs}` and protocol tests: advance the minor, retain legacy exec decode identifiers for the compatibility window, and stop advertising active exec semantics.
- Modify `crates/k10s-server/src/{control.rs,streams.rs,exec.rs,lifecycle.rs}` and tests: return typed fail-closed tombstones without reaching the backend.
- Modify `crates/k10s-backend/src/{stream.rs,fake.rs,kube/mod.rs,kube/exec.rs}` and tests: remove active exec sessions/input while retaining logs.
- Modify `docs/{protocol.md,security.md,configuration.md,troubleshooting.md}` and `README.md`: document external Desktop behavior, prerequisites, residual UID race, and compatibility tombstone.

### Task 1: Shared UI capability and external-shell action

**Files:**
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Modify: `crates/k10s-ui/src/ui/detail/frame.rs`
- Modify: `crates/k10s-ui/src/ui/detail/pod.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Test: `crates/k10s-ui/tests/ui_snapshots.rs`
- Test: `crates/k10s-ui/tests/ui_resilience.rs`

- [ ] **Step 1: Add failing capability projection tests**

Add tests that construct Pod detail views with `ExternalShellAvailability::{Unavailable, Available { generation }}`. At this stage assert unavailable views contain no new external-shell button and available views expose one `Open shell` button with accessible tooltip text. Assertions removing the legacy Shell tab/shortcut belong to Task 4.

- [ ] **Step 2: Run the focused tests and confirm failure**

Run separately: `cargo test --locked -p k10s-ui --test ui_snapshots external_shell -- --exact` and `cargo test --locked -p k10s-ui --test ui_resilience external_shell -- --exact`.

Expected: each command runs exactly one named test and FAILs because the capability and button do not exist. If a command reports zero tests, correct the filter before proceeding.

- [ ] **Step 3: Add the target and action boundary**

Define shared, platform-neutral types near `ResourceAction`:

```rust
pub struct ExternalShellTarget {
    pub generation: u64,
    pub namespace: String,
    pub pod: String,
    pub uid: String,
    pub container: String,
    pub program: String,
}

pub enum ExternalShellAvailability {
    Unavailable,
    Available { generation: u64 },
}

pub enum ResourceAction {
    // existing variants
    OpenExternalShell { window: WindowId, target: ExternalShellTarget },
}
```

Store availability in `UiShell`, render the button only for an exact core/v1 Pod with complete current detail authority, and queue a structured action. Context must not be copied into the target; it belongs to the Desktop descriptor. Reuse the existing logs container selection state where valid, otherwise use the typed Pod default container. Tests cover a visible multi-container chooser/current selection, typed-default fallback, exact namespace/pod/UID/container/program/generation target construction, and absence when namespace, pod, UID, container, or authoritative detail data is missing.

- [ ] **Step 4: Drain but do not execute the new action in `K10sApp`**

Add a bounded one-frame queue and public `drain_external_shell_requests()` seam. `handle_resource_action` must revalidate the active detail identity and generation before enqueueing. Connection replacement/reconnect and a committed Kubernetes context switch synchronously revoke availability and clear pending requests. `K10sApp` emits a typed committed-context-change event but never sees or rebuilds a Desktop descriptor. Test that revocation and queue clearing happen in the same transition and that a delayed old-generation request is rejected.

- [ ] **Step 5: Run focused UI tests**

Run the two exact focused commands from Step 2 separately.

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-ui/src crates/k10s-ui/tests
git commit -m "feat(ui): expose capability-gated external shell action"
```

### Task 2: Desktop launch descriptor and safe script rendering

**Files:**
- Create: `apps/k10s-desktop/src/external_shell.rs`
- Create: `apps/k10s-desktop/tests/external_shell.rs`
- Modify: `apps/k10s-desktop/src/lib.rs`
- Modify: `apps/k10s-desktop/Cargo.toml`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Create or Modify: `crates/k10s-backend/src/kube/config.rs`
- Test: `apps/k10s-desktop/tests/kube_factory.rs`

- [ ] **Step 1: Write failing descriptor and injection tests**

Cover one-shot shared kube preparation, exact kubectl and exec-plugin executable resolution, default and explicit kubeconfig sources, two kubeconfig files with the same context name, rejection of fake/in-memory backend modes, and rejection when required environment is sensitive/unrepresentable. Probe the current platform launcher before publishing availability: `/usr/bin/open` on macOS, at least one exact Linux adapter, and PowerShell on Windows. Use isolated environment maps rather than mutating process-global variables in parallel tests.

- [ ] **Step 2: Run tests and confirm failure**

Run: `cargo test --locked -p k10s-desktop --test external_shell -- descriptor`

Expected: FAIL because `external_shell` does not exist.

- [ ] **Step 3: Implement `KubectlLaunchDescriptor`**

Add a shared kube preparation result returned before the embedded kernel starts: the ready kernel/client configuration plus exact ordered source paths, selected context, and resolved exec-plugin command metadata. Both server launch and the immutable descriptor consume this one snapshot; neither independently rediscovers environment configuration. Include `generation`, resolved kubectl path, authoritative context, and a fixed allowed environment set (`PATH`, `HOME`/platform profile directory, and `KUBECONFIG` source paths). Reject non-Unicode values and exec plugins whose command cannot be resolved or whose declared configuration requires inherited variables outside that set. Values named or shaped like credentials/tokens/keys are never rendered; such configurations are unavailable. Tests cover missing dependencies, secret-looking declared env, profile/file-based plugins, and environment changes after preparation.

- [ ] **Step 4: Write failing renderer/injection tests**

For POSIX and PowerShell renderers cover spaces, quotes, `$()`, backticks, `%`, `!`, `&`, `|`, `<`, `>`, Unicode, newline, and NUL. Assert rejected fields create no file. Execute output with a fake kubectl and verify exact argv/environment, UID mismatch short-circuit, and preserved exit status. For UID lookup failure, mismatch, and exec nonzero, assert a diagnostic is printed, acknowledgement reads safely from the terminal (including EOF), the original status survives, and self-cleanup runs afterward. Gate real PowerShell execution with `cfg(windows)`.

- [ ] **Step 5: Implement structured rendering**

Render a UID lookup followed by `kubectl exec -it` using dedicated POSIX and PowerShell literal functions. Default program is `/bin/sh`. Treat UID lookup failure, UID mismatch, and exec failure as distinct script stages without parsing kubectl stderr. Each failure prints a stable prefix, waits for one acknowledgement only when stdin is a terminal, treats EOF as acknowledgement, preserves the original kubectl/status code across cleanup, and never hangs headless tests. Never log rendered scripts.

- [ ] **Step 6: Run focused Desktop tests**

Run: `cargo test --locked -p k10s-desktop --test external_shell -- descriptor && cargo test --locked -p k10s-desktop --test external_shell -- render`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add apps/k10s-desktop crates/k10s-backend/src/kube crates/k10s-backend/tests apps/k10s-desktop/tests/kube_factory.rs
git commit -m "feat(desktop): build guarded kubectl shell scripts"
```

### Task 3: Secure temporary scripts and platform launchers

**Files:**
- Modify: `apps/k10s-desktop/src/external_shell.rs`
- Modify: `apps/k10s-desktop/src/external_shell/unix.rs`
- Modify: `apps/k10s-desktop/src/external_shell/windows.rs`
- Modify: `apps/k10s-desktop/tests/external_shell.rs`
- Modify: `apps/k10s-desktop/src/lib.rs`
- Modify: `apps/k10s-desktop/Cargo.toml`

- [ ] **Step 1: Write failing temporary-storage tests**

Assert cryptographically random names from the fixed safe alphabet, atomic create-new behavior, Unix `openat`/`O_NOFOLLOW` plus `0700`, parent ownership/type validation, Windows owner-SID ACL and handle-based reparse refusal, synchronous launch-failure cleanup, self-cleanup instructions, oldest-first 128-entry/24-hour startup cleanup, and refusal to touch lookalikes outside the owned parent. The manifest has a version, random launch ID, creation timestamp, and exact expected script basename and is atomically created owner-only before the script. Fault injection at directory creation, ACL/chmod, manifest write, script create/write/chmod, and renderer failure proves every partially created owned object is rolled back without touching pre-existing entries.

- [ ] **Step 2: Run tests and confirm failure**

Run: `cargo test --locked -p k10s-desktop --test external_shell -- temporary`

Expected: FAIL because storage is unimplemented.

- [ ] **Step 3: Implement storage and cleanup**

Use `getrandom` for names. Implement mandatory descriptor-relative/no-follow Unix operations and Windows handle/reparse/owner-SID checks in separate cfg modules (using narrowly scoped `libc` and `windows-sys` APIs if std lacks them); refusal is mandatory, not best-effort. A creation transaction records only objects it successfully created and rolls them back in reverse order on every pre-launch error, including permission and rendering failures. Never recursively delete an unvalidated root. Cleanup work is bounded, but each attempted deletion is safety validated. POSIX scripts remove script/manifest/directory; PowerShell removes both literal files, rechecks the parent, removes the directory, and preserves kubectl status.

- [ ] **Step 4: Write failing launcher tests**

Inject executable lookup and process spawning. Assert exact order and argv: macOS `/usr/bin/open <script>`; Linux `xdg-terminal-exec --`, `x-terminal-emulator -e`, `gnome-terminal --`, `konsole -e`, `kitty --`; Windows `powershell.exe -NoLogo -NoProfile -ExecutionPolicy Bypass -File` plus `CREATE_NEW_CONSOLE`. A successful spawn stops fallback; missing executable or synchronous error advances it.

- [ ] **Step 5: Implement platform launch adapters**

Use `std::process::Command` argv exclusively. Do not invoke `sh -c`, `cmd /c`, or interpolate a command line. On Windows use `std::os::windows::process::CommandExt`. Return typed stage errors and clean the launch directory on synchronous total failure.

- [ ] **Step 6: Wire Desktop frame draining and error display**

During `DesktopApp` startup and before the first capability publication, validate the stable private parent and invoke bounded startup cleanup. A validation failure refuses cleanup and leaves external-shell availability disabled with a typed storage-unavailable reason. Desktop launch integration tests prove expired validated children are handled, live and invalid lookalikes remain, and only the oldest 128 entries are examined. After each inner `K10sApp` frame, drain requests, compare request generation and current embedded descriptor generation, launch valid requests, and expose sanitized errors in the existing application status/error surface. Desktop consumes the typed committed-context-change event, keeps UI unavailable while rebuilding the descriptor from retained prepared sources, assigns a new generation, verifies the platform launcher/kubectl probes, then republishes availability. Clear capability before any server/connection replacement. Integration tests prove there is no launchable frame between context commit and new descriptor publication.

- [ ] **Step 7: Run focused tests**

Run: `cargo test --locked -p k10s-desktop --test external_shell`

Expected: PASS on the current platform; platform-specific CI covers the other adapters.

- [ ] **Step 8: Commit**

```bash
git add apps/k10s-desktop
git commit -m "feat(desktop): open kubectl exec in system terminals"
```

### Task 4: Remove embedded shell UI and navigation state

**Files:**
- Delete: `crates/k10s-ui/src/ui/tools/shell.rs`
- Modify: `crates/k10s-ui/src/ui/tools/mod.rs`
- Modify: `crates/k10s-ui/src/ui/mod.rs`
- Modify: `crates/k10s-ui/src/ui/detail/mod.rs`
- Modify: `crates/k10s-ui/src/workspace/detail.rs`
- Modify: `crates/k10s-ui/src/workspace/guard.rs`
- Modify: `crates/k10s-ui/src/workspace/snapshot.rs`
- Modify: `crates/k10s-ui/src/app.rs`
- Test: `crates/k10s-ui/tests/stream_tools.rs`
- Test: `crates/k10s-ui/tests/ui_snapshots.rs`
- Test: `crates/k10s-ui/tests/ui_command_palette.rs`

- [ ] **Step 1: Change tests to require absence of embedded shell**

Delete Shell-tool behavior assertions and add final regressions proving unavailable Web/remote and available Desktop Pod views all lack the legacy Shell tab, shortcut, palette action, and placeholder; context switching and window close are never blocked by an external shell; and snapshots do not persist shell state. Preserve the capability-gated `Open shell` button tests from Task 1.

- [ ] **Step 2: Run focused tests and confirm failure**

Run the three focused test binaries as separate commands so an expected first failure cannot skip the others.

Expected: FAIL while the old tab, state, and stream actions remain.

- [ ] **Step 3: Remove old UI/session/guard paths**

Remove `DetailTab::Shell`, `ShellState`, `BlockReason::ConnectedShell`, stream stores/actions, render branch, shortcuts, connection guards, reconciliation, and signal projection. Preserve Logs behavior and the new external-shell request queue.

- [ ] **Step 4: Run the k10s-ui suite**

Run: `cargo test --locked -p k10s-ui`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/k10s-ui
git commit -m "refactor(ui): remove embedded shell sessions"
```

### Task 5: Retire active exec protocol and backend with a tombstone

**Files:**
- Modify: `crates/k10s-protocol/src/lib.rs`
- Modify: `crates/k10s-protocol/src/stream.rs`
- Modify: `crates/k10s-protocol/tests/golden_protocol.rs`
- Modify: `crates/k10s-server/src/control.rs`
- Modify: `crates/k10s-server/src/streams.rs`
- Modify: `crates/k10s-server/src/exec.rs`
- Modify: `crates/k10s-server/src/lifecycle.rs`
- Modify: `crates/k10s-server/tests/control_socket.rs`
- Modify: `crates/k10s-server/tests/stream_sockets.rs`
- Delete: `crates/k10s-server/tests/exec_loopback.rs`
- Modify: `crates/k10s-ui/src/client/state.rs`
- Modify: `crates/k10s-ui/src/client/streams.rs`
- Modify: `crates/k10s-ui/tests/stream_tools.rs`
- Modify: `crates/k10s-backend/src/stream.rs`
- Modify: `crates/k10s-backend/src/fake.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Delete: `crates/k10s-backend/src/kube/exec.rs`
- Modify: `crates/k10s-backend/tests/exec_session.rs`

- [ ] **Step 1: Write failing tombstone compatibility tests**

Advance protocol minor and test current/previous-minor negotiation. An old exec ticket request must receive `unsupportedMessage`. The tombstone `/exec` accepts a normal hello containing a valid access token and any legacy ticket string, authenticates the token without redeeming the ticket, emits the existing `unsupportedMessage` stream error, and closes; an invalid token still receives the existing authentication error. Instrument the backend to prove all cases have zero subscribe/exec calls. New clients must never issue exec requests to old servers. Golden fixtures preserve legacy discriminants without assigning them new meanings.

- [ ] **Step 2: Run protocol/server tests and confirm failure**

Run: `cargo test --locked -p k10s-protocol && cargo test --locked -p k10s-server --test control_socket -- exec && cargo test --locked -p k10s-server --test stream_sockets -- exec`

Expected: FAIL because exec remains active.

- [ ] **Step 3: Implement the compatibility tombstone**

Keep an explicit compatibility allowlist for one window: `EXEC_PATH`, `StreamType::Exec`, exec ticket-request decoding, legacy stdin/resize/TTY numeric discriminants, and the tombstone route. Reject ticket issuance before backend dispatch. The tombstone parses hello, authenticates only its token/protocol version, deliberately does not validate/redeem the arbitrary ticket, returns typed unsupported, and closes. Remove advertised active capability and do not reuse discriminants.

- [ ] **Step 4: Remove active client/server/backend exec implementation**

Remove `StreamRoute::Exec`, client stdin/resize/TTY production and consumption, server active dispatch, lifecycle registration/cancellation beyond the tombstone, `ExecSessions`, kube attached-process code, fake exec state, Ready/Exit exec projection, and obsolete tests. Keep shared log ticket framing and all log backpressure/security limits. Add an `rg` regression/assertion that exec symbols occur only in the documented compatibility allowlist, golden fixtures, and tombstone tests.

- [ ] **Step 5: Run crate suites**

Run: `cargo test --locked -p k10s-protocol && cargo test --locked -p k10s-backend && cargo test --locked -p k10s-server`

Expected: PASS, including explicit proof that the tombstone cannot reach backend exec.

- [ ] **Step 6: Commit**

```bash
git add crates/k10s-protocol crates/k10s-backend crates/k10s-server crates/k10s-ui/src/client crates/k10s-ui/tests/stream_tools.rs
git commit -m "refactor(protocol): retire embedded exec behind tombstone"
```

### Task 6: Documentation, platform checks, and full verification

**Files:**
- Modify: `README.md`
- Modify: `docs/protocol.md`
- Modify: `docs/security.md`
- Modify: `docs/configuration.md`
- Modify: `docs/troubleshooting.md`
- Modify: `.github/workflows/ci.yml`
- Modify: `.github/workflows/release.yml` if release smoke hooks are kept there

- [ ] **Step 1: Update user and protocol documentation**

Document Desktop-local-only availability, local kubectl/terminal prerequisites, exact platform launch behavior, no Web/remote shell, non-atomic UID preflight race, descriptor reproduction rules, temporary-script cleanup, and the one-window exec tombstone.

- [ ] **Step 2: Add explicit platform execution coverage**

Add a macOS/Linux/Windows matrix job. Run generated scripts with fake kubectl and exercise platform secure-file primitives. Keep real graphical-terminal marker checks as release smoke tests rather than headless CI requirements.

- [ ] **Step 3: Run formatting and static checks**

Run: `cargo fmt --all -- --check && cargo clippy --workspace --all-targets --locked -- -D warnings`

Expected: PASS.

- [ ] **Step 4: Run full automated verification**

Run: `cargo test --workspace --all-targets --locked`

Expected: PASS.

- [ ] **Step 5: Verify Web has no Shell surface**

Run the repository's documented WASM build/check and browser acceptance command from `README.md`; inspect AccessKit/text output for absence of Shell controls.

Expected: build/check and browser tests PASS; no shell UI appears.

- [ ] **Step 6: Review the final diff for generated artifacts and secrets**

Record the implementation base commit before Task 1. Run: `git status --short`, `git diff --check`, `git diff --cached --check`, `git diff --stat <implementation-base>..HEAD`, and `git diff --stat` for remaining worktree changes.

Expected: only intended source/tests/docs changes; no rendered scripts, kubeconfig, credentials, or unrelated untracked files staged.

- [ ] **Step 7: Commit final documentation and CI changes**

```bash
git add README.md docs .github
git commit -m "docs: document external desktop shell workflow"
```
