# Exec Plugin Context Availability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Allow kubeconfig exec credential plugins while keeping plugin failures local to the affected context, visible as disabled selector entries with safe hover diagnostics, and recoverable through Refresh.

**Architecture:** Carry an explicit availability state from backend bootstrap through the wire protocol into UI state. Build kube clients lazily per context, classify only kube exec-auth failures as context-unavailable, and serialize probes through a refresh coordinator without holding the registry lock while a child process runs. Preserve backward wire compatibility by defaulting missing availability to `available`, and return machine-readable `contextUnavailable` details on rejected switches.

**Tech Stack:** Rust 2024, kube-rs 4.2, Tokio, Tower, Serde/serde_json, GPUI, cargo-nextest/cargo test.

---

## File map

- `crates/k10s-backend/src/port.rs`: shared availability and typed context-unavailable backend error.
- `crates/k10s-backend/src/runtime/context.rs`: mutable registry state, generation-safe availability transitions and fallback selection.
- `crates/k10s-backend/src/kube/config.rs`: accept exec users and normalize interactive execution policy.
- `crates/k10s-backend/src/kube/auth.rs`: new focused module for exec-auth error classification and safe diagnostics.
- `crates/k10s-backend/src/kube/mod.rs`: lazy/coalesced client construction, refresh orchestration, cache eviction, and switch enforcement.
- `crates/k10s-backend/src/kube/auth_observer.rs`: new Tower layer that detects runtime exec-auth refresh failures.
- `crates/k10s-protocol/src/bootstrap.rs`: backward-compatible wire availability fields.
- `crates/k10s-server/src/control.rs`: machine-readable unavailable-context error frame.
- `crates/k10s-ui/src/app.rs`: retain full context models, reconcile bootstrap, disable failed switch targets, and make Refresh request Bootstrap.
- `crates/k10s-ui/src/ui/top_bar.rs`, `crates/k10s-ui/src/ui/shell.rs`: render unavailable contexts as hoverable, non-selectable rows.
- `apps/k10s-web/src/lib.rs`: adapt ready-view rendering to full context values.
- `crates/k10s-backend/tests/support/exec_plugin.rs`: deterministic temporary kubeconfig/plugin/local-service fixtures shared by backend integration tests.
- `scripts/exec-plugin-smoke.sh`: account-independent packaged-app smoke fixture and launcher.

### Task 1: Availability domain and wire compatibility

**Files:**
- Modify: `crates/k10s-backend/src/port.rs`
- Modify: `crates/k10s-backend/src/kernel.rs`
- Modify: `crates/k10s-protocol/src/bootstrap.rs`
- Test: `crates/k10s-protocol/src/bootstrap.rs`
- Test: `crates/k10s-backend/tests/kernel_bootstrap.rs`

- [ ] **Step 1: Write one failing test `context_availability_round_trips`** that serializes/deserializes `Unknown`, `Available`, and `Unavailable` and compares both state and reason.
- [ ] **Step 2: Write one failing test `legacy_context_defaults_to_available`** that deserializes the old four-field JSON and asserts `Available` plus no reason.
- [ ] **Step 3: Write one failing test `old_peer_ignores_context_availability`** using a local `LegacyContext` derive and deserialize new JSON into it; assert the original four fields.
- [ ] **Step 4: Write table test `context_reason_is_normalized`** for both deserialization and serialization of `(available, Some)`, `(unknown, Some)`, and `(unavailable, None)`: the first two must never emit/retain the reason and the last must receive the generic safe reason `credential plugin is unavailable` before its next round trip.
- [ ] **Step 5: Run `cargo test -p k10s-protocol context_ -- --nocapture`** and verify the new assertions fail to compile or deserialize.
- [ ] **Step 6: Add the shared enum and fields.** Use the exact wire shape, with a private raw serde representation and `From<RawContext> for Context` calling `normalize_reason()` so invalid state/reason pairs cannot escape deserialization:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub enum ContextAvailability {
    Unknown,
    #[default]
    Available,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    pub name: String,
    pub cluster: String,
    pub namespace: Option<String>,
    pub is_current: bool,
    pub availability: ContextAvailability,
    pub unavailable_reason: Option<String>,
}

impl Serialize for Context {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        #[serde(rename_all = "camelCase")]
        struct WireContext<'a> {
            name: &'a str,
            cluster: &'a str,
            #[serde(skip_serializing_if = "Option::is_none")]
            namespace: &'a Option<String>,
            is_current: bool,
            availability: ContextAvailability,
            #[serde(skip_serializing_if = "Option::is_none", rename = "unavailableReason")]
            unavailable_reason: Option<&'a str>,
        }
        WireContext {
            name: &self.name,
            cluster: &self.cluster,
            namespace: &self.namespace,
            is_current: self.is_current,
            availability: self.availability,
            unavailable_reason: matches!(self.availability, ContextAvailability::Unavailable)
                .then(|| self.unavailable_reason.as_deref()).flatten(),
        }.serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawContext {
    name: String,
    cluster: String,
    #[serde(default)]
    namespace: Option<String>,
    is_current: bool,
    #[serde(default)]
    availability: ContextAvailability,
    #[serde(default, rename = "unavailableReason")]
    unavailable_reason: Option<String>,
}

impl From<RawContext> for Context {
    fn from(raw: RawContext) -> Self {
        let unavailable_reason = match raw.availability {
            ContextAvailability::Unavailable => Some(raw.unavailable_reason
                .unwrap_or_else(|| "credential plugin is unavailable".into())),
            _ => None,
        };
        Self { name: raw.name, cluster: raw.cluster, namespace: raw.namespace,
            is_current: raw.is_current, availability: raw.availability,
            unavailable_reason }
    }
}

impl<'de> Deserialize<'de> for Context {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        RawContext::deserialize(deserializer).map(Into::into)
    }
}
```

Mirror the state in `ContextInfo`, map it in `BackendKernel`, and provide `ContextInfo::available(...)` for fixtures so call sites stay readable.
- [ ] **Step 7: Run `cargo test -p k10s-protocol && cargo test -p k10s-backend kernel_bootstrap`** and expect PASS.
- [ ] **Step 8: Run `git add crates/k10s-protocol/src/bootstrap.rs crates/k10s-backend/src/port.rs crates/k10s-backend/src/kernel.rs crates/k10s-backend/tests/kernel_bootstrap.rs`, then `git commit -m "feat: expose context availability in bootstrap"`.**

### Task 2: Registry transitions, fallback, and typed rejection

**Files:**
- Modify: `crates/k10s-backend/src/port.rs`
- Modify: `crates/k10s-backend/src/runtime/context.rs`
- Test: `crates/k10s-backend/tests/context_registry.rs`
- Test: `crates/k10s-backend/tests/context_switch.rs`

- [ ] **Step 1: Add failing registry tests** for marking unavailable, refusing an unavailable switch, preserving stable order, selecting the first already-available fallback, clearing current when none is available, clearing a stale error on success, and rejecting a stale generation commit.
- [ ] **Step 2: Run `cargo test -p k10s-backend --test context_registry availability -- --nocapture`** and expect compile/test failures.
- [ ] **Step 3: Add a registry generation and short mutation APIs:**

```rust
pub fn snapshot(&self) -> (u64, Vec<ContextInfo>);
pub fn mark_available(&mut self, generation: u64, name: &str) -> bool;
pub fn mark_unavailable(&mut self, generation: u64, name: &str, reason: String) -> bool;
pub fn choose_available_fallback(&mut self) -> Option<String>;
```

Add `BackendError::ContextUnavailable { context, reason }`. `prepare_switch` must return it before any discovery or client use.
- [ ] **Step 4: Run `cargo test -p k10s-backend --test context_registry && cargo test -p k10s-backend --test context_switch`** and expect PASS.
- [ ] **Step 5: Run `git add crates/k10s-backend/src/port.rs crates/k10s-backend/src/runtime/context.rs crates/k10s-backend/tests/context_registry.rs crates/k10s-backend/tests/context_switch.rs`, then `git commit -m "feat: track context availability transitions"`.**

### Task 3: Accept exec config and produce safe diagnostics

**Files:**
- Create: `crates/k10s-backend/src/kube/auth.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Modify: `crates/k10s-backend/src/kube/config.rs`
- Modify: `crates/k10s-backend/Cargo.toml`
- Test: `crates/k10s-backend/tests/context_registry.rs`
- Test: `crates/k10s-backend/src/kube/auth.rs`

- [ ] **Step 1: Replace the old rejection test with failing `exec_plugin_context_is_accepted` and `interactive_exec_policy_is_normalized` tests.** Assert `Always` becomes a typed unavailable failure, `IfAvailable` becomes `Never`, and `Never` remains unchanged.
- [ ] **Step 2: Add classifier table test `exec_auth_failures_are_safe`** using `kube::client::AuthError::{MissingCommand, AuthExecStart, AuthExecRun, AuthExecParse, ExecPluginFailed}`. Put secrets independently in stderr, stdout, command name, arguments, environment-shaped text, JWT, PEM, invalid UTF-8, control characters, and beyond the 2 KiB UTF-8 boundary; assert only sanitized bounded stderr appears.
- [ ] **Step 3: Run `cargo test -p k10s-backend --lib kube::auth::tests -- --nocapture` and `cargo test -p k10s-backend --test context_registry exec_plugin_context_is_accepted -- --nocapture`** and confirm failures.
- [ ] **Step 4: Add failing tracing-capture test `plugin_failure_log_does_not_leak`** and a serialized `BackendError` diagnostic test using different canary secrets in stdout, stderr, args, env, and raw error text. Assert the redacted stderr fragment is present and every other canary absent; run `cargo test -p k10s-backend --lib plugin_failure_log_does_not_leak -- --nocapture` and confirm FAIL.
- [ ] **Step 5: Remove `has_exec_plugin` rejection and normalize execution policy.** `Always` returns typed unavailable, `IfAvailable` is rewritten to `Never`, and `Never` is unchanged in a cloned per-context config immediately before client construction.
- [ ] **Step 6: Implement exhaustive `kube::client::AuthError` classification** returning `Option<SafeExecFailure>`. Initial construction receives `kube::Error::Auth(auth)` from `ClientBuilder::try_from`; runtime service failures carry `kube::client::AuthError` inside `tower::BoxError`. Construct diagnostics only from structured auth variants and sanitized stderr; never format the generic kube error or `Output` with `Debug`.
- [ ] **Step 7: Run `cargo test -p k10s-backend --lib kube::auth::tests -- --nocapture`, `cargo test -p k10s-backend --test context_registry exec_plugin -- --nocapture`, and `cargo clippy -p k10s-backend --all-targets -- -D warnings`** and expect PASS.
- [ ] **Step 8: Run `git add crates/k10s-backend/Cargo.toml crates/k10s-backend/src/kube/auth.rs crates/k10s-backend/src/kube/config.rs crates/k10s-backend/src/kube/mod.rs crates/k10s-backend/tests/context_registry.rs`, then `git commit -m "feat: allow kube exec credential plugins safely"`.**

### Task 4: Lazy client probes and bootstrap fallback

**Files:**
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Test: `crates/k10s-backend/tests/context_registry.rs`
- Create: `crates/k10s-backend/tests/support/exec_plugin.rs`
- Modify: `crates/k10s-backend/tests/support/mod.rs`

- [ ] **Step 1: Create `support/exec_plugin.rs` and failing `bootstrap_runs_only_current_exec`.** The helper owns a `mktemp`-style directory, writes executable scripts with `std::fs`, renders an HTTPS localhost kubeconfig, exposes atomic counter/status files, and starts the existing test TLS Kubernetes service. Each plugin emits an `ExecCredential` JSON object; tests configure success, stderr failure, or rotating behavior without using the operator account.
- [ ] **Step 2: Add `switch_lazily_runs_unknown_exec_once` and `concurrent_switch_coalesces_exec_probe`**, with counter assertions of zero before selection and one after all joins.
- [ ] **Step 3: Add `failed_current_falls_back_without_bootstrap_error`, `all_exec_failures_still_bootstrap`, `non_exec_build_failure_stays_unknown`, and `failed_exec_client_is_not_cached`** with exact availability/current/cache assertions.
- [ ] **Step 4: Run `cargo test -p k10s-backend --test context_registry exec -- --nocapture`** and verify failures.
- [ ] **Step 5: Refactor adapter coordination:** make registry and client cache shareable, add a refresh-operation mutex plus per-context build locks, and ensure every plugin build happens outside the registry lock. Run `Config::from_custom_kubeconfig`/`ClientBuilder::try_from` in blocking work from the desktop single-thread runtime.
- [ ] **Step 6: Implement generation-checked probe commits.** Bootstrap validates the configured current context; on exec failure mark only it unavailable and probe stable-order unknown contexts until one succeeds. Ordinary TLS/URL/network/RBAC failures remain retryable `Unknown` and must not be misclassified.
- [ ] **Step 7: Implement lazy switch.** Unknown destinations are probed before discovery; successful probes become available, exec failures become unavailable and return `ContextUnavailable` without moving current/workspace.
- [ ] **Step 8: Run `cargo test -p k10s-backend --test context_registry && cargo test -p k10s-backend --test context_switch && cargo test -p k10s-backend`** and expect PASS.
- [ ] **Step 9: Run `git add crates/k10s-backend/src/kube/mod.rs crates/k10s-backend/tests/context_registry.rs crates/k10s-backend/tests/support/mod.rs crates/k10s-backend/tests/support/exec_plugin.rs`, then `git commit -m "feat: isolate exec plugin failures by context"`.**

### Task 5: Refresh recovery and runtime auth failure observation

**Files:**
- Create: `crates/k10s-backend/src/kube/auth_observer.rs`
- Modify: `crates/k10s-backend/src/kube/mod.rs`
- Modify: `crates/k10s-backend/Cargo.toml`
- Test: `crates/k10s-backend/tests/context_registry.rs`

- [ ] **Step 1: Add failing `refresh_retries_unavailable_contexts`** and assert each retry counter plus final state, including independent commits when only some retries recover.
- [ ] **Step 2: Add failing `refresh_preserves_available_current`, `refresh_retries_former_current_in_stable_order`, and `refresh_without_current_probes_unknown_last`.** Record invocation order; assert unavailable retries precede unknown probes and current changes only after successful results.
- [ ] **Step 3: Add failing `runtime_failure_never_probes_unknown_context`** with an unknown-plugin counter that must remain zero after the active request fails.
- [ ] **Step 4: Add failing real kube-rs test `expired_exec_credential_failure_disables_once`.** Configure the fixture plugin to emit a valid `ExecCredential` with an expiration timestamp 11 seconds ahead on invocation one, then exit non-zero with adversarial stdout/stderr on invocation two. Because kube-rs compares wall-clock `jiff::Timestamp::now()`, use `tokio::time::timeout(Duration::from_secs(15), async { tokio::time::sleep(Duration::from_secs(2)).await; ... })`; once the credential enters kube-rs's 10-second refresh window, issue eight concurrent requests through the real client. Assert exactly two plugin invocations, one transition/log, eviction, every request maps to `ContextUnavailable`, and later requests are blocked without a third invocation.
- [ ] **Step 5: Run `cargo test -p k10s-backend --test context_registry refresh_ -- --nocapture` and `cargo test -p k10s-backend --test context_registry expired_exec_credential_failure -- --nocapture`** and confirm failures.
- [ ] **Step 6: Add failing tracing assertions to Step 4** proving runtime stdout/raw-output/args/env canaries never reach logs or typed errors and sanitized bounded stderr does.
- [ ] **Step 7: Make `tower` a normal backend dependency** and implement `AuthObserverLayer` as `Layer<kube::client::GenericService>` whose service future maps `Err(BoxError)`. Inspect `error.downcast_ref::<kube::client::AuthError>()`; when classified, update the registry/cache and return a private typed boxed marker that adapter error mapping converts to `BackendError::ContextUnavailable`. Install it via `builder.with_layer(&AuthObserverLayer::new(context, state))` before `build()`.
- [ ] **Step 8: Implement authoritative Refresh:** serialize refreshes; snapshot generation; retry unavailable contexts in stable order outside the registry lock; commit each matching-generation result independently; preserve an available current; when no current remains, then probe unknown contexts in stable order; return the final snapshot.
- [ ] **Step 9: Run both exact test commands from Step 5, `cargo test -p k10s-backend`, and `cargo clippy -p k10s-backend --all-targets -- -D warnings`** and expect PASS.
- [ ] **Step 10: Run `git add crates/k10s-backend/Cargo.toml crates/k10s-backend/src/kube/auth_observer.rs crates/k10s-backend/src/kube/mod.rs crates/k10s-backend/tests/context_registry.rs crates/k10s-backend/tests/support/exec_plugin.rs`, then `git commit -m "feat: recover context auth state on refresh"`.**

### Task 6: Machine-readable server error

**Files:**
- Modify: `crates/k10s-server/src/control.rs`
- Test: `crates/k10s-server/tests/control_socket.rs`
- Test: `crates/k10s-server/tests/loopback_gate.rs`

- [ ] **Step 1: Add a failing control test** that switches to an unavailable context and expects code `conflict`, retryability `afterRefresh`, plus exact details:

```json
{"kind":"contextUnavailable","context":"broken","reason":"credential plugin exited with status 1: denied"}
```

- [ ] **Step 2: Run `cargo test -p k10s-server context_unavailable -- --nocapture`** and expect FAIL because details are absent.
- [ ] **Step 3: Add failing `unavailable_frame_does_not_leak_raw_exec_data`** with distinct stdout/args/env/raw-output canaries and assert only the pre-sanitized reason appears; rerun the Step 2 command and confirm both tests fail.
- [ ] **Step 4: Extend `send_backend_error`** to build `ErrorFrame::with_details(...)` only for `BackendError::ContextUnavailable`; keep all existing error mappings unchanged.
- [ ] **Step 5: Run `cargo test -p k10s-server --test control_socket context_unavailable -- --nocapture && cargo test -p k10s-server`** and expect PASS.
- [ ] **Step 6: Run `git add crates/k10s-server/src/control.rs crates/k10s-server/tests/control_socket.rs crates/k10s-server/tests/loopback_gate.rs`, then `git commit -m "feat: report unavailable contexts to clients"`.**

### Task 7: UI disabled selector entries and reconciliation

**Files:**
- Modify: `crates/k10s-ui/src/app.rs`
- Modify: `crates/k10s-ui/src/ui/top_bar.rs`
- Modify: `crates/k10s-ui/src/ui/shell.rs`
- Modify: `apps/k10s-web/src/lib.rs`
- Test: inline tests in the files above

- [ ] **Step 1: Add failing app-state tests** for retaining context availability, selecting only available entries, immediately disabling a target from `contextUnavailable` details, keeping workspace/connection intact, and issuing Bootstrap reconciliation. Add Refresh test proving it issues Bootstrap.
- [ ] **Step 2: Add failing top-bar render/interaction tests** proving unavailable entries remain visible, cannot dispatch a switch, expose sanitized reason on hover, and available entries still dispatch normally.
- [ ] **Step 3: Add end-to-end app-state test `runtime_auth_failure_reconciles_without_losing_workspace`**: feed the runtime failure frame (`Conflict`, `AfterRefresh`, `contextUnavailable` details), assert the target is disabled, stale workspace rows remain, Bootstrap is queued, and the subsequent snapshot selects fallback or no current.
- [ ] **Step 4: Run `cargo test -p k10s-ui context_unavailable -- --nocapture && cargo test -p k10s-ui runtime_auth_failure -- --nocapture`** and verify failures.
- [ ] **Step 5: Replace `context_names: Vec<String>` with `contexts: Vec<Context>`** through `AppView`, shell, top bar, and web renderer. Provide derived helpers for names to reduce fixture churn.
- [ ] **Step 6: Implement a hover-capable unavailable menu row** using normal pointer tracking plus suppressed click handling, not a native disabled control that drops hover. Tooltip text is the backend-provided safe reason.
- [ ] **Step 7: Parse and reconcile the unavailable error.** Branch on `details.kind`, update the target locally, preserve current state, and enqueue Bootstrap. Reconcile the authoritative response and ensure current fallback selection excludes unavailable contexts.
- [ ] **Step 8: Run `cargo test -p k10s-ui && cargo test -p k10s-desktop && cargo test -p k10s-server`** and expect PASS.
- [ ] **Step 9: Run `git add crates/k10s-ui/src/app.rs crates/k10s-ui/src/ui/top_bar.rs crates/k10s-ui/src/ui/shell.rs apps/k10s-web/src/lib.rs`, then `git commit -m "feat: show unavailable kube contexts in selector"`.**

### Task 8: Security docs, regression suite, and macOS artifact

**Files:**
- Modify: `docs/security-boundaries.md`
- Modify: `docs/superpowers/specs/2026-08-21-k10s-runtime-architecture-design.md`
- Create: `scripts/exec-plugin-smoke.sh`

- [ ] **Step 1: Update security documentation** to state direct argv execution, noninteractive policy, lazy execution, best-effort redaction/truncation, failure isolation, and the explicit lack of hard kill/hostile-output guarantees.
- [ ] **Step 2: Run `cargo fmt --all -- --check`**, then format if needed and rerun.
- [ ] **Step 3: Run `cargo test --workspace`** and require all tests PASS.
- [ ] **Step 4: Run `cargo clippy --workspace --all-targets --all-features -- -D warnings`** and require zero warnings.
- [ ] **Step 5: Create `scripts/exec-plugin-smoke.sh`.** It uses `mktemp -d` plus a cleanup trap, writes `success-plugin.sh`, `failure-plugin.sh`, and `config`; the successful plugin emits ExecCredential JSON, the failing plugin prints `fixture plugin denied` to stderr and exits 17, both point to the script's local fixture API endpoint, and the script exports its generated `KUBECONFIG` before launching the binary path passed as `$1`.
- [ ] **Step 6: Run `cargo build --locked --release --workspace && cargo packager --release`**, then `test -x target/release/packages/k10s.app/Contents/MacOS/k10s-desktop` and `test -f target/release/packages/k10s_0.1.0_aarch64.dmg`.
- [ ] **Step 7: Run `scripts/exec-plugin-smoke.sh target/release/packages/k10s.app/Contents/MacOS/k10s-desktop`.** Verify the successful context is selectable, the failed one is visible/disabled with `fixture plugin denied`, Refresh retries it, and the process remains alive. Treat any real-account smoke test as optional and operator-owned.
- [ ] **Step 8: Run `git add docs/security-boundaries.md docs/superpowers/specs/2026-08-21-k10s-runtime-architecture-design.md scripts/exec-plugin-smoke.sh`, then `git commit -m "docs: document exec plugin failure isolation"`.**
