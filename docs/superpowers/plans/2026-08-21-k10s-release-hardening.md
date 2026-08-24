# k10s Release Hardening Implementation Plan

**Status:** implementation complete; final release gates are enforced by CI
and the Release workflow. Task checkboxes below preserve the original build
plan; merged issue/PR history is the execution record.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Prove the complete k10s system is bounded, resumable, secure in its single-user model, performant at the approved capacity, and distributable on macOS, Linux, Windows, and web.

**Architecture:** Hardening deepens existing modules rather than adding new public seams. Resume journals and priority queues remain inside the server adapter; load budgets exercise the existing protocol; packaging wraps the same desktop and standalone-server entry points built in earlier plans.

**Tech Stack:** Existing workspace; proptest 1.11.0; tokio-tungstenite 0.30.0 test client; tracing-subscriber 0.3.23; kind; Trunk 0.21.14; `@playwright/test` 1.62.0; cargo-packager 0.11.8; cargo-dist 0.32.0; Docker Buildx; GitHub Actions platform runners.

---

### Task 1: Implement control resume journals and reconnection

**Files:** create `k10s-server/src/resume.rs`, modify `control.rs`, modify UI client state, test `tests/resume.rs`.

- [ ] Write failing tests for contiguous ack, replay within age/count budget, expired cursor, wrong server instance, session takeover, duplicate replay suppression, safe query retry, and preserving the already-shipped operation recovery by ID/idempotency key.
- [ ] Run focused tests; expect missing resume behavior.
- [ ] Add bounded per-session journal replay as an optimization over Plan 1 full-jitter reconnect/full-resync. Keep the existing correctness path: emit `ResyncRequired` instead of partial replay whenever any gap cannot be satisfied, then re-bootstrap/re-subscribe/query nonterminal operations.
- [ ] Run property tests across random disconnect/duplicate/reorder sequences; expect the client projection to match a fresh snapshot.
- [ ] Commit `feat: resume websocket control sessions`.

### Task 2: Stress and harden priority scheduling and every resource budget

**Files:** modify `k10s-server/src/{outbound,config,control,logs,exec}.rs`, modify UI client inbox, create `crates/k10s-server/tests/backpressure.rs`, create `crates/k10s-server/tests/budget_config.rs`.

- [ ] Write failing tests for P0/P1/P2 ordering, same-resource P2 coalescing, non-droppable terminal operation events, snapshot chunk limits, separate frame/assembled-message limits on control, Logs, and Exec routes—including fragmented oversized `Hello`, Exec input, and resize/control messages—an undrained ewebsock callback burst with no hidden receiver queue, full client inbox, subscription/session limits, and slow-client close reason.
- [ ] Run focused tests; expect pressure-policy or configuration gaps in the bounds introduced alongside each feature.
- [ ] Fuzz, tune, and centralize the already-bounded queues, semaphores, chunker, coalescer, and explicit overload closure. Make every default configurable and validated at startup; reject zero/impossible budgets. This task must not be the first point at which any queue becomes bounded.
- [ ] Run backpressure tests under Tokio paused time and leak/task-count assertions; expect PASS.
- [ ] Commit `feat: enforce websocket resource budgets`.

### Task 3: Harden token configuration and localhost/web exposure

**Files:** modify server `config.rs`/`auth.rs`, modify UI connection gate, create `tests/security.rs`, modify README.

- [ ] Write failing tests for secret file/env precedence, missing non-loopback token refusal, redaction, constant-time comparison wrapper, hello deadline, unauthenticated connection cap, token absent from URL/assets/localStorage/traces, and trusted proxy header policy.
- [ ] Run focused tests; expect gaps.
- [ ] Implement validated secret sources and safe configuration diagnostics. Add same-origin checks where browser Origin is present without rejecting the native client that has no Origin.
- [ ] Run security suite and inspect built web assets for fixture tokens; expect PASS/no matches.
- [ ] Commit `security: harden k10s access token handling`.

### Task 4: Verify graceful shutdown and crash-adjacent recovery

**Files:** modify lifecycle/runtime/stream modules, create `tests/system_shutdown.rs`.

- [ ] Write failing tests for ordered stop-accepting→notice→reject mutation→cancel watch/logs→terminate exec→drain tasks, deadline expiration, backend restart during mutation, and desktop window close.
- [ ] Run focused tests; expect incomplete ordering.
- [ ] Implement root/child cancellation tree and task tracking without detached tasks. On restart, change server_instance_id and force previous in-flight operations to unknown/refresh.
- [ ] Run shutdown tests repeatedly under sanitizing task counters; expect no listener or session leaks.
- [ ] Commit `feat: harden runtime shutdown`.

### Task 5: Establish medium-large cluster load budgets

**Files:** modify `crates/k10s-backend/benches/fake_scale.rs`, create `crates/k10s-backend/benches/cache_load.rs`, create `crates/k10s-server/benches/protocol_load.rs`, create `tests/load/{README,run.rs}`, modify config defaults.

- [ ] Write executable load assertions for 50,000 normalized objects, chunked first snapshot, 10,000-event burst, slow browser, sustained logs, watch relist, and operation-event priority.
- [ ] Run them once to compare against the Plan 2 fake/UI baseline and record expected failing thresholds plus named hardware/runner metadata.
- [ ] Optimize only measured hot paths: normalization allocation, snapshot chunking, delta coalescing, table projection, or queue scheduling. Do not change public protocol semantics.
- [ ] Set reviewed memory/latency/frame-time thresholds in the load harness and run twice to rule out warm-up anomalies.
- [ ] Commit `perf: establish k10s capacity budgets`.

### Task 6: Add browser E2E and cross-platform UI verification

**Files:** modify `package.json`, `package-lock.json`, `playwright.config.ts`, and `tests/browser/foundation.spec.ts`; create `tests/browser/{resources,recovery,streams,operations,layout}.spec.ts`; modify UI snapshots and `.github/workflows/ci.yml`.

- [ ] Write browser tests for connection gate, wrong/right token, context/list/detail, reconnect/resync, Logs, Exec, mutation dialog, tab refresh token loss, and 640×420 layout.
- [ ] Run `trunk build --release && npm ci && npx playwright install chromium firefox webkit && npx playwright test`; capture failures before expanding automation.
- [ ] Extend the Plan 1 pinned Playwright 1.62.0 setup: run the full suite on Chromium and bootstrap/reconnect compatibility smoke on Firefox/WebKit. `playwright.config.ts` starts `cargo run --locked -p k10s-server -- --fake --token-file tests/browser/token.txt --listen 127.0.0.1:18080`; keep egui assertions at AccessKit/text/state level and visual snapshots at fixed supported renderers.
- [ ] Add Linux full tests, macOS/Windows build+loopback launch smoke, `trunk build --release`, and Playwright trace/artifact retention to CI; run the matrix.
- [ ] Commit `test: add browser and platform coverage`.

### Task 7: Package desktop and standalone web releases

**Files:** create `Packager.toml`, `dist-workspace.toml`, `packaging/icons/*`, `packaging/container/Dockerfile`, `packaging/container/entrypoint.sh`, `packaging/smoke/{desktop,server}.rs`, `.github/workflows/release.yml`; modify server asset embedding and README.

- [ ] Write packaging smoke checks that assert each artifact contains the correct binary/assets, desktop binds loopback only with a fresh high-entropy token, and the packaged server reports `/healthz` 200 plus `/readyz` 503→200 before web bootstrap. During controlled shutdown assert `/readyz` returns 503 before `/healthz` disappears. Expected desktop artifacts are macOS `.app` + `.dmg`, Linux `.deb` + `.AppImage`, and Windows `.msi` + NSIS `.exe`; server artifacts are per-target `.tar.xz`/`.zip` archives plus an OCI image.
- [ ] Run checks and confirm failure before packaging metadata exists.
- [ ] Pin/install tools with `cargo install cargo-packager --version 0.11.8 --locked` and `cargo install cargo-dist --version 0.32.0 --locked`. Configure `Packager.toml` to package the already-built `k10s-desktop`; configure `dist-workspace.toml` to archive `k10s-server`; build the web assets first with Trunk and embed the same fingerprinted `dist/` in the server. Add a multi-stage, non-root OCI image exposing the standalone server only. Document signing/notarization inputs without committing secrets.
- [ ] Build in dependency order: `trunk build --release`; then `cargo build --locked --release --workspace`; then `cargo packager --release`; then `dist build --artifacts=local`; finally `docker buildx build --load -f packaging/container/Dockerfile -t k10s:test .`. Start from a clean checkout in CI and run smoke checks on each native runner and inside the container so stale/missing embedded assets cannot pass.
- [ ] Commit `build: package k10s releases`.

### Task 8: Complete operational documentation and final release gate

**Files:** modify `README.md`, create `docs/{configuration,deployment,security,troubleshooting,protocol}.md`, modify roadmap status.

- [ ] Write documentation acceptance checks for every CLI/config key, secret source, kubeconfig behavior, reverse-proxy requirement, `/healthz` liveness and `/readyz` startup/failure/draining semantics, fake mode, log redaction, supported protocol range, and recovery message.
- [ ] Run a clean-machine walkthrough from packaged artifacts and file every undocumented step as a failing checklist item.
- [ ] Complete docs, include example reverse-proxy configuration, and link correlation IDs to troubleshooting without exposing payloads.
- [ ] Run the full `--locked` release gate from the roadmap plus Playwright, kind read, kind operations, load, `cargo packager`, `dist build`, and container jobs; expect PASS.
- [ ] Commit `docs: prepare k10s release operations`.

## Plan 5 verification gate

- Resume never produces a silently divergent UI; gaps force resync.
- Every queue, journal, frame, subscription, and stream has a tested bound.
- Tokens are absent from URLs, assets, persistence, and normal traces.
- 50,000-object and 10,000-event scenarios satisfy recorded regression budgets.
- Browser and three native platforms build and pass their required smoke suites.
- Desktop and web artifacts contain the same protocol/UI version and are reproducible from CI.
- Full fake and kind-cluster product workflows pass before release.
- Release outputs are explicitly `.app/.dmg`, `.deb/.AppImage`, `.msi/NSIS .exe`, standalone archives, and one non-root OCI image built from the same committed lockfiles and fingerprinted web assets.
