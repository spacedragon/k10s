# k10s

A local Kubernetes control-plane client with a Rust workspace foundation: a
protocol crate shared by native and web frontends, a fake-backed backend
kernel, and an embeddable Axum control server. The release candidate includes
real kubeconfig-backed reads and operations, guarded YAML, logs, exec, bounded
recovery, browser/native frontends, load budgets, and native/server packages.

## Scope

Normal desktop and standalone launches use the real Kubernetes adapter and
standard kubeconfig discovery. Deterministic fake mode remains available only
through the explicit standalone `--fake` development/test flag; a missing or
invalid kubeconfig never silently changes backend.

## Workspace layout

| Path | Purpose |
| --- | --- |
| `crates/k10s-protocol` | Wire protocol: frames, envelopes, error contract (no platform dependencies) |
| `crates/k10s-backend` | Backend port, kernel, and the deterministic fake adapter |
| `crates/k10s-server` | Embeddable Axum control server: WebSocket control socket, auth, outbound scheduler, readiness probes, ordered shutdown |
| `crates/k10s-ui` | Shared client state machine and transport used by both frontends |
| `apps/k10s-desktop` | Native egui/eframe app embedding the server on a random loopback port |
| `apps/k10s-web` | WASM frontend built with Trunk |

## Native development

```bash
cargo test --locked --workspace          # all unit + integration tests
cargo run -p k10s-desktop                # desktop app (embeds the server)
cargo run -p k10s-server-app             # standalone server on 127.0.0.1:8080
```

Standalone server environment:

- `K10S_BIND_ADDR` — listener address (default `127.0.0.1:8080`)
- `K10S_DIST_DIR` — optional external Trunk output tree for development. Release
  binaries embed the exact fingerprinted `dist/` built before Cargo runs.
- `K10S_ACCESS_TOKEN_FILE` — path of a file containing the access token
  (surrounding whitespace trimmed). When set, it always takes precedence over
  `K10S_ACCESS_TOKEN`. An empty or unreadable file refuses startup.
- `K10S_ACCESS_TOKEN` — inline access token. Required for non-loopback binds;
  loopback defaults to an empty token.

The server logs structured, credential-free telemetry to stderr at `info`
level. `SIGINT`/`SIGTERM` trigger the ordered drain described below.

Operator references: [configuration](docs/configuration.md),
[deployment](docs/deployment.md), [security](docs/security.md),
[troubleshooting](docs/troubleshooting.md), and [protocol](docs/protocol.md).

## Security model

**Access tokens.** The token travels exclusively in the first protocol `Hello`
it is accepted on, and it must never reach URLs, built assets,
localStorage-style persistence, logs, or error payloads:

- Server config types redact the token from every `Debug` rendering.
- The web gate holds the entered value only in an ephemeral form buffer and
  hands it straight to the protocol client as connection state; on successful
  authentication the buffer is discarded. Persisted settings carry only the
  credential-free endpoint URL, and the transport rejects any WebSocket URL
  containing userinfo, query strings, or fragments.
- Secret sources resolve with documented precedence: `K10S_ACCESS_TOKEN_FILE`
  wins over `K10S_ACCESS_TOKEN`; empty files refuse to start; no source at all
  is only valid for loopback-only development binds (`StandaloneConfig` still
  rejects non-loopback listeners without an explicit token).
- Token comparison inside the control socket uses a constant-time byte compare,
  and connections are bounded before authentication completes (unauthenticated
  connection cap, first-frame deadline, frame/message size limits).

**Same-origin enforcement.** Control-socket upgrades that carry a browser
`Origin` header must match the request's own `Host` authority (default ports
80/443 compare equal to their implicit form); anything else is rejected with
HTTP 403. Native desktop clients send no `Origin` and are unconstrained, so
they keep working unchanged. Browsers enforce no reliable same-origin policy
on WebSocket connections, so this server-side check is the enforcement point,
not defense in depth; clients must not rely on browser behavior for it.

**Reverse proxies / trusted headers.** k10s never infers client identity or
trust from request headers: `X-Forwarded-*`, `X-Real-Ip`, and similar are
ignored by default. When placing the server behind a TLS/auth-terminating
reverse proxy, enforce authentication there and keep the host/origin seen by
k10s identical to what the browser uses (same host end-to-end); otherwise the
same-origin check will refuse upgrades.

## Web development

```bash
trunk build --release                    # builds apps/k10s-web into dist/
cargo check --locked -p k10s-web --target wasm32-unknown-unknown
npx playwright install chromium          # once, for the browser smoke
npx playwright test tests/browser/foundation.spec.ts --project=chromium
```

`Trunk.toml` pins `locked = true`; CI pins Trunk 0.21.14. The web entry derives
only the scheme and authority from `window.location` and replaces the path with
the root-level control endpoint; it never forces WSS on HTTP development pages.

## Resilient states, accessibility, and capacity

The connected UI prototype closes with a quality gate covering every
loading/empty/stale/error state of the approved screen set:

- `crates/k10s-ui/tests/ui_resilience.rs` — loading vs empty vs filtered-empty
  lists, stale-connection banners, textual (never color-only) status and
  metrics conditions, conflict reasons inside operation dialogs, a gone-resource
  projection for deleted selections, unavailable GVKs after context switches,
  disconnected logs that retain history, active-shell navigation guards,
  keyboard focus order, and minimum-size non-overlap.
- `crates/k10s-ui/tests/ui_snapshots.rs` + `tests/snapshots/*.txt` — stable
  accessibility-tree snapshots of the approved screens. These are deterministic
  text dumps (roles, labels, values in widget order) rather than pixel PNGs:
  byte-stable across renderers and CI runners, and they double as the AccessKit
  coverage. Regenerate intentionally with
  `K10S_UPDATE_SNAPSHOTS=1 cargo test -p k10s-ui --test ui_snapshots`.

### Capacity benchmarks

A deterministic **50,000-object / 1,000-node** fake dataset
(`FakeKubernetes::with_capacity`) anchors the capacity gate; the same fixed
distribution is used end to end:

| Command | Proves |
| --- | --- |
| `cargo bench -p k10s-backend --bench fake_scale -- --test` | dataset build time, full pod-list query time, subscription snapshot registration, and stable live memory across repeated queries |
| `cargo test -p k10s-server --test fake_capacity` | the whole dominant-kind snapshot (~18,750 rows) streams through the real control socket as bounded ≤16-row pages and reassembles completely |
| `tests/kind/cluster.sh up` then `cargo test --locked -p k10s-backend --test kind_read_path -- --ignored --nocapture` | real kubeconfig contexts, discovery, built-ins, CRDs, lists/details/YAML, owner traversal, events, RBAC denial, honest missing metrics, and live watch apply/delete recovery against ephemeral kind |
| `cargo test --locked -p k10s-backend --test kind_operations -- --ignored --nocapture` (with the same kind cluster) | least-privilege server-side dry-run/apply, UID/RV conflicts, scale/restart/delete propagation, Job/CronJob actions, bounded logs/exec, RBAC denial, and reconciliation after an induced lost mutation response |
| `cargo test --locked -p k10s-server --test kind_server_read_path -- --ignored --nocapture` | the same configured cluster reaches clients through the authenticated real control WebSocket and never falls back to fake fixture contexts |
| `cargo bench -p k10s-ui --bench ui_capacity -- --test` | the shell renders/filters/scrolls the 50k-object model at a fixed 1440×900 viewport within recorded frame-time and allocation-per-frame ceilings |

Recorded baselines (developer workstation; CI ceilings carry an order of
magnitude of headroom while still catching order-of-magnitude regressions):

- backend: dataset build ≈ 0.11 s, full pod-list query ≈ 11 ms, snapshot
  registration ≈ 10 ms, no live-memory drift across repeated queries
- server: full socket transfer of ~1,175 bounded frames ≈ 1.4 s (debug build)
- UI: ≈ 1.3 ms average frame time with virtualized rows (~30k allocations per
  frame), ceilings 100 ms / 150k — losing row virtualization or doing
  model-sized work per frame breaches them

Both benches are hand-rolled (`harness = false`) because the ceilings
themselves are the assertions; they run once under `--test` for CI
determinism. Plan 5 repeats this gate against real runtime pressure.

## Health probes

| Probe | Semantics |
| --- | --- |
| `/healthz` | Liveness: `200 ok\n` while the process event loop is alive — including during shutdown — until the listener itself closes |
| `/readyz` | Readiness: `503 starting\n` during initialization, `200 ready\n` only after initialization and request acceptance, `503 initialization failed\n` after a failed startup, `503 draining\n` once shutdown begins |

Probe bodies are fixed strings: no kubeconfig paths or credentials.

## Graceful shutdown order

Shutdown is an explicitly sequenced state machine; every stage is published as
a `k10s_server::lifecycle` log event:

1. Mark not-ready — `/readyz` flips to `503 draining`.
2. Stop accepting application connections — new control upgrades are refused.
3. Send `ShutdownNotice` to connected sessions and close the mutation gate;
   status reads keep working for a bounded grace window (`drain_grace_timeout`).
4. Cancel watches/logs and terminate exec streams as each socket task unwinds.
5. Drain tracked connection tasks under one absolute hard deadline
   (`drain_timeout`); survivors are force-closed, any task that still ignores
   the force signal is aborted and joined before returning, and `shutdown`
   reports `TimedOut`. Upgrades accepted but not yet running hold a pending
   registration so they cannot slip past the drain.
6. Close the listener last — forced teardown completes inside the serving
   lifetime, so `/healthz` stays reachable until the listener itself closes.

Connection tasks are tracked with `tokio_util::TaskTracker`; in-flight requests
observe cancellation and return structured errors. Access tokens are sent only
in the first `Hello` frame and are never logged, persisted, or embedded in
error payloads or probe bodies.

## CI

`.github/workflows/ci.yml` runs, per pull request and on `main`:

- **Unit tests** — `cargo fmt --all -- --check`, Clippy with `-D warnings`,
  and `cargo test --locked --workspace --all-targets`
- **WASM check** — `cargo check --locked -p k10s-web --target wasm32-unknown-unknown`
- **Web foundation** — pinned Trunk 0.21.14 release build plus the Chromium
  Playwright smoke against the standalone server
- **Native platform smoke** — release-mode server/desktop builds and loopback
  launch probes on registered Windows/macOS self-hosted runners

## Releasing

Release builds are automated by [`.github/workflows/release.yml`](.github/workflows/release.yml)
on self-hosted native runners. The fixed build order is Trunk 0.21.14, the
locked release workspace, cargo-packager 0.11.8, cargo-dist 0.32.0, then the
OCI image. Desktop outputs are `.deb` + `.AppImage`, `.msi` + NSIS `.exe`, and
`.app` + `.dmg`; the standalone server is a per-target `.tar.xz`/`.zip` with
the same web bundle embedded, plus a non-root OCI image.

Local release verification uses the same order:

```sh
trunk build --release
cargo build --locked --release --workspace
cargo install cargo-packager --version 0.11.8 --locked
cargo packager --release
cargo install cargo-dist --version 0.32.0 --locked
dist build --artifacts=local
docker buildx build --load -f packaging/container/Dockerfile -t k10s:test .
```

Signing remains opt-in and secrets are never stored in the repository. Windows
signing supplies the certificate to the runner and configures the packager
thumbprint/timestamp inputs at release time. macOS signing/notarization supplies
an Apple signing identity, App Store Connect issuer/key ID, and private key via
the runner keychain/environment. Pull requests exercise source, web, native
build, and loopback launch gates; installer/archive creation and OCI packaging
run only for a release tag or an explicit manual release-pipeline smoke test.

To cut a release:

1. Bump the matching `version` in `Cargo.toml` and `Packager.toml`, then merge
   the change (CI on `main` must stay green).
2. Tag exactly `v<version>` (the workflow fails if the tag does not match the workspace version):

   ```sh
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. Monitor the Release workflow; the release is created automatically with generated release notes.

A manual `workflow_dispatch` run of the same workflow builds all platform
artifacts without publishing. Run it before tagging whenever packaging metadata,
the container definition, release tooling, or release workflow changes.
