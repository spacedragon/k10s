# k10s

A local Kubernetes control-plane client with a Rust workspace foundation: a
protocol crate shared by native and web frontends, a fake-backed backend
kernel, and an embeddable Axum control server. The current milestone is the
**runtime foundation** — connection lifecycle, probes, graceful shutdown, and
CI. Resource windows and operational workflows come later.

## Scope

Everything is **fake-only** today: `BackendKernel` serves deterministic fake
Kubernetes data (`FakeKubernetes`) over the loopback control WebSocket. No real
cluster access, no credentials, and no mutation operations exist yet.

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
- `K10S_DIST_DIR` — exact Trunk output tree to host (default `dist`)
- `K10S_ACCESS_TOKEN_FILE` — path of a file containing the access token
  (surrounding whitespace trimmed). When set, it always takes precedence over
  `K10S_ACCESS_TOKEN`. An empty or unreadable file refuses startup.
- `K10S_ACCESS_TOKEN` — inline access token. Required for non-loopback binds;
  loopback defaults to an empty token.

The server logs structured, credential-free telemetry to stderr at `info`
level. `SIGINT`/`SIGTERM` trigger the ordered drain described below.

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
