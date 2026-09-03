# Configuration

The standalone `k10s-server` validates security-sensitive inputs and the real
Kubernetes backend before binding its listener. Unknown flags, missing values,
invalid addresses, bad kubeconfigs, and missing assets fail startup.

## Standalone CLI and environment

| Input | Meaning |
| --- | --- |
| `--listen` `HOST:PORT` | Listener address. Overrides `K10S_BIND_ADDR`, whose default is `127.0.0.1:8080`. |
| `--kubeconfig` `PATH` | Use exactly this kubeconfig. Without it, discovery uses `KUBECONFIG`, then `~/.kube/config`. |
| `--token-file` `PATH` | Read and trim the access token from this file. Overrides `K10S_ACCESS_TOKEN_FILE`. |
| `--fake` | Explicit deterministic development backend. Production and normal desktop launches never fall back to `--fake`. |
| `--shutdown-file` `PATH` | Test/automation hook: begin ordered shutdown when the path appears. Prefer SIGINT/SIGTERM operationally. |
| `K10S_BIND_ADDR` | Listener used when `--listen` is absent. |
| `K10S_ACCESS_TOKEN_FILE` | Token-file source used when `--token-file` is absent. |
| `K10S_ACCESS_TOKEN` | Inline token source. `K10S_ACCESS_TOKEN_FILE` wins over `K10S_ACCESS_TOKEN`; a CLI token file wins over both. |
| `K10S_DIST_DIR` | Development override for the exact Trunk output directory. Release artifacts use embedded assets when absent. |

A token is mandatory for every non-loopback bind. Empty/unreadable token files
and tokens over the security bound fail before bind. Avoid the inline variable
in production because process environments are more easily exposed than a
permission-restricted file.

Kubeconfig discovery preserves kube-rs behavior, including path-list handling
in `KUBECONFIG`. The selected file must contain a valid current context and all
referenced clusters/users. Credentials and raw kubeconfig content are never
returned to clients. Desktop uses standard discovery and reports failure; it
never falls back to `--fake` or switches to fixtures.

## Desktop external-shell descriptor

There is no standalone flag or Web setting for shell access. The native desktop
publishes `Open shell` only for its own embedded-local connection after it can
resolve local `kubectl`, faithfully reproduce the ordered kubeconfig sources
and selected context, and find a platform terminal adapter. An in-memory or
unavailable kubeconfig, a remote/standalone connection, missing kubectl, or a
descriptor that would require secret environment makes the action unavailable.
On Windows, the fixed non-secret allowlist also preserves `SYSTEMROOT` and
`WINDIR`, which Windows PowerShell and the CLR require after environment
sanitization.

The descriptor, not Pod data, is authoritative for context and kubeconfig.
Allowed non-secret exec-plugin environment is captured when the embedded server
is prepared; resource context/namespace/name/UID/container values remain
structured and platform-quoted. macOS uses `open` on a private `.command` file,
Linux uses the documented ordered terminal-adapter fallback, and Windows starts
`powershell.exe` directly with a private `.ps1` file and new console. See
[Security](security.md) for the UID preflight's residual race and cleanup rules.
Finback serializes the exact merged configuration prepared for the backend into
the owner-only launch directory and points kubectl at that immutable snapshot,
so later edits to the original source paths cannot retarget the shell.

## Embeddable `ServerConfig`

These Rust API fields are not standalone environment variables. Integrators
set them before `run`/`run_with_assets`; `validate()` rejects zero or
contradictory hard bounds.

| Field | Default | Purpose |
| --- | --- | --- |
| `access_token` | empty | First-`Hello` shared secret. |
| `startup_readiness_delay` | 0 | Intentional starting interval. |
| `probe_drain_grace` | 0 | Minimum observable probe-draining interval. |
| `hello_timeout` | 5 s | Control authentication deadline. |
| `graceful_flush_timeout` | 250 ms | Best-effort writer flush and overload close-handshake limit. |
| `max_frame_size` | 1 MiB | Control WebSocket frame bound. |
| `max_message_size` | 4 MiB | Assembled control-message bound. |
| `max_unauthenticated_connections` | 32 | Pre-auth socket cap. |
| `max_authenticated_connections` | 128 | Authenticated control socket cap. |
| `outbound_queue_capacity` | 64 | Per-control-socket outbound queue. |
| `max_resource_subscriptions_per_session` | 64 | Live resource watches per session. |
| `snapshot_rows_per_chunk` | 128 | Normalized rows per snapshot chunk. |
| `drain_grace_timeout` | 250 ms | Read-only status window after shutdown notice. |
| `drain_timeout` | 10 s | Absolute tracked-task drain deadline. |
| `capabilities` | logs.tail | Advertised wire capability identifiers. External desktop shell is not a server capability. |
| `max_stream_frame_size` | 64 KiB | Log-stream frame bound. |
| `max_stream_message_size` | 256 KiB | Assembled log-stream message bound. |
| `stream_hello_timeout` | 5 s | Dedicated-stream authentication deadline. |
| `stream_rate_budget_bytes_per_sec` | 512 KiB/s | Per-direction stream rate budget. |
| `max_stream_connections` | 64 | Concurrent dedicated stream cap. |
| `resume_max_journal_entries` | 1,024 | Replay frames retained per session. |
| `resume_max_sessions` | 256 | Retained fresh/disconnected sessions. |
| `resume_entry_max_age` | 30 s | Maximum replay-frame age. |

The standalone release intentionally fixes its startup readiness delay to one
second and externally observable drain grace to 250 ms. Changing library
bounds requires tests and an operator-facing release note.
# Desktop Pod and Service port forwarding

Port forwarding is desktop-only. The native application's embedded server
advertises `service.portForward` and `pod.portForward`; the standalone server
and browser deployment advertise neither and expose no flag or environment
variable to enable them. Every listener binds only to `127.0.0.1`. Blank or
`0` selects an available local port; an explicit port must be in `1..=65535`.

The limits—16 active sessions, 32 accepted connections globally, and 8 per
session—are shared across both target types. The global Port Forwards window
manages both Pod and Service sessions from the same server-owned feed. A
context switch stops and joins both types before committing the switch, and
application shutdown stops and joins them before the embedded runtime exits.
