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
| `graceful_flush_timeout` | 250 ms | Best-effort writer flush limit. |
| `max_frame_size` | 1 MiB | Control WebSocket frame bound. |
| `max_message_size` | 4 MiB | Assembled control-message bound. |
| `max_unauthenticated_connections` | 32 | Pre-auth socket cap. |
| `max_authenticated_connections` | 128 | Authenticated control socket cap. |
| `outbound_queue_capacity` | 64 | Per-control-socket outbound queue. |
| `max_resource_subscriptions_per_session` | 64 | Live resource watches per session. |
| `snapshot_rows_per_chunk` | 16 | Normalized rows per snapshot chunk. |
| `drain_grace_timeout` | 250 ms | Read-only status window after shutdown notice. |
| `drain_timeout` | 10 s | Absolute tracked-task drain deadline. |
| `capabilities` | logs.tail, exec.attach | Advertised wire capability identifiers. |
| `max_stream_frame_size` | 64 KiB | Logs/exec frame bound. |
| `max_stream_message_size` | 256 KiB | Assembled logs/exec message bound. |
| `stream_hello_timeout` | 5 s | Dedicated-stream authentication deadline. |
| `stream_rate_budget_bytes_per_sec` | 512 KiB/s | Per-direction stream rate budget. |
| `max_stream_connections` | 64 | Concurrent dedicated stream cap. |
| `resume_max_journal_entries` | 1,024 | Replay frames retained per session. |
| `resume_max_sessions` | 256 | Retained fresh/disconnected sessions. |
| `resume_entry_max_age` | 30 s | Maximum replay-frame age. |

The standalone release intentionally fixes its startup readiness delay to one
second and externally observable drain grace to 250 ms. Changing library
bounds requires tests and an operator-facing release note.
