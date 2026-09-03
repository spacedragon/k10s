# Troubleshooting

Start with the timestamp, endpoint, safe error code, and correlation ID shown
by the UI/server. Search structured server logs for that exact ID. Do not copy
access tokens, request payloads, resource YAML, pod logs, generated shell
scripts, or raw kubeconfig into tickets.

| Symptom | Check / recovery |
| --- | --- |
| Startup says no kubeconfig | Set `KUBECONFIG`, create `~/.kube/config`, or pass `--kubeconfig`; verify current-context and referenced entries. There is no fake fallback. |
| Startup rejects token | Non-loopback binds require one; check file readability and non-empty trimmed content. CLI file overrides env file, which overrides inline env. |
| Port forward says local port is in use | Clear the local-port field or enter `0` to let the desktop choose an available loopback port. |
| Port forward reports no ready endpoint | Verify the Service has a ready Pod-backed EndpointSlice, that the Service UID has not changed, and that the required RBAC is granted. |
| Port forwarding controls are absent | Port forwarding is available only in the native desktop application; standalone web deployments intentionally do not advertise it. |
| `Open shell` is absent | It exists only for the native desktop application's own embedded-local server. Verify local `kubectl`, a supported terminal launcher, and that the kubeconfig/exec-plugin environment can be reproduced without secrets. Web and remote/standalone connections intentionally have no shell surface. |
| Shell launch reports missing kubectl or terminal | Install `kubectl` and verify it is discoverable at desktop startup. On macOS verify `.command` files have a usable `open` association; on Linux install one of `xdg-terminal-exec`, `x-terminal-emulator`, `gnome-terminal`, `konsole`, or `kitty`; on Windows verify `powershell.exe` can create a new console. Restart after changing PATH. |
| Shell reports Pod UID mismatch or missing Pod | Refresh the Pod selection. The preflight deliberately refuses a replaced/deleted Pod; a same-name recreation can still race after the check because Kubernetes exec has no UID precondition. |
| Shell opens against unexpected configuration | Do not construct a reduced command. Restart the desktop and verify the exact kubeconfig sources and current context it discovered. If those files are unavailable or reproduction requires secret environment, shell should remain unavailable. |
| A temporary shell file remains | Scripts normally self-delete and launch failures clean immediately. Restarting the desktop performs bounded cleanup of validated owner-only entries older than 24 hours. Do not weaken permissions or recursively delete the system temporary directory. |
| `403` WebSocket upgrade | Browser Origin and upstream Host differ. Preserve both through the proxy; do not bypass the same-origin check. |
| `/readyz` says starting | Wait for initialization. Persistent failure should be correlated with startup logs. |
| `/readyz` says initialization failed | Fix the kubeconfig/backend error and restart; do not route traffic. |
| `/readyz` says draining | The process is shutting down. Route elsewhere and allow its drain deadline. |
| `resyncRequired` or reconnect banner | Keep the client open: it discards stale server-issued state, bootstraps again, recreates desired subscriptions, and refreshes nonterminal operations. |
| Operation outcome unknown | Refresh the object and query operation status before retrying; reuse the idempotency key where supported. |
| Logs rejected | Check Kubernetes RBAC, selected context/namespace/container, ticket expiry, connection/rate limits, and proxy WebSocket timeout. Legacy embedded-exec requests receive the protocol-minor-6 typed compatibility tombstone and cannot reach Kubernetes. |
| Overload/slow-client close | Reduce subscriptions/streams, fix the stalled client, and inspect configured bounds; do not blindly increase every limit. |

For an incident, record k10s version/platform, deployment form, sanitized
startup command (token values removed), probe transitions, cluster version,
correlation IDs, and minimal reproduction. A new server instance invalidates
unsafe replay and intentionally forces a complete resync.
