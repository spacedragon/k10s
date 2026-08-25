# Security

k10s is a single-user cluster client, not a multi-tenant authorization layer.
Kubernetes RBAC is authoritative. Remote web deployment additionally requires
TLS, authentication, and access control at a reverse proxy.

## Secrets and credentials

- Prefer `--token-file`; then `K10S_ACCESS_TOKEN_FILE`; use
  `K10S_ACCESS_TOKEN` only for constrained automation. File precedence is
  intentional, empty files fail, and non-loopback listening requires a token.
- The access token appears only in the first WebSocket `Hello`. It must not be
  placed in URLs, web assets, browser persistence, errors, or probes.
- Config debug output substitutes `[REDACTED]`; normal telemetry records safe
  identifiers and state transitions, never credentials or raw kubeconfig.
- Kubeconfigs that reference exec credential plugins are rejected before the
  backend is committed; k10s never executes external credential binaries.
  Protect supported credential material with operating-system permissions and
  run k10s as a dedicated user.
- The OCI image runs as `10001:10001`; mount credentials read-only and grant
  only the Kubernetes RBAC needed for intended operations.

## Logs and diagnostics

Request/subscription/operation IDs are safe correlation IDs for joining client
errors to server lifecycle and operation logs. Operators may share those IDs,
timestamps, error codes, and redacted configuration. They must never log payloads,
YAML bodies, exec input/output, log-stream contents, access tokens, or raw
kubeconfig. See [Troubleshooting](troubleshooting.md) for the correlation flow.

Browser WebSocket origins are checked against the observed host. The server
ignores `X-Forwarded-*` and `X-Real-IP` as trust signals. Preserve Host/Origin
at the proxy and do not expose an unprotected upstream.
